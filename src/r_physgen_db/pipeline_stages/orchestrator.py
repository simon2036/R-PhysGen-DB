"""Stage orchestrator for the production dataset build."""

from __future__ import annotations

import json
import time
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd

from r_physgen_db.constants import DATA_DIR, PROJECT_ROOT
from r_physgen_db.pipeline_stages.artifacts import ArtifactRef, StageResult
from r_physgen_db.pipeline_stages.context import BuildState


StageCallable = Callable[[BuildState], StageResult]


@dataclass(slots=True)
class StageSpec:
    stage_id: str
    stage_name: str
    order: int
    func: StageCallable
    required_inputs: tuple[str, ...] = ()
    produced_outputs: tuple[str, ...] = ()


def build_dataset_staged(
    *,
    refresh_remote: bool = False,
    selected_stage_ids: Iterable[str] | None = None,
    stop_after: str | None = None,
    run_id: str | None = None,
    parent_run_id: str | None = None,
    resume_from_stage_id: str | None = None,
    project_root: Path | None = None,
    data_dir: Path | None = None,
) -> dict:
    from r_physgen_db.pipeline_stages.stages import STAGES

    state = BuildState(
        refresh_remote=refresh_remote,
        run_id=run_id or f"run_{uuid.uuid4().hex[:12]}",
        parent_run_id=parent_run_id,
        resume_from_stage_id=resume_from_stage_id,
        selected_stage_ids=tuple(selected_stage_ids or ()),
        project_root=project_root or PROJECT_ROOT,
        data_dir=data_dir or DATA_DIR,
    )
    results = run_stages(state, STAGES, stop_after=stop_after)
    failed = [result for result in results if result.status == "failed"]
    if failed:
        failure = failed[-1]
        raise RuntimeError(f"Stage {failure.stage_id} failed: {failure.error_message or failure.notes}")
    if state.report:
        return state.report
    return {
        "run_id": state.run_id,
        "stage_results": [
            {"stage_id": result.stage_id, "status": result.status, "notes": result.notes}
            for result in results
        ],
    }


def run_stages(state: BuildState, specs: Iterable[StageSpec], *, stop_after: str | None = None) -> list[StageResult]:
    ordered_specs = sorted(specs, key=lambda item: item.order)
    _restore_parent_artifacts(state, ordered_specs)
    enabled = set(state.selected_stage_ids) if state.selected_stage_ids else None
    resume_order = _resume_order(state, ordered_specs)
    results: list[StageResult] = []
    for spec in ordered_specs:
        if enabled is not None and spec.stage_id not in enabled:
            continue
        if enabled is None and resume_order is not None and spec.order < resume_order:
            continue

        attempt_id, attempt_number = state.next_attempt(spec.stage_id)
        started_at = _utc_now()
        guard_result = _check_required_inputs(state, spec)
        if guard_result is not None:
            finished_at = _utc_now()
            _persist_manifest(state, spec, guard_result, attempt_id, attempt_number, started_at, finished_at)
            results.append(guard_result)
            break

        try:
            result = spec.func(state)
        except Exception as exc:  # noqa: BLE001 - manifest must capture unexpected stage failures
            result = StageResult(
                stage_id=spec.stage_id,
                status="failed",
                error_message=str(exc),
                exception_traceback=traceback.format_exc(),
                notes=f"Unhandled exception in {spec.stage_name}",
            )

        for ref in result.outputs:
            state.register_artifact(ref)

        finished_at = _utc_now()
        _persist_manifest(state, spec, result, attempt_id, attempt_number, started_at, finished_at)
        results.append(result)

        if result.status not in {"succeeded", "degraded"}:
            break
        if stop_after is not None and spec.stage_id == stop_after:
            break
    return results


def _check_required_inputs(state: BuildState, spec: StageSpec) -> StageResult | None:
    missing: list[str] = []
    missing_files: list[str] = []
    for input_name in spec.required_inputs:
        artifact = state.artifacts.get(input_name)
        if artifact is None:
            missing.append(input_name)
            continue
        if artifact.kind in {"file", "table"} and artifact.path is not None and not artifact.path.exists():
            missing_files.append(f"{input_name}:{artifact.path}")
    if not missing and not missing_files:
        return None
    return StageResult(
        stage_id=spec.stage_id,
        status="failed",
        error_message="Required inputs missing",
        notes=f"missing={missing}; missing_files={missing_files}",
    )


def _persist_manifest(
    state: BuildState,
    spec: StageSpec,
    result: StageResult,
    attempt_id: str,
    attempt_number: int,
    started_at: datetime,
    finished_at: datetime,
) -> None:
    manifest_path = state.data_dir / "bronze" / "stage_run_manifest.parquet"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    input_refs = [state.artifacts[name].to_dict() for name in spec.required_inputs if name in state.artifacts]
    output_refs = [ref.to_dict() for ref in result.outputs]
    record = {
        "run_id": state.run_id,
        "parent_run_id": state.parent_run_id or "",
        "attempt_id": attempt_id,
        "attempt_number": attempt_number,
        "stage_id": spec.stage_id,
        "stage_name": spec.stage_name,
        "stage_order": spec.order,
        "status": result.status,
        "started_at": started_at,
        "finished_at": finished_at,
        "elapsed_s": (finished_at - started_at).total_seconds(),
        "refresh_remote": int(state.refresh_remote),
        "resume_from_stage_id": state.resume_from_stage_id or "",
        "selected_stage_ids_json": json.dumps(list(state.selected_stage_ids), ensure_ascii=False),
        "code_version": state.code_version,
        "dataset_version": state.dataset_version,
        "parser_version": "",
        "pipeline_args_json": json.dumps(
            {
                "refresh_remote": state.refresh_remote,
                "parent_run_id": state.parent_run_id or "",
                "resume_from_stage_id": state.resume_from_stage_id or "",
                "selected_stage_ids": list(state.selected_stage_ids),
            },
            ensure_ascii=False,
        ),
        "stage_config_json": json.dumps(state.config.get(spec.stage_id, {}), ensure_ascii=False),
        "input_artifacts_json": json.dumps(input_refs, ensure_ascii=False),
        "output_artifacts_json": json.dumps(output_refs, ensure_ascii=False),
        "input_digest_json": json.dumps({item["name"]: item.get("checksum_sha256", "") for item in input_refs}, ensure_ascii=False),
        "output_digest_json": json.dumps({item["name"]: item.get("checksum_sha256", "") for item in output_refs}, ensure_ascii=False),
        "row_count_summary_json": json.dumps(result.row_count_summary, ensure_ascii=False),
        "warning_count": len(result.warnings),
        "error_message": result.error_message,
        "exception_traceback": result.exception_traceback,
        "notes": result.notes,
    }
    frame = pd.DataFrame([record])
    _write_manifest_frame(frame, manifest_path, state.run_id)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _restore_parent_artifacts(state: BuildState, specs: list[StageSpec]) -> None:
    if not state.parent_run_id:
        return
    manifest = _load_manifest(state.data_dir / "bronze" / "stage_run_manifest.parquet")
    if manifest.empty or "run_id" not in manifest.columns:
        return
    parent = manifest.loc[manifest["run_id"].fillna("").astype(str) == state.parent_run_id].copy()
    if parent.empty:
        return
    if "status" in parent.columns:
        parent = parent.loc[parent["status"].fillna("").astype(str).isin({"succeeded", "degraded"})]
    if parent.empty:
        return

    resume_order = _resume_order(state, specs)
    if resume_order is not None and "stage_order" in parent.columns:
        parent = parent.loc[pd.to_numeric(parent["stage_order"], errors="coerce") < resume_order]
    if parent.empty:
        return

    parent = parent.sort_values(["stage_order", "attempt_number"], kind="stable")
    for payload in parent.get("output_artifacts_json", pd.Series(dtype="object")).fillna("").astype(str):
        if not payload:
            continue
        try:
            refs = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if not isinstance(refs, list):
            continue
        for item in refs:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            if not name:
                continue
            path_text = str(item.get("path", "") or "").strip()
            ref = ArtifactRef(
                name=name,
                path=Path(path_text) if path_text else None,
                kind=str(item.get("kind", "") or "logical"),
                exists=bool(item.get("exists", True)),
                row_count=_optional_int(item.get("row_count")),
                checksum_sha256=str(item.get("checksum_sha256", "") or ""),
                notes=str(item.get("notes", "") or ""),
            )
            state.artifacts[name] = ref.refresh()


def _resume_order(state: BuildState, specs: list[StageSpec]) -> int | None:
    if not state.resume_from_stage_id:
        return None
    by_id = {spec.stage_id: spec.order for spec in specs}
    if state.resume_from_stage_id not in by_id:
        raise ValueError(f"Unknown resume_from_stage_id: {state.resume_from_stage_id}")
    return by_id[state.resume_from_stage_id]


def _load_manifest(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _optional_int(value: object) -> int | None:
    if value in {None, ""}:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _write_manifest_frame(frame: pd.DataFrame, manifest_path: Path, run_id: str) -> None:
    for attempt in range(6):
        try:
            output = frame
            if manifest_path.exists():
                existing = pd.read_parquet(manifest_path)
                output = pd.concat([existing, frame], ignore_index=True)
            output.to_parquet(manifest_path, index=False)
            return
        except PermissionError:
            if attempt == 5:
                break
            time.sleep(0.5 * (2**attempt))

    fallback_path = manifest_path.with_name(f"stage_run_manifest_{run_id}.jsonl")
    fallback_path.parent.mkdir(parents=True, exist_ok=True)
    text = frame.to_json(orient="records", lines=True, force_ascii=False)
    with fallback_path.open("a", encoding="utf-8") as handle:
        handle.write(text)
        if text and not text.endswith("\n"):
            handle.write("\n")
