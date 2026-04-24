"""Blueprint for stage-oriented R-PhysGen-DB pipeline refactor.

This is a P0 blueprint, not production code. It demonstrates the target
orchestrator semantics before editing the live `src/r_physgen_db/pipeline.py`.

v3 improvements:
- Stage manifest supports attempt_id / attempt_number.
- Input guard supports both file and logical artifacts.
- Produced outputs are registered automatically after each stage.
- Manifest rows are written in finally blocks for both explicit and exception failures.
- code_version and dataset_version are auto-filled.
- Stage 07 substeps can degrade without failing the whole stage.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import logging
import subprocess
import traceback
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core structures
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class ArtifactRef:
    name: str
    path: Path | None = None
    kind: str = "file"  # file | table | logical
    exists: bool = False
    row_count: int | None = None
    checksum_sha256: str = ""
    notes: str = ""

    def to_manifest(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": "" if self.path is None else str(self.path),
            "kind": self.kind,
            "exists": self.exists,
            "row_count": self.row_count,
            "checksum_sha256": self.checksum_sha256,
            "notes": self.notes,
        }


@dataclass(slots=True)
class StageResult:
    stage_id: str
    status: str  # succeeded | failed | skipped | degraded
    outputs: list[ArtifactRef] = field(default_factory=list)
    row_count_summary: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    notes: str = ""
    error_message: str = ""
    exception_traceback: str = ""


@dataclass(slots=True)
class BuildContext:
    project_root: Path
    data_dir: Path
    run_id: str
    refresh_remote: bool = False
    parent_run_id: str | None = None
    resume_from_stage_id: str | None = None
    selected_stage_ids: tuple[str, ...] = ()
    config: dict[str, Any] = field(default_factory=dict)
    artifacts: dict[str, ArtifactRef] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)
    attempts: dict[str, int] = field(default_factory=dict)

    def register_artifact(self, ref: ArtifactRef) -> ArtifactRef:
        if ref.path is not None and ref.kind in {"file", "table"}:
            ref.exists = ref.path.exists()
            if ref.exists and ref.path.is_file():
                ref.checksum_sha256 = _sha256_file(ref.path)
        elif ref.kind == "logical":
            ref.exists = True
        self.artifacts[ref.name] = ref
        return ref

    def get_artifact(self, name: str) -> ArtifactRef | None:
        return self.artifacts.get(name)

    def next_attempt(self, stage_id: str) -> tuple[str, int]:
        number = self.attempts.get(stage_id, 0) + 1
        self.attempts[stage_id] = number
        return f"{self.run_id}_{stage_id}_attempt{number}", number


StageCallable = Callable[[BuildContext], StageResult]


@dataclass(slots=True)
class StageSpec:
    stage_id: str
    stage_name: str
    order: int
    func: StageCallable
    required_inputs: tuple[str, ...] = ()
    produced_outputs: tuple[str, ...] = ()
    allow_missing_inputs_when_selected: bool = False


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def _utc_now() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


def _iso(ts: _dt.datetime) -> str:
    return ts.isoformat()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _placeholder_artifact(ctx: BuildContext, name: str, relative_path: str, payload: dict[str, Any]) -> ArtifactRef:
    path = ctx.data_dir / "_p0_blueprint" / relative_path
    _write_json(path, payload)
    return ArtifactRef(name=name, path=path, kind="file", notes="P0 blueprint placeholder")


def _auto_fill_versions(ctx: BuildContext) -> None:
    try:
        result = subprocess.run(
            ["git", "describe", "--tags", "--always"],
            cwd=str(ctx.project_root),
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        ctx.state["code_version"] = result.stdout.strip() or "unknown"
    except Exception:
        ctx.state["code_version"] = "unknown"

    version_file = ctx.project_root / "VERSION"
    if version_file.exists():
        ctx.state["dataset_version"] = version_file.read_text(encoding="utf-8").strip()
    else:
        today = _utc_now().strftime("%Y%m%d")
        ctx.state["dataset_version"] = f"v1.5.0-{today}-draft"


# ---------------------------------------------------------------------------
# Manifest persistence
# ---------------------------------------------------------------------------

def _persist_stage_manifest(
    ctx: BuildContext,
    spec: StageSpec,
    result: StageResult,
    *,
    attempt_id: str,
    attempt_number: int,
    started_at: _dt.datetime,
    finished_at: _dt.datetime,
) -> None:
    """Write a stage execution manifest row.

    Production implementation should write Parquet to
    data/bronze/stage_run_manifest.parquet. Blueprint writes JSONL for easy inspection.
    """
    manifest_path = ctx.data_dir / "bronze" / "stage_run_manifest.blueprint.jsonl"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    input_refs = [
        ctx.get_artifact(name).to_manifest() if ctx.get_artifact(name) else {"name": name, "missing": True}
        for name in spec.required_inputs
    ]
    output_refs = [ref.to_manifest() for ref in result.outputs]

    record = {
        "run_id": ctx.run_id,
        "parent_run_id": ctx.parent_run_id or "",
        "attempt_id": attempt_id,
        "attempt_number": attempt_number,
        "stage_id": spec.stage_id,
        "stage_name": spec.stage_name,
        "stage_order": spec.order,
        "status": result.status,
        "started_at": _iso(started_at),
        "finished_at": _iso(finished_at),
        "elapsed_s": (finished_at - started_at).total_seconds(),
        "refresh_remote": int(ctx.refresh_remote),
        "resume_from_stage_id": ctx.resume_from_stage_id or "",
        "selected_stage_ids_json": json.dumps(list(ctx.selected_stage_ids), ensure_ascii=False),
        "code_version": ctx.state.get("code_version", ""),
        "dataset_version": ctx.state.get("dataset_version", ""),
        "parser_version": ctx.state.get("parser_version", ""),
        "pipeline_args_json": json.dumps(ctx.config.get("pipeline_args", {}), ensure_ascii=False),
        "stage_config_json": json.dumps(ctx.config.get("stages", {}).get(spec.stage_id, {}), ensure_ascii=False),
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

    with manifest_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def _check_required_inputs(ctx: BuildContext, spec: StageSpec) -> StageResult | None:
    missing: list[str] = []
    missing_files: list[str] = []
    for input_name in spec.required_inputs:
        artifact = ctx.get_artifact(input_name)
        if artifact is None:
            missing.append(input_name)
            continue
        if artifact.kind in {"file", "table"} and artifact.path is not None and not artifact.path.exists():
            missing_files.append(f"{input_name}:{artifact.path}")

    if missing or missing_files:
        return StageResult(
            stage_id=spec.stage_id,
            status="failed",
            error_message="Required inputs missing",
            notes=f"missing={missing}; missing_files={missing_files}",
        )
    return None


# ---------------------------------------------------------------------------
# Stage implementations: placeholders with real artifact semantics
# ---------------------------------------------------------------------------

def stage00_init_run(ctx: BuildContext) -> StageResult:
    for sub in ("bronze", "silver", "gold", "extensions", "_p0_blueprint"):
        (ctx.data_dir / sub).mkdir(parents=True, exist_ok=True)
    _auto_fill_versions(ctx)
    outputs = [
        ArtifactRef(name="pipeline_context", kind="logical", notes="Initialized BuildContext"),
    ]
    return StageResult(
        stage_id="00",
        status="succeeded",
        outputs=outputs,
        notes=f"code_version={ctx.state.get('code_version')}; dataset_version={ctx.state.get('dataset_version')}",
    )


def stage01_load_inventory(ctx: BuildContext) -> StageResult:
    outputs = [
        _placeholder_artifact(ctx, "seed_catalog", "stage01_seed_catalog.json", {"stage": "01", "artifact": "seed_catalog"}),
        _placeholder_artifact(ctx, "manual_observations", "stage01_manual_observations.json", {"stage": "01", "artifact": "manual_observations"}),
        _placeholder_artifact(ctx, "manual_sources", "stage01_manual_sources.json", {"stage": "01", "artifact": "manual_sources"}),
    ]
    return StageResult(stage_id="01", status="succeeded", outputs=outputs, notes="load_inventory skeleton")


def stage02_resolve_identity(ctx: BuildContext) -> StageResult:
    outputs = [
        _placeholder_artifact(ctx, "molecule_core", "stage02_molecule_core.json", {"stage": "02", "artifact": "molecule_core"}),
        _placeholder_artifact(ctx, "molecule_alias", "stage02_molecule_alias.json", {"stage": "02", "artifact": "molecule_alias"}),
    ]
    return StageResult(stage_id="02", status="succeeded", outputs=outputs, notes="resolve_identity skeleton")


def stage03_acquire_global_sources(ctx: BuildContext) -> StageResult:
    outputs = [
        _placeholder_artifact(ctx, "epa_gwp_table", "stage03_epa_gwp_table.json", {"stage": "03"}),
        _placeholder_artifact(ctx, "epa_ods_table", "stage03_epa_ods_table.json", {"stage": "03"}),
        _placeholder_artifact(ctx, "epa_snap_cache", "stage03_epa_snap_cache.json", {"stage": "03"}),
    ]
    return StageResult(
        stage_id="03",
        status="succeeded",
        outputs=outputs,
        notes="Stage 03 caches global sources only; entity mapping occurs in Stage 05.",
    )


def stage04_acquire_entity_sources(ctx: BuildContext) -> StageResult:
    outputs = [
        _placeholder_artifact(ctx, "nist_snapshots", "stage04_nist_snapshots.json", {"stage": "04"}),
        _placeholder_artifact(ctx, "coolprop_observations", "stage04_coolprop_observations.json", {"stage": "04"}),
        _placeholder_artifact(ctx, "seed_resolution", "stage04_seed_resolution.json", {"stage": "04"}),
    ]
    return StageResult(stage_id="04", status="succeeded", outputs=outputs, notes="acquire_entity_sources skeleton")


def stage05_harmonize_observations(ctx: BuildContext) -> StageResult:
    outputs = [
        _placeholder_artifact(ctx, "property_observation", "stage05_property_observation.json", {"stage": "05"}),
        _placeholder_artifact(ctx, "observation_condition_set", "stage05_observation_condition_set.json", {"stage": "05"}),
        _placeholder_artifact(ctx, "qc_issues", "stage05_qc_issues.json", {"stage": "05"}),
    ]
    return StageResult(
        stage_id="05",
        status="succeeded",
        outputs=outputs,
        row_count_summary={"property_observation": 0, "observation_condition_set": 0},
        notes="harmonize_observations skeleton; extracts entity-level rows from global EPA/SNAP caches",
    )


def stage06_integrate_governance_bundle(ctx: BuildContext) -> StageResult:
    outputs = [
        _placeholder_artifact(ctx, "canonical_overlay", "stage06_canonical_overlay.json", {"stage": "06"}),
        _placeholder_artifact(ctx, "strict_layer", "stage06_strict_layer.json", {"stage": "06"}),
        _placeholder_artifact(ctx, "review_queue", "stage06_review_queue.json", {"stage": "06"}),
        _placeholder_artifact(ctx, "extension_mirror", "stage06_extension_mirror.json", {"stage": "06"}),
    ]
    return StageResult(stage_id="06", status="succeeded", outputs=outputs, notes="integrate_governance_bundle skeleton")


def stage07_build_feature_and_recommendation_layers(ctx: BuildContext) -> StageResult:
    substep_status: dict[str, str] = {}
    warnings: list[str] = []
    outputs: list[ArtifactRef] = []

    for substep, artifact_name in [
        ("property_recommended", "property_recommended"),
        ("structure_features", "structure_features"),
        ("molecule_master_and_property_matrix", "molecule_master"),
        ("property_matrix", "property_matrix"),
    ]:
        try:
            outputs.append(_placeholder_artifact(ctx, artifact_name, f"stage07_{artifact_name}.json", {"stage": "07", "substep": substep}))
            substep_status[substep] = "succeeded"
        except Exception as exc:  # noqa: BLE001 - blueprint wants defensive semantics
            substep_status[substep] = f"failed:{exc}"
            warnings.append(f"{substep} failed: {exc}")

    if all(v == "succeeded" for v in substep_status.values()):
        status = "succeeded"
    elif any(v == "succeeded" for v in substep_status.values()):
        status = "degraded"
    else:
        status = "failed"

    return StageResult(
        stage_id="07",
        status=status,
        outputs=outputs,
        warnings=warnings,
        notes=f"substep_status={substep_status}",
    )


def stage08_build_model_outputs(ctx: BuildContext) -> StageResult:
    outputs = [
        _placeholder_artifact(ctx, "molecule_split_definition", "stage08_molecule_split_definition.json", {"stage": "08"}),
        _placeholder_artifact(ctx, "model_dataset_index", "stage08_model_dataset_index.json", {"stage": "08"}),
        _placeholder_artifact(ctx, "model_ready", "stage08_model_ready.json", {"stage": "08"}),
    ]
    return StageResult(stage_id="08", status="succeeded", outputs=outputs, notes="build_model_outputs skeleton")


def stage09_validate_and_publish(ctx: BuildContext) -> StageResult:
    outputs = [
        _placeholder_artifact(ctx, "validation_report", "stage09_validation_report.json", {"stage": "09"}),
        _placeholder_artifact(ctx, "quality_report", "stage09_quality_report.json", {"stage": "09"}),
        _placeholder_artifact(ctx, "duckdb_index", "stage09_duckdb_index.json", {"stage": "09"}),
    ]
    return StageResult(stage_id="09", status="succeeded", outputs=outputs, notes="validate_and_publish skeleton")


STAGES: tuple[StageSpec, ...] = (
    StageSpec("00", "init_run", 0, stage00_init_run, produced_outputs=("pipeline_context",)),
    StageSpec("01", "load_inventory", 1, stage01_load_inventory, required_inputs=("pipeline_context",), produced_outputs=("seed_catalog", "manual_observations", "manual_sources")),
    StageSpec("02", "resolve_identity", 2, stage02_resolve_identity, required_inputs=("seed_catalog",), produced_outputs=("molecule_core", "molecule_alias")),
    StageSpec("03", "acquire_global_sources", 3, stage03_acquire_global_sources, required_inputs=("pipeline_context",), produced_outputs=("epa_gwp_table", "epa_ods_table", "epa_snap_cache")),
    StageSpec("04", "acquire_entity_sources", 4, stage04_acquire_entity_sources, required_inputs=("seed_catalog", "molecule_core"), produced_outputs=("nist_snapshots", "coolprop_observations", "seed_resolution")),
    StageSpec("05", "harmonize_observations", 5, stage05_harmonize_observations, required_inputs=("manual_observations", "epa_gwp_table", "epa_ods_table", "epa_snap_cache", "nist_snapshots", "coolprop_observations"), produced_outputs=("property_observation", "observation_condition_set", "qc_issues")),
    StageSpec("06", "integrate_governance_bundle", 6, stage06_integrate_governance_bundle, required_inputs=("molecule_core", "molecule_alias", "property_observation"), produced_outputs=("canonical_overlay", "strict_layer", "review_queue", "extension_mirror")),
    StageSpec("07", "build_feature_and_recommendation_layers", 7, stage07_build_feature_and_recommendation_layers, required_inputs=("property_observation", "canonical_overlay", "molecule_core"), produced_outputs=("property_recommended", "structure_features", "molecule_master", "property_matrix")),
    StageSpec("08", "build_model_outputs", 8, stage08_build_model_outputs, required_inputs=("molecule_master", "property_matrix"), produced_outputs=("molecule_split_definition", "model_dataset_index", "model_ready")),
    StageSpec("09", "validate_and_publish", 9, stage09_validate_and_publish, required_inputs=("model_ready",), produced_outputs=("validation_report", "quality_report", "duckdb_index")),
)


def build_dataset_staged(
    project_root: Path,
    data_dir: Path,
    run_id: str | None = None,
    *,
    refresh_remote: bool = False,
    selected_stage_ids: Iterable[str] | None = None,
    stop_after: str | None = None,
    resume_from_stage_id: str | None = None,
    parent_run_id: str | None = None,
    config: dict[str, Any] | None = None,
) -> list[StageResult]:
    selected_tuple = tuple(selected_stage_ids or ())
    ctx = BuildContext(
        project_root=project_root,
        data_dir=data_dir,
        run_id=run_id or f"run_{uuid.uuid4().hex[:12]}",
        refresh_remote=refresh_remote,
        parent_run_id=parent_run_id,
        resume_from_stage_id=resume_from_stage_id,
        selected_stage_ids=selected_tuple,
        config=config or {},
    )

    enabled = set(selected_tuple) if selected_tuple else None
    results: list[StageResult] = []

    for spec in STAGES:
        if enabled is not None and spec.stage_id not in enabled:
            continue

        attempt_id, attempt_number = ctx.next_attempt(spec.stage_id)
        started_at = _utc_now()

        guard_result = _check_required_inputs(ctx, spec)
        if guard_result is not None:
            finished_at = _utc_now()
            _persist_stage_manifest(ctx, spec, guard_result, attempt_id=attempt_id, attempt_number=attempt_number, started_at=started_at, finished_at=finished_at)
            results.append(guard_result)
            break

        try:
            result = spec.func(ctx)
        except Exception as exc:  # noqa: BLE001
            result = StageResult(
                stage_id=spec.stage_id,
                status="failed",
                error_message=str(exc),
                exception_traceback=traceback.format_exc(),
                notes=f"Unhandled exception in stage {spec.stage_name}",
            )

        # Register outputs even for degraded stages; failed stages may still emit diagnostics.
        for ref in result.outputs:
            ctx.register_artifact(ref)

        finished_at = _utc_now()
        _persist_stage_manifest(ctx, spec, result, attempt_id=attempt_id, attempt_number=attempt_number, started_at=started_at, finished_at=finished_at)
        results.append(result)

        if result.status not in {"succeeded", "degraded"}:
            break
        if stop_after is not None and spec.stage_id == stop_after:
            break

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    results = build_dataset_staged(
        project_root=Path(".").resolve(),
        data_dir=Path("./data").resolve(),
        refresh_remote=False,
    )
    for item in results:
        print(f"{item.stage_id}: {item.status} - {item.notes}")
        for warning in item.warnings:
            print(f"  warning: {warning}")
        if item.error_message:
            print(f"  error: {item.error_message}")
