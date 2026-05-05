"""Blueprint for stage-oriented R-PhysGen-DB pipeline refactor.

This file is intentionally a draft blueprint rather than production code.
Its purpose is to make the P0 stage split concrete before editing the live
`src/r_physgen_db/pipeline.py`.

P0 Review 优化记录：
- 问题 17：补充 orchestrator 的异常捕获 + manifest 写回机制
- 问题 18：补充 required_inputs guard 检查
- 问题 20：补充 code_version 自动填充（git describe）
- 问题 21：Stage 07 内部子步骤独立失败处理注释
"""

from __future__ import annotations

import datetime
import json
import logging
import subprocess
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core data structures
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class ArtifactRef:
    name: str
    path: Path
    exists: bool = False
    notes: str = ""


@dataclass(slots=True)
class StageResult:
    stage_id: str
    status: str  # succeeded | failed | skipped
    outputs: list[ArtifactRef] = field(default_factory=list)
    row_count_summary: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    notes: str = ""
    error_message: str = ""


@dataclass(slots=True)
class BuildContext:
    project_root: Path
    data_dir: Path
    run_id: str
    refresh_remote: bool = False
    config: dict[str, Any] = field(default_factory=dict)
    artifacts: dict[str, ArtifactRef] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)

    def register_artifact(self, name: str, path: Path, notes: str = "") -> ArtifactRef:
        ref = ArtifactRef(name=name, path=path, exists=path.exists(), notes=notes)
        self.artifacts[name] = ref
        return ref

    def get_artifact(self, name: str) -> ArtifactRef | None:
        return self.artifacts.get(name)


StageCallable = Callable[[BuildContext], StageResult]


@dataclass(slots=True)
class StageSpec:
    stage_id: str
    stage_name: str
    order: int
    func: StageCallable
    required_inputs: tuple[str, ...] = ()
    produced_outputs: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Manifest persistence (Issue 17: 无论成功失败都写回 manifest)
# ---------------------------------------------------------------------------

def _persist_stage_manifest(
    ctx: BuildContext,
    spec: StageSpec,
    result: StageResult,
    started_at: str,
) -> None:
    """Write a stage execution record to stage_run_manifest.

    In production this would append to a Parquet file or database table.
    Blueprint version writes a JSON-lines file for demonstration.
    """
    manifest_path = ctx.data_dir / "bronze" / "stage_run_manifest.jsonl"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    record = {
        "run_id": ctx.run_id,
        "stage_id": spec.stage_id,
        "stage_name": spec.stage_name,
        "stage_order": spec.order,
        "status": result.status,
        "started_at": started_at,
        "finished_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "refresh_remote": int(ctx.refresh_remote),
        "code_version": ctx.state.get("code_version", ""),
        "dataset_version": ctx.state.get("dataset_version", ""),
        "input_artifacts_json": json.dumps(list(spec.required_inputs)),
        "output_artifacts_json": json.dumps(list(spec.produced_outputs)),
        "row_count_summary_json": json.dumps(result.row_count_summary),
        "warning_count": len(result.warnings),
        "error_message": result.error_message or "",
        "notes": result.notes,
    }
    with open(manifest_path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    logger.info(
        "Manifest written: run=%s stage=%s status=%s",
        ctx.run_id, spec.stage_id, result.status,
    )


# ---------------------------------------------------------------------------
# Input guard (Issue 18: required_inputs 依赖检查)
# ---------------------------------------------------------------------------

def _check_required_inputs(
    ctx: BuildContext,
    spec: StageSpec,
) -> StageResult | None:
    """Return a failed StageResult if any required input artifact is missing.

    Returns None if all inputs are satisfied.
    """
    for input_name in spec.required_inputs:
        artifact = ctx.get_artifact(input_name)
        if artifact is None:
            msg = f"Missing required input artifact: {input_name}"
            logger.error("Stage %s blocked: %s", spec.stage_id, msg)
            return StageResult(
                stage_id=spec.stage_id,
                status="failed",
                error_message=msg,
                notes=f"Input guard failed — {input_name} not registered in context",
            )
        if not artifact.exists:
            msg = f"Required input artifact registered but file missing: {input_name} ({artifact.path})"
            logger.error("Stage %s blocked: %s", spec.stage_id, msg)
            return StageResult(
                stage_id=spec.stage_id,
                status="failed",
                error_message=msg,
                notes=f"Input guard failed — {input_name} file not found on disk",
            )
    return None


# ---------------------------------------------------------------------------
# Version auto-fill (Issue 20: code_version / dataset_version)
# ---------------------------------------------------------------------------

def _auto_fill_versions(ctx: BuildContext) -> None:
    """Populate code_version and dataset_version in ctx.state."""
    # code_version from git
    try:
        result = subprocess.run(
            ["git", "describe", "--tags", "--always"],
            capture_output=True,
            text=True,
            cwd=str(ctx.project_root),
            timeout=5,
        )
        ctx.state["code_version"] = result.stdout.strip() or "unknown"
    except Exception:
        ctx.state["code_version"] = "unknown"

    # dataset_version from VERSION file
    version_file = ctx.project_root / "VERSION"
    if version_file.exists():
        ctx.state["dataset_version"] = version_file.read_text(encoding="utf-8").strip()
    else:
        ctx.state["dataset_version"] = "unversioned"


# ---------------------------------------------------------------------------
# Stage implementations (blueprints)
# ---------------------------------------------------------------------------

def stage00_init_run(ctx: BuildContext) -> StageResult:
    # Initialize output directories
    for sub in ("bronze", "silver", "gold"):
        (ctx.data_dir / sub).mkdir(parents=True, exist_ok=True)

    # Auto-fill versions (Issue 20)
    _auto_fill_versions(ctx)

    return StageResult(
        stage_id="00",
        status="succeeded",
        notes=(
            f"init_run complete — "
            f"code_version={ctx.state.get('code_version')}, "
            f"dataset_version={ctx.state.get('dataset_version')}"
        ),
    )


def stage01_load_inventory(ctx: BuildContext) -> StageResult:
    # TODO:
    # - read seed_catalog.csv
    # - read manual_property_observations.csv
    # - read observations/*.csv
    # - read coolprop_aliases.yaml
    # - register input artifacts via ctx.register_artifact(...)
    return StageResult(stage_id="01", status="succeeded", notes="load_inventory blueprint")


def stage02_resolve_identity(ctx: BuildContext) -> StageResult:
    # TODO:
    # - resolve PubChem / bulk PubChem lookup
    # - standardize smiles
    # - build molecule_core and molecule_alias
    return StageResult(stage_id="02", status="succeeded", notes="resolve_identity blueprint")


def stage03_acquire_global_sources(ctx: BuildContext) -> StageResult:
    # TODO:
    # - fetch EPA technology transitions GWP table
    # - fetch EPA ODS table
    # - fetch EPA SNAP pages (全量缓存)
    # - register source_manifest rows for global sources
    #
    # 注意 (Issue 19)：
    #   Stage 03 仅负责全局表的获取和缓存。
    #   entity 级别的 SNAP/EPA 行提取和 mol_id 映射由 Stage 05 harmonize 完成。
    #   Stage 03 不进行分子级别的数据筛选。
    return StageResult(stage_id="03", status="succeeded", notes="acquire_global_sources blueprint")


def stage04_acquire_entity_sources(ctx: BuildContext) -> StageResult:
    # TODO:
    # - fetch NIST snapshots for entity rows
    # - generate CoolProp observations
    # - populate seed_resolution
    return StageResult(stage_id="04", status="succeeded", notes="acquire_entity_sources blueprint")


def stage05_harmonize_observations(ctx: BuildContext) -> StageResult:
    # TODO:
    # - merge manual / EPA / NIST / CoolProp / Excel inputs
    # - extract entity-level rows from global EPA/SNAP cache (Issue 19)
    # - normalize into property_observation
    # - create observation_condition_set where possible
    # - run QC
    return StageResult(stage_id="05", status="succeeded", notes="harmonize_observations blueprint")


def stage06_integrate_governance_bundle(ctx: BuildContext) -> StageResult:
    # TODO:
    # - call integrate_property_governance_bundle(...)
    # - persist extension mirror
    # - persist canonical overlay / strict / review queue
    return StageResult(stage_id="06", status="succeeded", notes="integrate_governance_bundle blueprint")


def stage07_build_feature_and_recommendation_layers(ctx: BuildContext) -> StageResult:
    """Build recommendation, feature, and master layers.

    内部包含三个语义独立的子步骤 (Issue 21):
      (a) property_recommended 构建
      (b) structure_features 计算（RDKit 密集型）
      (c) molecule_master + property_matrix 聚合

    P0 保持合并，但各子步骤应具备独立失败处理。
    单分子 RDKit 解析失败不应阻塞其余子步骤。
    V1.5 计划拆分为 Stage 07a / 07b / 07c。
    """
    sub_results: dict[str, str] = {}

    # Sub-step (a): property_recommended
    try:
        # TODO: build property_recommended
        sub_results["property_recommended"] = "succeeded"
    except Exception as exc:
        logger.warning("Stage 07a (property_recommended) failed: %s", exc)
        sub_results["property_recommended"] = f"failed: {exc}"

    # Sub-step (b): structure_features (RDKit)
    try:
        # TODO: compute structure_features
        sub_results["structure_features"] = "succeeded"
    except Exception as exc:
        logger.warning("Stage 07b (structure_features) failed: %s", exc)
        sub_results["structure_features"] = f"failed: {exc}"

    # Sub-step (c): molecule_master + property_matrix
    try:
        # TODO: build molecule_master and property_matrix
        sub_results["molecule_master_and_matrix"] = "succeeded"
    except Exception as exc:
        logger.warning("Stage 07c (molecule_master_and_matrix) failed: %s", exc)
        sub_results["molecule_master_and_matrix"] = f"failed: {exc}"

    # Determine overall status
    all_ok = all(v == "succeeded" for v in sub_results.values())
    any_ok = any(v == "succeeded" for v in sub_results.values())

    if all_ok:
        status = "succeeded"
    elif any_ok:
        status = "succeeded"  # partial success still allows downstream
    else:
        status = "failed"

    warnings = [
        f"Sub-step {k}: {v}" for k, v in sub_results.items() if v != "succeeded"
    ]

    return StageResult(
        stage_id="07",
        status=status,
        notes=f"Sub-step results: {sub_results}",
        warnings=warnings,
    )


def stage08_build_model_outputs(ctx: BuildContext) -> StageResult:
    # TODO:
    # - build model_dataset_index
    # - build model_ready
    # - keep compatibility with current gold outputs
    return StageResult(stage_id="08", status="succeeded", notes="build_model_outputs blueprint")


def stage09_validate_and_publish(ctx: BuildContext) -> StageResult:
    # TODO:
    # - run validate_dataset()
    # - emit quality_report.json (include dataset_version)
    # - run research_task_readiness checks
    # - write duckdb index
    # - update stage_run_manifest
    return StageResult(stage_id="09", status="succeeded", notes="validate_and_publish blueprint")


# ---------------------------------------------------------------------------
# Stage registry
# ---------------------------------------------------------------------------

STAGES: tuple[StageSpec, ...] = (
    StageSpec("00", "init_run", 0, stage00_init_run),
    StageSpec("01", "load_inventory", 1, stage01_load_inventory,
             produced_outputs=("seed_catalog", "manual_observations")),
    StageSpec("02", "resolve_identity", 2, stage02_resolve_identity,
             required_inputs=("seed_catalog",),
             produced_outputs=("molecule_core", "molecule_alias")),
    StageSpec("03", "acquire_global_sources", 3, stage03_acquire_global_sources,
             produced_outputs=("epa_gwp_table", "epa_ods_table", "epa_snap_cache")),
    StageSpec("04", "acquire_entity_sources", 4, stage04_acquire_entity_sources,
             required_inputs=("seed_catalog",),
             produced_outputs=("nist_snapshots", "coolprop_observations")),
    StageSpec("05", "harmonize_observations", 5, stage05_harmonize_observations,
             required_inputs=("manual_observations", "epa_gwp_table", "nist_snapshots"),
             produced_outputs=("property_observation", "observation_condition_set")),
    StageSpec("06", "integrate_governance_bundle", 6, stage06_integrate_governance_bundle,
             required_inputs=("property_observation",),
             produced_outputs=("canonical_overlay", "strict_layer", "review_queue")),
    StageSpec("07", "build_feature_and_recommendation_layers", 7,
             stage07_build_feature_and_recommendation_layers,
             required_inputs=("property_observation", "canonical_overlay", "molecule_core"),
             produced_outputs=("property_recommended", "structure_features",
                               "molecule_master", "property_matrix")),
    StageSpec("08", "build_model_outputs", 8, stage08_build_model_outputs,
             required_inputs=("molecule_master", "property_matrix"),
             produced_outputs=("model_dataset_index", "model_ready")),
    StageSpec("09", "validate_and_publish", 9, stage09_validate_and_publish,
             required_inputs=("model_ready",),
             produced_outputs=("quality_report", "duckdb_index")),
)


# ---------------------------------------------------------------------------
# Orchestrator (Issues 17 & 18: 异常捕获 + manifest 写回 + input guard)
# ---------------------------------------------------------------------------

def build_dataset_staged(
    project_root: Path,
    data_dir: Path,
    run_id: str,
    *,
    refresh_remote: bool = False,
    selected_stage_ids: Iterable[str] | None = None,
    stop_after: str | None = None,
) -> list[StageResult]:
    ctx = BuildContext(
        project_root=project_root,
        data_dir=data_dir,
        run_id=run_id,
        refresh_remote=refresh_remote,
    )

    enabled = set(selected_stage_ids) if selected_stage_ids is not None else None
    results: list[StageResult] = []

    for spec in STAGES:
        if enabled is not None and spec.stage_id not in enabled:
            continue

        started_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # Issue 18: Input guard — check required artifacts before running
        guard_result = _check_required_inputs(ctx, spec)
        if guard_result is not None:
            _persist_stage_manifest(ctx, spec, guard_result, started_at)
            results.append(guard_result)
            break

        # Issue 17: Try/except with manifest write-back
        try:
            result = spec.func(ctx)
        except Exception as exc:
            logger.exception("Stage %s raised unhandled exception", spec.stage_id)
            result = StageResult(
                stage_id=spec.stage_id,
                status="failed",
                error_message=str(exc),
                notes=f"Unhandled exception:\n{traceback.format_exc()}",
            )
        finally:
            # Issue 17: 无论成功失败都写回 manifest
            _persist_stage_manifest(ctx, spec, result, started_at)

        results.append(result)

        if result.status != "succeeded":
            logger.error(
                "Stage %s failed, stopping pipeline. Error: %s",
                spec.stage_id, result.error_message,
            )
            break

        if stop_after is not None and spec.stage_id == stop_after:
            break

    return results


if __name__ == "__main__":
    # Example only; replace with real CLI integration when adopted.
    import uuid

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    run_id = f"draft-{uuid.uuid4().hex[:8]}"
    results = build_dataset_staged(
        project_root=Path(".").resolve(),
        data_dir=Path("./data").resolve(),
        run_id=run_id,
        refresh_remote=False,
    )
    for item in results:
        status_icon = "OK" if item.status == "succeeded" else "FAIL"
        print(f"[{status_icon}] Stage {item.stage_id}: {item.notes}")
        if item.warnings:
            for w in item.warnings:
                print(f"      WARN: {w}")
        if item.error_message:
            print(f"      ERROR: {item.error_message}")
