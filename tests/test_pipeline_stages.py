from __future__ import annotations

import inspect

import pandas as pd
import pytest

from r_physgen_db import pipeline
from r_physgen_db.pipeline import _assign_scaffold_splits, _load_or_fetch_json, build_dataset
from r_physgen_db.constants import DATA_DIR
from r_physgen_db.pipeline_stages.artifacts import StageResult
from r_physgen_db.pipeline_stages.context import BuildState
from r_physgen_db.pipeline_stages.orchestrator import StageSpec, run_stages
from r_physgen_db.pipeline_stages.stages import stage00_init_run


def test_build_dataset_public_signature_is_compatible() -> None:
    signature = inspect.signature(build_dataset)
    assert list(signature.parameters) == ["refresh_remote"]
    assert signature.parameters["refresh_remote"].default is False


def test_private_pipeline_helpers_remain_importable() -> None:
    assert callable(_load_or_fetch_json)
    split_map = _assign_scaffold_splits(
        pd.DataFrame(
            [
                {"mol_id": "a", "scaffold_key": "S1"},
                {"mol_id": "b", "scaffold_key": "S1"},
            ]
        )
    )
    assert split_map == {"a": "train", "b": "train"}


def test_source_manifest_entry_caches_duplicate_file_hashes(tmp_path, monkeypatch) -> None:
    local_path = tmp_path / "shared_bulk_source.csv"
    local_path.write_text("seed_id,value\nseed_a,1\n", encoding="utf-8")
    calls = {"count": 0}

    def fake_sha256_file(path):
        calls["count"] += 1
        return f"digest-{path.name}"

    pipeline._SOURCE_MANIFEST_CHECKSUM_CACHE.clear()
    monkeypatch.setattr(pipeline, "sha256_file", fake_sha256_file)

    first = pipeline._source_manifest_entry(
        source_id="source_a",
        source_type="derived_harmonized",
        source_name="Bulk",
        license_name="project",
        local_path=local_path,
        upstream_url="",
        status="generated",
    )
    second = pipeline._source_manifest_entry(
        source_id="source_b",
        source_type="derived_harmonized",
        source_name="Bulk",
        license_name="project",
        local_path=local_path,
        upstream_url="",
        status="generated",
    )

    assert calls["count"] == 1
    assert first["checksum_sha256"] == second["checksum_sha256"] == "digest-shared_bulk_source.csv"


def test_required_input_guard_writes_failed_manifest(tmp_path) -> None:
    state = BuildState(data_dir=tmp_path / "data", run_id="run_missing_input")
    specs = (
        StageSpec(
            "01",
            "needs_missing",
            1,
            lambda _: StageResult(stage_id="01", status="succeeded"),
            required_inputs=("missing_artifact",),
        ),
    )

    results = run_stages(state, specs)

    assert results[0].status == "failed"
    manifest = pd.read_parquet(tmp_path / "data" / "bronze" / "stage_run_manifest.parquet")
    assert manifest.iloc[0]["status"] == "failed"
    assert manifest.iloc[0]["error_message"] == "Required inputs missing"


def test_unhandled_stage_exception_writes_failed_manifest(tmp_path) -> None:
    state = BuildState(data_dir=tmp_path / "data", run_id="run_exception")

    def boom(_: BuildState) -> StageResult:
        raise ValueError("stage exploded")

    specs = (StageSpec("00", "boom", 0, boom),)
    results = run_stages(state, specs)

    assert results[0].status == "failed"
    manifest = pd.read_parquet(tmp_path / "data" / "bronze" / "stage_run_manifest.parquet")
    assert manifest.iloc[0]["error_message"] == "stage exploded"
    assert "ValueError" in manifest.iloc[0]["exception_traceback"]


def test_stage00_infers_tmp_data_dir_from_monkeypatched_paths(tmp_path, monkeypatch) -> None:
    from r_physgen_db import pipeline as legacy

    tmp_data_dir = tmp_path / "data"
    original_paths = legacy._paths()

    def remap(path):
        try:
            return tmp_data_dir / path.relative_to(DATA_DIR)
        except ValueError:
            return path

    monkeypatch.setattr(legacy, "_paths", lambda: {key: remap(path) for key, path in original_paths.items()})

    state = BuildState(data_dir=DATA_DIR, run_id="run_monkeypatched_paths")

    stage00_init_run(state)

    assert state.data_dir == tmp_data_dir
    assert state.paths["bronze_source_manifest"] == tmp_data_dir / "bronze" / "source_manifest.parquet"


def test_stop_after_stops_after_requested_stage(tmp_path) -> None:
    state = BuildState(data_dir=tmp_path / "data", run_id="run_stop_after")

    def stage(stage_id: str):
        return lambda ctx: StageResult(stage_id=stage_id, status="succeeded", outputs=[ctx.logical_artifact(stage_id)])

    specs = (
        StageSpec("00", "first", 0, stage("00")),
        StageSpec("01", "second", 1, stage("01")),
    )
    results = run_stages(state, specs, stop_after="00")

    assert [result.stage_id for result in results] == ["00"]
    manifest = pd.read_parquet(tmp_path / "data" / "bronze" / "stage_run_manifest.parquet")
    assert manifest["stage_id"].tolist() == ["00"]


def test_stage_resume_restores_parent_manifest_artifacts(tmp_path) -> None:
    data_dir = tmp_path / "data"
    artifact_path = data_dir / "silver" / "input.parquet"

    def produce(ctx: BuildState) -> StageResult:
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([{"value": 1}]).to_parquet(artifact_path, index=False)
        return StageResult(stage_id="00", status="succeeded", outputs=[ctx.file_artifact("input_table", artifact_path, kind="table")])

    def consume(ctx: BuildState) -> StageResult:
        assert "input_table" in ctx.artifacts
        return StageResult(stage_id="01", status="succeeded", outputs=[ctx.logical_artifact("consumed")])

    specs = (
        StageSpec("00", "produce", 0, produce, produced_outputs=("input_table",)),
        StageSpec("01", "consume", 1, consume, required_inputs=("input_table",), produced_outputs=("consumed",)),
    )
    parent_state = BuildState(data_dir=data_dir, run_id="run_parent")
    run_stages(parent_state, specs, stop_after="00")

    resume_state = BuildState(data_dir=data_dir, run_id="run_resume", parent_run_id="run_parent", resume_from_stage_id="01")
    results = run_stages(resume_state, specs)

    assert [result.stage_id for result in results] == ["01"]
    assert results[0].status == "succeeded"
    manifest = pd.read_parquet(data_dir / "bronze" / "stage_run_manifest.parquet")
    resume_row = manifest.loc[manifest["run_id"].eq("run_resume")].iloc[0]
    assert resume_row["resume_from_stage_id"] == "01"
    assert '"parent_run_id": "run_parent"' in resume_row["pipeline_args_json"]


def test_build_dataset_staged_raises_on_failed_stage(tmp_path) -> None:
    from r_physgen_db.pipeline_stages.orchestrator import build_dataset_staged

    with pytest.raises(RuntimeError, match="Stage 01 failed"):
        build_dataset_staged(selected_stage_ids=("01",), data_dir=tmp_path / "data", run_id="run_selected_missing")


def test_stage_registry_exposes_p0_followup_outputs() -> None:
    from r_physgen_db.pipeline_stages.stages import STAGES

    by_id = {stage.stage_id: stage for stage in STAGES}

    assert {"mixture_core", "mixture_composition"}.issubset(set(by_id["06"].produced_outputs))
    assert {"quantum_pilot_observation", "quantum_job", "quantum_artifact"}.issubset(set(by_id["05"].produced_outputs))
    assert {
        "quantum_job",
        "quantum_artifact",
        "mixture_core",
        "mixture_composition",
        "active_learning_queue",
        "active_learning_decision_log",
        "dataset_version",
    }.issubset(set(by_id["09"].produced_outputs))
