"""Shared build state for the stage-oriented pipeline."""

from __future__ import annotations

import subprocess
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from r_physgen_db.constants import DATA_DIR, PROJECT_ROOT
from r_physgen_db.pipeline_stages.artifacts import ArtifactRef


@dataclass(slots=True)
class BuildState:
    refresh_remote: bool = False
    project_root: Path = PROJECT_ROOT
    data_dir: Path = DATA_DIR
    run_id: str = field(default_factory=lambda: f"run_{uuid.uuid4().hex[:12]}")
    parent_run_id: str | None = None
    resume_from_stage_id: str | None = None
    selected_stage_ids: tuple[str, ...] = ()
    config: dict[str, Any] = field(default_factory=dict)

    paths: dict[str, Path] = field(default_factory=dict)
    artifacts: dict[str, ArtifactRef] = field(default_factory=dict)
    attempts: dict[str, int] = field(default_factory=dict)
    code_version: str = "unknown"
    dataset_version: str = ""

    seed_catalog: pd.DataFrame = field(default_factory=pd.DataFrame)
    manual_observations: pd.DataFrame = field(default_factory=pd.DataFrame)
    coolprop_aliases: dict[str, str] = field(default_factory=dict)
    bulk_pubchem_lookup: dict[str, dict[str, Any]] = field(default_factory=dict)
    global_sources: dict[str, Any] = field(default_factory=dict)

    source_manifest_rows: list[dict[str, Any]] = field(default_factory=list)
    resolution_rows: list[dict[str, Any]] = field(default_factory=list)
    pending_rows: list[dict[str, Any]] = field(default_factory=list)
    molecule_rows: dict[str, dict[str, Any]] = field(default_factory=dict)
    seed_to_mol_id: dict[str, str] = field(default_factory=dict)
    alias_rows: list[dict[str, Any]] = field(default_factory=list)
    property_rows: list[dict[str, Any]] = field(default_factory=list)
    regulatory_rows: list[dict[str, Any]] = field(default_factory=list)

    pubchem: Any = None
    nist: Any = None
    nist_parser: Any = None
    coolprop: Any = None
    epa_gwp_reference_parser: Any = None
    epa_ods_parser: Any = None
    epa_snap_parser: Any = None
    comptox: Any = None

    molecule_core: pd.DataFrame = field(default_factory=pd.DataFrame)
    alias_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    molecule_context: pd.DataFrame = field(default_factory=pd.DataFrame)
    alias_lookup: dict[str, set[str]] = field(default_factory=dict)
    property_observation: pd.DataFrame = field(default_factory=pd.DataFrame)
    observation_condition_set: pd.DataFrame = field(default_factory=pd.DataFrame)
    condition_migration_progress: dict[str, Any] = field(default_factory=dict)
    cycle_case: pd.DataFrame = field(default_factory=pd.DataFrame)
    cycle_operating_point: pd.DataFrame = field(default_factory=pd.DataFrame)
    cycle_summary: dict[str, Any] = field(default_factory=dict)
    proxy_feature_summary: dict[str, Any] = field(default_factory=dict)
    quantum_job: pd.DataFrame = field(default_factory=pd.DataFrame)
    quantum_artifact: pd.DataFrame = field(default_factory=pd.DataFrame)
    quantum_pilot_summary: dict[str, Any] = field(default_factory=dict)
    mixture_core_table: pd.DataFrame = field(default_factory=pd.DataFrame)
    mixture_composition: pd.DataFrame = field(default_factory=pd.DataFrame)
    mixture_summary: dict[str, Any] = field(default_factory=dict)
    active_learning_queue: pd.DataFrame = field(default_factory=pd.DataFrame)
    active_learning_decision_log: pd.DataFrame = field(default_factory=pd.DataFrame)
    active_learning_summary: dict[str, Any] = field(default_factory=dict)
    qc_issues: pd.DataFrame = field(default_factory=pd.DataFrame)
    regulatory_status: pd.DataFrame = field(default_factory=pd.DataFrame)
    property_recommended: pd.DataFrame = field(default_factory=pd.DataFrame)
    structure_features: pd.DataFrame = field(default_factory=pd.DataFrame)
    property_matrix: pd.DataFrame = field(default_factory=pd.DataFrame)
    model_dataset_index: pd.DataFrame = field(default_factory=pd.DataFrame)
    molecule_master: pd.DataFrame = field(default_factory=pd.DataFrame)
    model_ready: pd.DataFrame = field(default_factory=pd.DataFrame)
    source_manifest: pd.DataFrame = field(default_factory=pd.DataFrame)
    resolution_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    pending_sources: pd.DataFrame = field(default_factory=pd.DataFrame)

    bundle_integration: dict[str, Any] = field(default_factory=dict)
    canonical_observation: pd.DataFrame = field(default_factory=pd.DataFrame)
    canonical_recommended: pd.DataFrame = field(default_factory=pd.DataFrame)
    canonical_recommended_strict: pd.DataFrame = field(default_factory=pd.DataFrame)
    canonical_review_queue: pd.DataFrame = field(default_factory=pd.DataFrame)
    property_governance_audit: dict[str, Any] = field(default_factory=dict)
    research_task_readiness_report: pd.DataFrame = field(default_factory=pd.DataFrame)
    research_task_readiness_summary: dict[str, Any] = field(default_factory=dict)

    report: dict[str, Any] = field(default_factory=dict)

    def next_attempt(self, stage_id: str) -> tuple[str, int]:
        number = self.attempts.get(stage_id, 0) + 1
        self.attempts[stage_id] = number
        return f"{self.run_id}_{stage_id}_attempt{number}", number

    def register_artifact(self, ref: ArtifactRef) -> ArtifactRef:
        self.artifacts[ref.name] = ref.refresh()
        return self.artifacts[ref.name]

    def logical_artifact(self, name: str, *, row_count: int | None = None, notes: str = "") -> ArtifactRef:
        return ArtifactRef(name=name, kind="logical", exists=True, row_count=row_count, notes=notes)

    def file_artifact(self, name: str, path: Path, *, kind: str = "file", row_count: int | None = None, notes: str = "") -> ArtifactRef:
        return ArtifactRef(name=name, path=path, kind=kind, row_count=row_count, notes=notes).refresh()

    def autofill_versions(self) -> None:
        try:
            result = subprocess.run(
                ["git", "describe", "--tags", "--always"],
                cwd=str(self.project_root),
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            self.code_version = result.stdout.strip() or "unknown"
        except Exception:
            self.code_version = "unknown"

        version_path = self.project_root / "data" / "gold" / "VERSION"
        if version_path.exists():
            self.dataset_version = version_path.read_text(encoding="utf-8").strip()
        else:
            self.dataset_version = "v1.5.0-draft"
