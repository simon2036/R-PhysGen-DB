"""Offline quantum pilot ingestion helpers."""

from __future__ import annotations

import hashlib
import errno
import json
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from rdkit import Chem, RDLogger
from rdkit.Chem import AllChem

from r_physgen_db.constants import DATA_DIR, PROJECT_ROOT


QUANTUM_SOURCE_ID = "source_r_physgen_quantum_pilot"
QUANTUM_SOURCE_NAME = "R-PhysGen-DB Quantum Pilot Offline Results"
QUANTUM_ASSESSMENT_VERSION = "pr-f-quantum-pilot-v1"
QUANTUM_ML_USE_STATUS = "long_form_quantum_pilot_not_wide_ml_target"

QUANTUM_FEATURES = {
    "quantum.homo_energy": {"property_name": "homo_ev", "unit": "eV"},
    "quantum.lumo_energy": {"property_name": "lumo_ev", "unit": "eV"},
    "quantum.homo_lumo_gap": {"property_name": "gap_ev", "unit": "eV"},
    "quantum.total_energy": {"property_name": "total_energy_eh", "unit": "Eh"},
    "quantum.dipole_moment": {"property_name": "dipole_moment_debye", "unit": "Debye"},
    "quantum.polarizability": {"property_name": "polarizability_au", "unit": "au"},
}
PHASE2_QUANTUM_FEATURES = {
    "quantum.zpe": {"property_name": "zpe_eh", "unit": "Eh"},
    "quantum.lowest_real_frequency": {"property_name": "lowest_real_frequency_cm_inv", "unit": "cm^-1"},
    "quantum.thermal_enthalpy_correction": {"property_name": "thermal_enthalpy_correction_eh", "unit": "Eh"},
    "quantum.thermal_gibbs_correction": {"property_name": "thermal_gibbs_correction_eh", "unit": "Eh"},
    "quantum.conformer_count": {"property_name": "conformer_count", "unit": "count"},
    "quantum.conformer_energy_window": {"property_name": "conformer_energy_window_kcal_mol", "unit": "kcal/mol"},
}
QUANTUM_CANONICAL_FEATURE_KEYS = set(QUANTUM_FEATURES)
QUANTUM_PROPERTY_NAMES = {item["property_name"] for item in QUANTUM_FEATURES.values()}
QUANTUM_FEATURE_KEYS = QUANTUM_CANONICAL_FEATURE_KEYS | QUANTUM_PROPERTY_NAMES
ALL_QUANTUM_FEATURES = {**QUANTUM_FEATURES, **PHASE2_QUANTUM_FEATURES}
ALL_QUANTUM_CANONICAL_FEATURE_KEYS = set(ALL_QUANTUM_FEATURES)
ALL_QUANTUM_PROPERTY_NAMES = {item["property_name"] for item in ALL_QUANTUM_FEATURES.values()}
ALL_QUANTUM_FEATURE_KEYS = ALL_QUANTUM_CANONICAL_FEATURE_KEYS | ALL_QUANTUM_PROPERTY_NAMES
QUANTUM_QUALITY_LEVELS = {"computed_standard", "computed_high", "estimated_group_contrib", "calculated_open_source"}
QUANTUM_FORBIDDEN_WIDE_COLUMNS = ALL_QUANTUM_PROPERTY_NAMES | {f"has_{name}" for name in ALL_QUANTUM_PROPERTY_NAMES}

QUANTUM_INPUT_COLUMNS = [
    "request_id",
    "mol_id",
    "canonical_feature_key",
    "value_num",
    "unit",
    "program",
    "program_version",
    "method_family",
    "theory_level",
    "basis_set",
    "solvation_model",
    "converged",
    "imaginary_frequency_count",
    "artifact_uri",
    "artifact_sha256",
    "quality_level",
    "notes",
]

QUANTUM_JOB_COLUMNS = [
    "request_id",
    "mol_id",
    "status",
    "program",
    "program_version",
    "method_family",
    "theory_level",
    "basis_set",
    "solvation_model",
    "quality_level",
    "converged",
    "imaginary_frequency_count",
    "derived_observation_count",
    "artifact_count",
    "source_id",
    "source_name",
    "created_by_stage_id",
    "notes",
]

QUANTUM_ARTIFACT_COLUMNS = [
    "artifact_id",
    "request_id",
    "mol_id",
    "artifact_uri",
    "artifact_sha256",
    "artifact_role",
    "source_id",
    "created_by_stage_id",
    "notes",
]

QUANTUM_REQUEST_COLUMNS = [
    "request_id",
    "mol_id",
    "canonical_smiles",
    "isomeric_smiles",
    "program",
    "method_family",
    "theory_level",
    "basis_set",
    "solvation_model",
    "status",
    "recommended_next_action",
    "notes",
]

QUANTUM_XYZ_MANIFEST_COLUMNS = [
    "request_id",
    "mol_id",
    "xyz_path",
    "xyz_status",
    "notes",
]

QUANTUM_RESULT_DEDUPE_COLUMNS = ["request_id", "program", "theory_level", "canonical_feature_key"]
DEFAULT_QUANTUM_MAX_REQUESTS = 2000
DEFAULT_DFT_MAX_REQUESTS = 150
PSI4_DFT_THEORY_LEVEL = "B3LYP-D3BJ"
PSI4_DFT_BASIS_SET = "def2-SVP"


@dataclass(slots=True)
class QuantumPilotBuild:
    property_rows: list[dict[str, Any]]
    quantum_job: pd.DataFrame
    quantum_artifact: pd.DataFrame
    summary: dict[str, Any]
    input_exists: bool


def build_quantum_pilot(
    input_path: Path,
    molecule_core: pd.DataFrame | None = None,
    *,
    created_by_stage_id: str = "05",
) -> QuantumPilotBuild:
    """Build quantum pilot tables and long-form observation rows from an optional CSV."""

    if not input_path.exists():
        return QuantumPilotBuild(
            property_rows=[],
            quantum_job=_empty_job_frame(),
            quantum_artifact=_empty_artifact_frame(),
            summary=quantum_pilot_summary(input_exists=False, input_path=input_path),
            input_exists=False,
        )

    raw = pd.read_csv(input_path).fillna("")
    raw = merge_quantum_result_rows(pd.DataFrame(columns=QUANTUM_INPUT_COLUMNS), raw)
    if molecule_core is not None and not molecule_core.empty and "mol_id" in molecule_core.columns:
        known_mol_ids = set(molecule_core["mol_id"].fillna("").astype(str).tolist())
    else:
        known_mol_ids = set()

    property_rows: list[dict[str, Any]] = []
    artifacts: dict[str, dict[str, Any]] = {}
    row_statuses: list[str] = []
    for row in raw.to_dict(orient="records"):
        status = _row_status(row, known_mol_ids)
        row_statuses.append(status)
        if status == "succeeded":
            property_rows.append(_observation_row(row, created_by_stage_id=created_by_stage_id))
        artifact = _artifact_row(row, created_by_stage_id=created_by_stage_id)
        if artifact is not None:
            artifacts[artifact["artifact_id"]] = artifact

    artifact_frame = _ensure_columns(pd.DataFrame(artifacts.values()), QUANTUM_ARTIFACT_COLUMNS)
    job_frame = _build_job_frame(raw, property_rows, artifact_frame, row_statuses, created_by_stage_id=created_by_stage_id)
    summary = quantum_pilot_summary(
        input_exists=True,
        input_path=input_path,
        input_row_count=len(raw),
        property_rows=property_rows,
        quantum_job=job_frame,
        quantum_artifact=artifact_frame,
    )
    return QuantumPilotBuild(
        property_rows=property_rows,
        quantum_job=job_frame,
        quantum_artifact=artifact_frame,
        summary=summary,
        input_exists=True,
    )


def quantum_pilot_summary(
    *,
    input_exists: bool,
    input_path: Path | None = None,
    input_row_count: int = 0,
    property_rows: list[dict[str, Any]] | None = None,
    quantum_job: pd.DataFrame | None = None,
    quantum_artifact: pd.DataFrame | None = None,
) -> dict[str, Any]:
    rows = property_rows or []
    feature_counts: dict[str, int] = {}
    for row in rows:
        key = _clean(row.get("canonical_feature_key"))
        feature_counts[key] = feature_counts.get(key, 0) + 1
    return {
        "input_status": "loaded" if input_exists else "not_configured",
        "input_path": str(input_path) if input_path is not None else "",
        "input_row_count": int(input_row_count),
        "quantum_job_count": int(len(quantum_job)) if quantum_job is not None else 0,
        "quantum_artifact_count": int(len(quantum_artifact)) if quantum_artifact is not None else 0,
        "quantum_observation_count": int(len(rows)),
        "quantum_molecule_count": int(len({_clean(row.get("mol_id")) for row in rows if _clean(row.get("mol_id"))})),
        "feature_counts": dict(sorted(feature_counts.items())),
    }


def merge_quantum_result_rows(existing: pd.DataFrame | None, incoming: pd.DataFrame | None) -> pd.DataFrame:
    """Merge ingestion-ready quantum rows by stable calculation identity.

    Later rows win for the identity tuple required by the production contract:
    ``(request_id, program, theory_level, canonical_feature_key)``.  Blank
    failure/audit rows are retained only when no converged feature rows exist
    for the same request/program/theory.
    """

    frames = []
    for frame in (existing, incoming):
        if frame is None or frame.empty:
            continue
        frames.append(_ensure_columns(frame.copy().fillna(""), QUANTUM_INPUT_COLUMNS))
    if not frames:
        return pd.DataFrame(columns=QUANTUM_INPUT_COLUMNS)

    merged = pd.concat(frames, ignore_index=True)
    merged["_merge_order"] = range(len(merged))
    merged = (
        merged.sort_values("_merge_order", kind="stable")
        .drop_duplicates(subset=QUANTUM_RESULT_DEDUPE_COLUMNS, keep="last")
        .sort_values("_merge_order", kind="stable")
        .reset_index(drop=True)
    )
    merged = _drop_superseded_failure_audits(merged)
    return _ensure_columns(merged.drop(columns=["_merge_order"], errors="ignore"), QUANTUM_INPUT_COLUMNS)


def _drop_superseded_failure_audits(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows
    key_columns = ["request_id", "program"]
    feature_keys = rows["canonical_feature_key"].fillna("").astype(str).str.strip()
    values = pd.to_numeric(rows["value_num"], errors="coerce")
    converged = rows["converged"].map(_truthy)
    success_keys = {
        tuple(record[column] for column in key_columns)
        for record in rows.loc[feature_keys.ne("") & values.notna() & converged, key_columns].fillna("").astype(str).to_dict(orient="records")
    }
    if not success_keys:
        return rows
    keep_mask = []
    for record in rows.fillna("").astype(str).to_dict(orient="records"):
        identity = tuple(record[column] for column in key_columns)
        keep_mask.append(not (identity in success_keys and not record.get("canonical_feature_key", "").strip()))
    return rows.loc[keep_mask].reset_index(drop=True)


def build_quantum_pilot_request_manifest(
    molecule_core: pd.DataFrame,
    *,
    active_learning_queue: pd.DataFrame | None = None,
    completed_request_ids: set[str] | None = None,
    max_requests: int | None = None,
    tools_available: bool | None = None,
    xyz_dir: Path | None = None,
    trash_project_root: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Build a deterministic quantum pilot request and XYZ manifest.

    This function only describes work to be run by an external quantum executor.
    Missing local executors are represented as pending request rows rather than
    synthetic quantum observations.
    """

    if molecule_core is None or molecule_core.empty or "mol_id" not in molecule_core.columns:
        request_manifest = pd.DataFrame(columns=QUANTUM_REQUEST_COLUMNS)
        xyz_manifest = pd.DataFrame(columns=QUANTUM_XYZ_MANIFEST_COLUMNS)
        return request_manifest, xyz_manifest, _request_summary(
            request_manifest,
            xyz_manifest,
            executor_available=False,
            selection_source="no_molecule_candidates",
        )

    max_requests = _quantum_max_requests() if max_requests is None else int(max_requests)
    executor_available = _quantum_executor_available() if tools_available is None else bool(tools_available)
    xyz_dir = xyz_dir or DATA_DIR / "raw" / "generated" / "quantum_xyz"
    completed_requests = {_clean(request_id) for request_id in (completed_request_ids or set()) if _clean(request_id)}
    candidates = molecule_core.copy().fillna("")
    selected_records = _active_learning_quantum_candidate_records(candidates, active_learning_queue)
    selection_source = "active_learning_queue" if selected_records else "promoted_coverage_fallback"
    if not selected_records:
        selected_records = _promoted_quantum_candidate_records(candidates)

    request_rows: list[dict[str, Any]] = []
    xyz_rows: list[dict[str, Any]] = []
    for record in selected_records:
        if len(request_rows) >= max_requests:
            break
        mol_id = _clean(record.get("mol_id"))
        smiles = _clean(record.get("isomeric_smiles")) or _clean(record.get("canonical_smiles"))
        if not mol_id or not smiles:
            continue
        request_id = "qreq_" + hashlib.sha256(f"{mol_id}|{smiles}|xtb-gfn2".encode("utf-8")).hexdigest()[:16]
        status = (
            "completed"
            if request_id in completed_requests
            else ("ready_for_executor" if executor_available else "pending_executor_unavailable")
        )
        notes = "request only; build never fabricates quantum results"
        queue_entry_id = _clean(record.get("_active_learning_queue_entry_id"))
        if queue_entry_id:
            priority_score = _clean(record.get("_active_learning_priority_score"))
            notes = (
                f"{notes}; selected from active_learning_queue "
                f"queue_entry_id={queue_entry_id} priority_score={priority_score}"
            )
        if status == "completed":
            notes = f"{notes}; completed quantum pilot results already ingested"
        request_rows.append(
            {
                "request_id": request_id,
                "mol_id": mol_id,
                "canonical_smiles": _clean(record.get("canonical_smiles")) or smiles,
                "isomeric_smiles": smiles,
                "program": "xtb",
                "method_family": "semiempirical",
                "theory_level": "GFN2-xTB",
                "basis_set": "",
                "solvation_model": "gas_phase",
                "status": status,
                "recommended_next_action": "run_quantum",
                "notes": notes,
            }
        )
        xyz_path = xyz_dir / f"{request_id}.xyz"
        xyz_generated, xyz_note = _write_xyz_from_smiles(smiles, xyz_path, request_id=request_id, mol_id=mol_id)
        if status == "completed":
            executor_note = "executor not required; completed quantum pilot results already ingested"
        else:
            executor_note = "executor available" if executor_available else "executor unavailable; request remains pending"
        xyz_rows.append(
            {
                "request_id": request_id,
                "mol_id": mol_id,
                "xyz_path": str(xyz_path),
                "xyz_status": "generated" if xyz_generated else "generation_failed",
                "notes": f"{xyz_note}; {executor_note}",
            }
        )
    request_manifest = _ensure_columns(pd.DataFrame(request_rows), QUANTUM_REQUEST_COLUMNS)
    xyz_manifest = _ensure_columns(pd.DataFrame(xyz_rows), QUANTUM_XYZ_MANIFEST_COLUMNS)
    stale_xyz_trashed = _trash_stale_xyz_files(
        xyz_dir,
        keep_paths={Path(path) for path in xyz_manifest["xyz_path"].astype(str).tolist()},
        project_root=trash_project_root or PROJECT_ROOT,
    )
    return request_manifest, xyz_manifest, _request_summary(
        request_manifest,
        xyz_manifest,
        executor_available=executor_available,
        selection_source=selection_source,
        stale_xyz_trashed=stale_xyz_trashed,
    )


def _quantum_executor_available() -> bool:
    xtb_bin = _clean(os.getenv("R_PHYSGEN_XTB_BIN"))
    if xtb_bin and (Path(xtb_bin).exists() or shutil.which(xtb_bin)):
        return True
    return bool(shutil.which("xtb"))


def _psi4_executor_available() -> bool:
    psi4_bin = _clean(os.getenv("R_PHYSGEN_PSI4_BIN"))
    if psi4_bin and (Path(psi4_bin).exists() or shutil.which(psi4_bin)):
        return True
    if shutil.which("psi4"):
        return True
    try:
        __import__("psi4")
    except Exception:
        return False
    return True


def _quantum_max_requests(default: int = DEFAULT_QUANTUM_MAX_REQUESTS) -> int:
    value = _clean(os.getenv("R_PHYSGEN_QUANTUM_MAX_REQUESTS"))
    if not value:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _dft_max_requests(default: int = DEFAULT_DFT_MAX_REQUESTS) -> int:
    value = _clean(os.getenv("R_PHYSGEN_DFT_MAX_REQUESTS"))
    if not value:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def build_psi4_dft_request_manifest(
    molecule_core: pd.DataFrame,
    *,
    xtb_results: pd.DataFrame | None = None,
    active_learning_queue: pd.DataFrame | None = None,
    completed_request_ids: set[str] | None = None,
    max_requests: int | None = None,
    tools_available: bool | None = None,
    xyz_dir: Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Build a deterministic Psi4/DFT request manifest from completed xTB rows."""

    if molecule_core is None or molecule_core.empty or "mol_id" not in molecule_core.columns:
        request_manifest = pd.DataFrame(columns=QUANTUM_REQUEST_COLUMNS)
        xyz_manifest = pd.DataFrame(columns=QUANTUM_XYZ_MANIFEST_COLUMNS)
        return request_manifest, xyz_manifest, _request_summary(
            request_manifest,
            xyz_manifest,
            executor_available=False,
            selection_source="no_molecule_candidates",
        )

    max_requests = _dft_max_requests() if max_requests is None else int(max_requests)
    executor_available = _psi4_executor_available() if tools_available is None else bool(tools_available)
    xyz_dir = xyz_dir or DATA_DIR / "raw" / "generated" / "quantum_dft_xyz"
    xtb_completed = _completed_xtb_mol_ids(xtb_results)
    xtb_optimized_xyz_by_mol = _completed_xtb_optimized_xyz_by_mol_id(xtb_results)
    completed_requests = {_clean(request_id) for request_id in (completed_request_ids or set()) if _clean(request_id)}
    priority_lookup = _active_learning_priority_lookup(active_learning_queue)
    selected_records = _stratified_dft_candidate_records(molecule_core.fillna(""), xtb_completed, priority_lookup, max_requests=max_requests)

    request_rows: list[dict[str, Any]] = []
    xyz_rows: list[dict[str, Any]] = []
    for record in selected_records:
        mol_id = _clean(record.get("mol_id"))
        smiles = _clean(record.get("isomeric_smiles")) or _clean(record.get("canonical_smiles"))
        if not mol_id or not smiles:
            continue
        request_id = "qreq_" + hashlib.sha256(
            f"{mol_id}|{smiles}|psi4|{PSI4_DFT_THEORY_LEVEL}|{PSI4_DFT_BASIS_SET}".encode("utf-8")
        ).hexdigest()[:16]
        status = "completed" if request_id in completed_requests else ("ready_for_executor" if executor_available else "pending_executor_unavailable")
        completion_note = "completed Psi4 DFT results already present; " if status == "completed" else ""
        request_rows.append(
            {
                "request_id": request_id,
                "mol_id": mol_id,
                "canonical_smiles": _clean(record.get("canonical_smiles")) or smiles,
                "isomeric_smiles": smiles,
                "program": "psi4",
                "method_family": "DFT",
                "theory_level": PSI4_DFT_THEORY_LEVEL,
                "basis_set": PSI4_DFT_BASIS_SET,
                "solvation_model": "gas_phase",
                "status": status,
                "recommended_next_action": "run_quantum",
                "notes": (
                    f"{completion_note}Psi4 DFT request selected from completed xTB molecules; "
                    f"stratification_key={record.get('_dft_stratification_key', '')}; "
                    f"priority_score={record.get('_dft_priority_score', 0.0)}"
                ),
            }
        )
        xyz_path = xyz_dir / f"{request_id}.xyz"
        xtb_optimized_xyz = xtb_optimized_xyz_by_mol.get(mol_id)
        if xtb_optimized_xyz is not None and xtb_optimized_xyz.exists():
            xyz_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(xtb_optimized_xyz, xyz_path)
            xyz_generated = True
            xyz_note = "XYZ copied from completed xTB optimized geometry for Psi4 singlepoint"
        else:
            xyz_generated, xyz_note = _write_xyz_from_smiles(smiles, xyz_path, request_id=request_id, mol_id=mol_id)
        xyz_rows.append(
            {
                "request_id": request_id,
                "mol_id": mol_id,
                "xyz_path": str(xyz_path),
                "xyz_status": "generated" if xyz_generated else "generation_failed",
                "notes": f"{xyz_note}; Psi4 executor {'available' if executor_available else 'unavailable'}",
            }
        )
    request_manifest = _ensure_columns(pd.DataFrame(request_rows), QUANTUM_REQUEST_COLUMNS)
    xyz_manifest = _ensure_columns(pd.DataFrame(xyz_rows), QUANTUM_XYZ_MANIFEST_COLUMNS)
    summary = _request_summary(
        request_manifest,
        xyz_manifest,
        executor_available=executor_available,
        selection_source="completed_xtb_stratified" if selected_records else "no_completed_xtb_candidates",
    )
    summary["completed_xtb_candidate_count"] = int(len(xtb_completed))
    summary["dft_target_count"] = int(max_requests)
    return request_manifest, xyz_manifest, summary


def _completed_xtb_mol_ids(xtb_results: pd.DataFrame | None) -> set[str]:
    return completed_xtb_mol_ids(xtb_results)


def completed_xtb_mol_ids(xtb_results: pd.DataFrame | None) -> set[str]:
    if xtb_results is None or xtb_results.empty:
        return set()
    return _completed_xtb_entity_ids(xtb_results, entity_column="mol_id")


def completed_xtb_request_ids(xtb_results: pd.DataFrame | None) -> set[str]:
    if xtb_results is None or xtb_results.empty:
        return set()
    return _completed_xtb_entity_ids(xtb_results, entity_column="request_id")


def completed_psi4_request_ids(quantum_results: pd.DataFrame | None) -> set[str]:
    if quantum_results is None or quantum_results.empty:
        return set()
    return _completed_quantum_entity_ids(
        quantum_results,
        program="psi4",
        feature_keys=QUANTUM_CANONICAL_FEATURE_KEYS,
        entity_column="request_id",
    )


def _completed_xtb_entity_ids(xtb_results: pd.DataFrame, *, entity_column: str) -> set[str]:
    return _completed_quantum_entity_ids(
        xtb_results,
        program="xtb",
        feature_keys=QUANTUM_CANONICAL_FEATURE_KEYS,
        entity_column=entity_column,
    )


def _completed_quantum_entity_ids(
    quantum_results: pd.DataFrame,
    *,
    program: str,
    feature_keys: set[str],
    entity_column: str,
) -> set[str]:
    rows = _ensure_columns(quantum_results.copy().fillna(""), QUANTUM_INPUT_COLUMNS)
    if entity_column not in rows.columns:
        return set()
    rows = rows.loc[
        rows["program"].astype(str).str.lower().eq(program.lower())
        & rows["canonical_feature_key"].astype(str).isin(feature_keys)
        & rows["converged"].map(_truthy)
        & pd.to_numeric(rows["value_num"], errors="coerce").notna()
    ].copy()
    if rows.empty:
        return set()
    rows[entity_column] = rows[entity_column].fillna("").astype(str).str.strip()
    rows = rows.loc[rows[entity_column].ne("")]
    feature_counts = rows.groupby(entity_column)["canonical_feature_key"].nunique()
    return {str(entity_id) for entity_id, count in feature_counts.items() if int(count) >= len(feature_keys)}


def _completed_xtb_optimized_xyz_by_mol_id(xtb_results: pd.DataFrame | None) -> dict[str, Path]:
    if xtb_results is None or xtb_results.empty:
        return {}
    rows = _ensure_columns(xtb_results.copy().fillna(""), QUANTUM_INPUT_COLUMNS)
    rows = rows.loc[
        rows["program"].astype(str).str.lower().eq("xtb")
        & rows["canonical_feature_key"].astype(str).isin(QUANTUM_CANONICAL_FEATURE_KEYS)
        & rows["converged"].map(_truthy)
    ].copy()
    if rows.empty:
        return {}

    output: dict[str, Path] = {}
    for record in rows.to_dict(orient="records"):
        mol_id = _clean(record.get("mol_id"))
        if not mol_id or mol_id in output:
            continue
        for candidate in _xtb_artifact_geometry_candidates(_clean(record.get("artifact_uri"))):
            if candidate.exists():
                output[mol_id] = candidate
                break
    return output


def _xtb_artifact_geometry_candidates(artifact_uri: str) -> list[Path]:
    if not artifact_uri or "://" in artifact_uri and not artifact_uri.startswith("file://"):
        return []
    path_text = artifact_uri.removeprefix("file://")
    artifact_path = Path(path_text)
    if not artifact_path.is_absolute():
        artifact_path = PROJECT_ROOT / artifact_path
    if artifact_path.is_dir():
        roots = [artifact_path]
    else:
        roots = [artifact_path.parent]
    return [root / "xtbopt.xyz" for root in roots]


def _active_learning_priority_lookup(active_learning_queue: pd.DataFrame | None) -> dict[str, float]:
    if active_learning_queue is None or active_learning_queue.empty or "mol_id" not in active_learning_queue.columns:
        return {}
    queue = active_learning_queue.copy().fillna("")
    queue["_priority_score_num"] = pd.to_numeric(queue.get("priority_score", pd.Series(0.0, index=queue.index)), errors="coerce").fillna(0.0)
    return queue.sort_values("_priority_score_num", ascending=False, kind="stable").drop_duplicates("mol_id").set_index("mol_id")[
        "_priority_score_num"
    ].astype(float).to_dict()


def _stratified_dft_candidate_records(
    molecule_core: pd.DataFrame,
    completed_xtb_mol_ids: set[str],
    priority_lookup: dict[str, float],
    *,
    max_requests: int,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for record in molecule_core.to_dict(orient="records"):
        mol_id = _clean(record.get("mol_id"))
        smiles = _clean(record.get("isomeric_smiles")) or _clean(record.get("canonical_smiles"))
        if not mol_id or mol_id not in completed_xtb_mol_ids or not smiles:
            continue
        enriched = dict(record)
        enriched["_dft_priority_score"] = float(priority_lookup.get(mol_id, 0.0))
        enriched["_dft_stratification_key"] = "|".join(
            [
                _clean(record.get("coverage_tier")) or "unassigned",
                _clean(record.get("scaffold_key")) or "no_scaffold",
                _halogen_signature(smiles),
            ]
        )
        candidates.append(enriched)
    if not candidates or max_requests <= 0:
        return []

    coverage_order = {"A": 0, "B": 1, "C": 2, "D": 3, "": 4}
    by_coverage: dict[str, list[dict[str, Any]]] = {}
    for record in candidates:
        coverage = _clean(record.get("coverage_tier"))
        by_coverage.setdefault(coverage, []).append(record)
    for coverage, records in by_coverage.items():
        by_coverage[coverage] = _diversify_records_within_coverage(records)

    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    ordered_coverages = sorted(by_coverage, key=lambda item: (coverage_order.get(item, 99), item))
    while len(selected) < max_requests:
        made_progress = False
        for coverage in ordered_coverages:
            records = by_coverage[coverage]
            while records and _clean(records[0].get("mol_id")) in seen:
                records.pop(0)
            if not records:
                continue
            record = records.pop(0)
            mol_id = _clean(record.get("mol_id"))
            if mol_id in seen:
                continue
            selected.append(record)
            seen.add(mol_id)
            made_progress = True
            if len(selected) >= max_requests:
                break
        if not made_progress:
            break
    return selected


def _diversify_records_within_coverage(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for record in sorted(records, key=lambda item: (-float(item.get("_dft_priority_score", 0.0)), _clean(item.get("mol_id")))):
        group_key = "|".join([_clean(record.get("scaffold_key")) or "no_scaffold", _halogen_signature(_clean(record.get("isomeric_smiles")))])
        groups.setdefault(group_key, []).append(record)
    ordered_groups = sorted(
        groups,
        key=lambda key: (-float(groups[key][0].get("_dft_priority_score", 0.0)), key),
    )
    diversified: list[dict[str, Any]] = []
    while True:
        made_progress = False
        for key in ordered_groups:
            if groups[key]:
                diversified.append(groups[key].pop(0))
                made_progress = True
        if not made_progress:
            break
    return diversified


def _halogen_signature(smiles: str) -> str:
    tokens = []
    for token in ["Cl", "Br", "F", "I"]:
        if token in smiles:
            tokens.append(token)
    return "".join(tokens) or "no_halogen"


def _request_summary(
    request_manifest: pd.DataFrame,
    xyz_manifest: pd.DataFrame,
    *,
    executor_available: bool,
    selection_source: str,
    stale_xyz_trashed: list[str] | None = None,
) -> dict[str, Any]:
    stale_xyz_trashed = stale_xyz_trashed or []
    return {
        "request_count": int(len(request_manifest)),
        "xyz_manifest_count": int(len(xyz_manifest)),
        "xyz_generated_count": int(xyz_manifest["xyz_status"].astype(str).eq("generated").sum()) if not xyz_manifest.empty else 0,
        "executor_available": bool(executor_available),
        "executor_status": "available" if executor_available else "unavailable",
        "selection_source": selection_source,
        "status_counts": request_manifest["status"].value_counts().sort_index().to_dict() if not request_manifest.empty else {},
        "stale_xyz_trashed_count": int(len(stale_xyz_trashed)),
    }


def _trash_stale_xyz_files(xyz_dir: Path, *, keep_paths: set[Path], project_root: Path) -> list[str]:
    if not xyz_dir.exists():
        return []
    keep_resolved = {path.resolve() for path in keep_paths}
    trashed: list[str] = []
    for path in sorted(xyz_dir.glob("*.xyz")):
        if path.resolve() in keep_resolved:
            continue
        destination = _trash_path(path, project_root=project_root)
        trashed.append(str(destination))
    return trashed


def _trash_path(path: Path, *, project_root: Path) -> Path:
    source = path.resolve()
    root = project_root.resolve()
    try:
        relative = source.relative_to(root)
    except ValueError:
        relative = Path(source.name)
    destination_root = root / ".trash"
    destination = _unique_destination(destination_root / relative)
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(destination))
    except OSError as exc:
        if exc.errno != errno.ENOSPC and "No space left on device" not in str(exc):
            raise
        home_destination = _unique_destination(Path.home() / ".trash" / relative)
        home_destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(home_destination))
        return home_destination
    return destination


def _unique_destination(destination: Path) -> Path:
    if not destination.exists():
        return destination
    timestamp = time.strftime("%Y%m%dT%H%M%S")
    return destination.with_name(f"{destination.name}.{timestamp}")


def _active_learning_quantum_candidate_records(
    molecule_core: pd.DataFrame,
    active_learning_queue: pd.DataFrame | None,
) -> list[dict[str, Any]]:
    if active_learning_queue is None or active_learning_queue.empty:
        return []
    if "mol_id" not in active_learning_queue.columns or "recommended_next_action" not in active_learning_queue.columns:
        return []

    queue = active_learning_queue.copy().fillna("")
    queue = queue.loc[queue["recommended_next_action"].astype(str).eq("run_quantum")].copy()
    if queue.empty:
        return []
    if "status" in queue.columns:
        statuses = queue["status"].astype(str)
        queue = queue.loc[~statuses.isin({"rejected"})].copy()
        if queue.empty:
            return []

    queue["_priority_score_num"] = pd.to_numeric(queue.get("priority_score", pd.Series(0, index=queue.index)), errors="coerce").fillna(0.0)
    queue["_queue_order"] = range(len(queue))
    queue = queue.sort_values(["_priority_score_num", "_queue_order"], ascending=[False, True], kind="stable")

    molecule_records: dict[str, dict[str, Any]] = {}
    for record in molecule_core.to_dict(orient="records"):
        mol_id = _clean(record.get("mol_id"))
        if mol_id and mol_id not in molecule_records:
            molecule_records[mol_id] = record

    selected: list[dict[str, Any]] = []
    seen_mol_ids: set[str] = set()
    for queue_record in queue.to_dict(orient="records"):
        mol_id = _clean(queue_record.get("mol_id"))
        if not mol_id or mol_id in seen_mol_ids or mol_id not in molecule_records:
            continue
        seen_mol_ids.add(mol_id)
        record = dict(molecule_records[mol_id])
        record["_active_learning_queue_entry_id"] = _clean(queue_record.get("queue_entry_id"))
        record["_active_learning_priority_score"] = _clean(queue_record.get("priority_score"))
        selected.append(record)
    return selected


def _promoted_quantum_candidate_records(molecule_core: pd.DataFrame) -> list[dict[str, Any]]:
    candidates = molecule_core.copy()
    if "model_inclusion" in candidates.columns:
        promoted = candidates.loc[candidates["model_inclusion"].astype(str).eq("yes")].copy()
        if not promoted.empty:
            candidates = promoted
    if "coverage_tier" in candidates.columns:
        promoted = candidates.loc[candidates["coverage_tier"].astype(str).isin({"A", "B", "C"})].copy()
        if not promoted.empty:
            candidates = promoted
    sort_columns = ["coverage_tier", "mol_id"] if "coverage_tier" in candidates.columns else ["mol_id"]
    return candidates.sort_values(sort_columns, kind="stable").to_dict(orient="records")


def _write_xyz_from_smiles(smiles: str, path: Path, *, request_id: str, mol_id: str) -> tuple[bool, str]:
    RDLogger.DisableLog("rdApp.warning")
    RDLogger.DisableLog("rdApp.error")
    try:
        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            return False, "invalid SMILES; XYZ not generated"
        mol = Chem.AddHs(mol)
        if mol.GetNumAtoms() <= 1:
            _write_linear_xyz(mol, path, request_id=request_id, mol_id=mol_id, smiles=smiles)
            return True, "XYZ generated from single-atom SMILES using deterministic coordinates"
        params = AllChem.ETKDGv3()
        seed = int(hashlib.sha256(f"{request_id}|{smiles}".encode("utf-8")).hexdigest()[:8], 16) & 0x7FFFFFFF
        params.randomSeed = seed
        status = AllChem.EmbedMolecule(mol, params)
        if status != 0:
            _write_linear_xyz(mol, path, request_id=request_id, mol_id=mol_id, smiles=smiles)
            return True, "RDKit conformer embedding failed; XYZ generated with deterministic linear fallback"
        try:
            if AllChem.MMFFHasAllMoleculeParams(mol):
                AllChem.MMFFOptimizeMolecule(mol)
            else:
                AllChem.UFFOptimizeMolecule(mol)
        except Exception:
            # Coordinates are still usable as executor input if force-field refinement is unsupported.
            pass
        conf = mol.GetConformer()
        lines = [
            str(mol.GetNumAtoms()),
            f"{request_id} {mol_id} generated_from_smiles={smiles}",
        ]
        for atom in mol.GetAtoms():
            pos = conf.GetAtomPosition(atom.GetIdx())
            lines.append(f"{atom.GetSymbol()} {pos.x:.8f} {pos.y:.8f} {pos.z:.8f}")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return True, "XYZ generated from canonical/isomeric SMILES using RDKit ETKDG"
    finally:
        RDLogger.EnableLog("rdApp.warning")
        RDLogger.EnableLog("rdApp.error")


def _write_linear_xyz(mol: Chem.Mol, path: Path, *, request_id: str, mol_id: str, smiles: str) -> None:
    """Write a deterministic, non-optimized XYZ when 3D embedding is not available."""

    lines = [
        str(mol.GetNumAtoms()),
        f"{request_id} {mol_id} generated_from_smiles={smiles} coordinate_fallback=linear",
    ]
    for idx, atom in enumerate(mol.GetAtoms()):
        lines.append(f"{atom.GetSymbol()} {idx * 1.20000000:.8f} 0.00000000 0.00000000")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _row_status(row: dict[str, Any], known_mol_ids: set[str]) -> str:
    mol_id = _clean(row.get("mol_id"))
    if known_mol_ids and mol_id not in known_mol_ids:
        return "failed_unknown_molecule"
    if not _truthy(row.get("converged")):
        return "failed_not_converged"
    if _int_value(row.get("imaginary_frequency_count")) != 0:
        return "failed_imaginary_frequency"
    if pd.isna(pd.to_numeric(pd.Series([row.get("value_num")]), errors="coerce").iloc[0]):
        return "failed_missing_value"
    return "succeeded"


def _observation_row(row: dict[str, Any], *, created_by_stage_id: str) -> dict[str, Any]:
    canonical_key = _clean(row.get("canonical_feature_key"))
    feature = ALL_QUANTUM_FEATURES.get(canonical_key, {})
    property_name = feature.get("property_name", canonical_key or "quantum_unknown")
    unit = _clean(row.get("unit")) or feature.get("unit", "eV")
    value_num = float(pd.to_numeric(pd.Series([row.get("value_num")]), errors="coerce").iloc[0])
    quality_level = _clean(row.get("quality_level")) or "computed_standard"
    method = _method_label(row)
    source_record_id = f"{_clean(row.get('request_id'))}:{canonical_key}"
    quality_score = 95 if quality_level == "computed_high" else (65 if quality_level == "estimated_group_contrib" else 80)
    return {
        "observation_id": "",
        "mol_id": _clean(row.get("mol_id")),
        "property_name": property_name,
        "value": f"{value_num:.12g}",
        "value_num": value_num,
        "unit": unit,
        "standard_unit": unit,
        "standard_value_num": value_num,
        "temperature": "298.15 K",
        "pressure": "",
        "phase": "gas",
        "source_type": "calculated_open_source",
        "source_name": QUANTUM_SOURCE_NAME,
        "source_id": QUANTUM_SOURCE_ID,
        "source_record_id": source_record_id,
        "method": method,
        "uncertainty": "",
        "quality_level": quality_level,
        "assessment_version": QUANTUM_ASSESSMENT_VERSION,
        "notes": _clean(row.get("notes")),
        "qc_status": "pass",
        "qc_flags": "",
        "canonical_feature_key": canonical_key,
        "source_priority_rank": 600,
        "data_quality_score_100": quality_score,
        "is_proxy_or_screening": 0,
        "ml_use_status": QUANTUM_ML_USE_STATUS,
        "ingestion_stage_id": created_by_stage_id,
        "normalization_rule_id": "quantum_ev_identity",
        "convergence_flag": 1,
    }


def _artifact_row(row: dict[str, Any], *, created_by_stage_id: str) -> dict[str, Any] | None:
    uri = _clean(row.get("artifact_uri"))
    digest = _clean(row.get("artifact_sha256"))
    if not uri and not digest:
        return None
    request_id = _clean(row.get("request_id"))
    artifact_id = "qart_" + hashlib.sha256(f"{request_id}|{uri}|{digest}".encode("utf-8")).hexdigest()[:16]
    return {
        "artifact_id": artifact_id,
        "request_id": request_id,
        "mol_id": _clean(row.get("mol_id")),
        "artifact_uri": uri,
        "artifact_sha256": digest,
        "artifact_role": "quantum_result_bundle",
        "source_id": QUANTUM_SOURCE_ID,
        "created_by_stage_id": created_by_stage_id,
        "notes": _clean(row.get("notes")),
    }


def _build_job_frame(
    raw: pd.DataFrame,
    property_rows: list[dict[str, Any]],
    artifact_frame: pd.DataFrame,
    row_statuses: list[str],
    *,
    created_by_stage_id: str,
) -> pd.DataFrame:
    if raw.empty:
        return _empty_job_frame()

    raw = raw.copy()
    raw["_row_status"] = row_statuses
    observation_count = {}
    for row in property_rows:
        request_id = _clean(row.get("source_record_id")).split(":", 1)[0]
        observation_count[request_id] = observation_count.get(request_id, 0) + 1
    artifact_count = artifact_frame.groupby("request_id").size().to_dict() if not artifact_frame.empty else {}

    jobs: list[dict[str, Any]] = []
    for request_id, group in raw.groupby("request_id", sort=True, dropna=False):
        first = group.iloc[0].to_dict()
        status = "succeeded" if int(observation_count.get(_clean(request_id), 0)) > 0 else "failed"
        jobs.append(
            {
                "request_id": _clean(request_id),
                "mol_id": _clean(first.get("mol_id")),
                "status": status,
                "program": _clean(first.get("program")),
                "program_version": _clean(first.get("program_version")),
                "method_family": _clean(first.get("method_family")),
                "theory_level": _clean(first.get("theory_level")),
                "basis_set": _clean(first.get("basis_set")),
                "solvation_model": _clean(first.get("solvation_model")),
                "quality_level": _clean(first.get("quality_level")) or "computed_standard",
                "converged": int(group["converged"].map(_truthy).all()),
                "imaginary_frequency_count": int(max(_int_value(value) for value in group["imaginary_frequency_count"].tolist())),
                "derived_observation_count": int(observation_count.get(_clean(request_id), 0)),
                "artifact_count": int(artifact_count.get(_clean(request_id), 0)),
                "source_id": QUANTUM_SOURCE_ID,
                "source_name": QUANTUM_SOURCE_NAME,
                "created_by_stage_id": created_by_stage_id,
                "notes": "; ".join(sorted(set(group["_row_status"].astype(str).tolist()))),
            }
        )
    return _ensure_columns(pd.DataFrame(jobs), QUANTUM_JOB_COLUMNS)


def _method_label(row: dict[str, Any]) -> str:
    pieces = [
        _clean(row.get("program")),
        _clean(row.get("theory_level")),
        _clean(row.get("basis_set")),
        _clean(row.get("solvation_model")),
    ]
    return " / ".join(piece for piece in pieces if piece) or "offline quantum calculation"


def _truthy(value: Any) -> bool:
    text = _clean(value).lower()
    if text in {"1", "true", "yes", "y", "converged", "succeeded"}:
        return True
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return bool(pd.notna(numeric) and float(numeric) == 1.0)


def _int_value(value: Any) -> int:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return 0
    return int(numeric)


def _empty_job_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=QUANTUM_JOB_COLUMNS)


def _empty_artifact_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=QUANTUM_ARTIFACT_COLUMNS)


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in df.columns:
            df[column] = None
    return df[columns]


def _clean(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()
