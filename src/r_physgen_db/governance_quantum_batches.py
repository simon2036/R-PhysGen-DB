"""Governance-driven quantum request batching for high-compute DFT runs."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from r_physgen_db.constants import DATA_DIR, PROJECT_ROOT
from r_physgen_db.quantum_pilot import (
    PSI4_DFT_BASIS_SET,
    PSI4_DFT_THEORY_LEVEL,
    QUANTUM_CANONICAL_FEATURE_KEYS,
    QUANTUM_FEATURES,
    QUANTUM_INPUT_COLUMNS,
    QUANTUM_REQUEST_COLUMNS,
    QUANTUM_XYZ_MANIFEST_COLUMNS,
    _psi4_executor_available,
    _quantum_executor_available,
    _write_xyz_from_smiles,
)

GOVERNANCE_QUEUE_PATH = DATA_DIR / "extensions" / "property_governance_20260422" / "tables" / "tbl_qm_dft_required_queue.parquet"
SEED_CATALOG_PATH = DATA_DIR / "raw" / "manual" / "seed_catalog.csv"
MOLECULE_CORE_PATH = DATA_DIR / "silver" / "molecule_core.parquet"
QUANTUM_JOB_PATH = DATA_DIR / "silver" / "quantum_job.parquet"
QUANTUM_ARTIFACT_PATH = DATA_DIR / "silver" / "quantum_artifact.parquet"
QUANTUM_RESULTS_PATH = DATA_DIR / "raw" / "manual" / "quantum_pilot_results.csv"
DEFAULT_OUTPUT_DIR = DATA_DIR / "raw" / "generated"
DEFAULT_XTB_XYZ_DIR = DEFAULT_OUTPUT_DIR / "governance_xtb_xyz"
DEFAULT_DFT_XYZ_DIR = DEFAULT_OUTPUT_DIR / "governance_dft_singlepoint_xyz"

PHASE1_DFT_FEATURE_KEYS = tuple(QUANTUM_FEATURES.keys())
PHASE2_DEFERRED_SCOPE = (
    "frequency/zpe/ir",
    "nbo/resp/atomic_charges",
    "thermochemistry",
    "conformer_ensembles",
)

GOVERNANCE_MAPPING_COLUMNS = [
    "substance_id",
    "refrigerant_number",
    "requested_property_count",
    "requested_properties",
    "seed_id",
    "mol_id",
    "canonical_smiles",
    "isomeric_smiles",
    "mapping_status",
    "mapping_reason",
    "has_completed_xtb",
    "has_completed_psi4_dft",
    "xtb_request_id",
    "dft_request_id",
    "xtb_enqueue_status",
    "dft_enqueue_status",
    "dft_xyz_source_path",
    "dft_blocking_issue",
]


def governance_xtb_request_id(mol_id: str, smiles: str) -> str:
    """Return the existing stable xTB request id for a molecule/smiles pair."""

    return "qreq_" + hashlib.sha256(f"{_clean(mol_id)}|{_clean(smiles)}|xtb-gfn2".encode("utf-8")).hexdigest()[:16]


def governance_dft_request_id(
    mol_id: str,
    smiles: str,
    *,
    program: str = "psi4",
    theory_level: str = PSI4_DFT_THEORY_LEVEL,
    basis_set: str = PSI4_DFT_BASIS_SET,
) -> str:
    """Return the existing stable Psi4/DFT request id for a molecule/smiles pair."""

    payload = f"{_clean(mol_id)}|{_clean(smiles)}|{_clean(program)}|{_clean(theory_level)}|{_clean(basis_set)}"
    return "qreq_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def build_governance_mapping(
    queue: pd.DataFrame,
    seed_catalog: pd.DataFrame,
    molecule_core: pd.DataFrame,
) -> pd.DataFrame:
    """Map governance DFT backlog substances to current molecule_core rows.

    The returned frame intentionally contains every unique governance substance,
    including unmapped rows, so unmapped substances stay auditable instead of
    silently falling out of the compute queue.
    """

    substances = _substance_requests(queue)
    seed_candidates = _seed_candidates_by_r_number(seed_catalog, molecule_core)
    rows: list[dict[str, Any]] = []
    for substance in substances.to_dict(orient="records"):
        refrigerant_number = _clean(substance.get("refrigerant_number"))
        candidates = seed_candidates.get(refrigerant_number, [])
        selected = candidates[0] if candidates else {}
        seed_id = _clean(selected.get("seed_id"))
        mol_id = _clean(selected.get("mol_id"))
        canonical_smiles = _clean(selected.get("canonical_smiles"))
        isomeric_smiles = _clean(selected.get("isomeric_smiles")) or canonical_smiles
        if not candidates:
            mapping_status = "unmapped"
            mapping_reason = "no_seed_catalog_match"
        elif not mol_id:
            mapping_status = "unmapped"
            mapping_reason = "no_molecule_core_match"
        elif not (canonical_smiles or isomeric_smiles):
            mapping_status = "unmapped"
            mapping_reason = "missing_smiles"
        else:
            mapping_status = "mapped"
            mapping_reason = "mapped_by_seed_catalog"
        rows.append(
            {
                "substance_id": _clean(substance.get("substance_id")),
                "refrigerant_number": refrigerant_number,
                "requested_property_count": int(substance.get("requested_property_count", 0) or 0),
                "requested_properties": _clean(substance.get("requested_properties")),
                "seed_id": seed_id,
                "mol_id": mol_id,
                "canonical_smiles": canonical_smiles or isomeric_smiles,
                "isomeric_smiles": isomeric_smiles or canonical_smiles,
                "mapping_status": mapping_status,
                "mapping_reason": mapping_reason,
            }
        )
    return _ensure_columns(pd.DataFrame(rows), GOVERNANCE_MAPPING_COLUMNS[:10])


def materialize_governance_quantum_batches(
    *,
    mode: str,
    queue: pd.DataFrame,
    seed_catalog: pd.DataFrame,
    molecule_core: pd.DataFrame,
    quantum_job: pd.DataFrame | None = None,
    quantum_results: pd.DataFrame | None = None,
    quantum_artifact: pd.DataFrame | None = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    xyz_dir: Path | None = None,
    batch_size: int = 100,
    tools_available: bool | None = None,
) -> dict[str, Any]:
    """Write governance xTB or Psi4 request/XYZ batches plus mapping reports."""

    if mode not in {"xtb-pregeometry", "psi4-singlepoint"}:
        raise ValueError(f"unsupported governance quantum batch mode: {mode}")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    xyz_dir = Path(xyz_dir) if xyz_dir is not None else (DEFAULT_XTB_XYZ_DIR if mode == "xtb-pregeometry" else DEFAULT_DFT_XYZ_DIR)
    batch_size = max(1, int(batch_size or 1))
    quantum_job = _ensure_columns((quantum_job if quantum_job is not None else pd.DataFrame()).copy().fillna(""), [])
    quantum_results = _ensure_columns((quantum_results if quantum_results is not None else pd.DataFrame()).copy().fillna(""), QUANTUM_INPUT_COLUMNS)
    quantum_artifact = (quantum_artifact if quantum_artifact is not None else pd.DataFrame()).copy().fillna("")

    mapping = build_governance_mapping(queue, seed_catalog, molecule_core)
    completed_xtb = _completed_mol_ids(
        quantum_job,
        quantum_results,
        program="xtb",
        theory_level="GFN2-xTB",
        basis_set="",
    )
    completed_psi4 = _completed_mol_ids(
        quantum_job,
        quantum_results,
        program="psi4",
        theory_level=PSI4_DFT_THEORY_LEVEL,
        basis_set=PSI4_DFT_BASIS_SET,
    )
    xtb_geometry_by_mol = _completed_xtb_geometry_by_mol_id(quantum_results, quantum_artifact, quantum_job)
    mapping = _enrich_mapping(mapping, completed_xtb, completed_psi4, xtb_geometry_by_mol)

    if mode == "xtb-pregeometry":
        executor_available = _quantum_executor_available() if tools_available is None else bool(tools_available)
        request_manifest, xyz_manifest = _build_xtb_manifests(mapping, xyz_dir=xyz_dir, executor_available=executor_available)
        request_files, xyz_files = _write_manifest_batches(
            request_manifest,
            xyz_manifest,
            output_dir=output_dir,
            request_prefix="governance_xtb_requests",
            xyz_prefix="governance_xtb_xyz_manifest",
            batch_size=batch_size,
        )
    else:
        executor_available = _psi4_executor_available() if tools_available is None else bool(tools_available)
        request_manifest, xyz_manifest = _build_dft_manifests(mapping, xyz_dir=xyz_dir, executor_available=executor_available)
        request_files, xyz_files = _write_manifest_batches(
            request_manifest,
            xyz_manifest,
            output_dir=output_dir,
            request_prefix="governance_dft_singlepoint_requests",
            xyz_prefix="governance_dft_singlepoint_xyz_manifest",
            batch_size=batch_size,
        )

    mapping_report = _ensure_columns(mapping, GOVERNANCE_MAPPING_COLUMNS)
    mapping_report_path = output_dir / "governance_dft_mapping_report.csv"
    mapping_report.to_csv(mapping_report_path, index=False)
    summary = _summary(
        mode=mode,
        queue=queue,
        mapping=mapping_report,
        request_manifest=request_manifest,
        xyz_manifest=xyz_manifest,
        batch_size=batch_size,
        executor_available=executor_available,
        request_files=request_files,
        xyz_files=xyz_files,
        mapping_report_path=mapping_report_path,
    )
    summary_path = output_dir / "governance_dft_mapping_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def _substance_requests(queue: pd.DataFrame) -> pd.DataFrame:
    queue = queue.copy().fillna("") if queue is not None else pd.DataFrame()
    if queue.empty:
        return pd.DataFrame(columns=["substance_id", "refrigerant_number", "requested_property_count", "requested_properties"])
    queue = _ensure_columns(queue, ["substance_id", "refrigerant_number", "requested_property"])
    queue["_substance_order"] = range(len(queue))
    rows: list[dict[str, Any]] = []
    for substance_id, group in queue.groupby("substance_id", sort=False, dropna=False):
        requested_properties = [_clean(value) for value in group["requested_property"].tolist() if _clean(value)]
        unique_properties = list(dict.fromkeys(requested_properties))
        refrigerant_number = _first_nonblank(group["refrigerant_number"].tolist())
        rows.append(
            {
                "substance_id": _clean(substance_id),
                "refrigerant_number": refrigerant_number,
                "requested_property_count": len(unique_properties),
                "requested_properties": ";".join(unique_properties),
                "_substance_order": int(group["_substance_order"].min()),
            }
        )
    return pd.DataFrame(rows).sort_values("_substance_order", kind="stable").drop(columns=["_substance_order"]).reset_index(drop=True)


def _seed_candidates_by_r_number(seed_catalog: pd.DataFrame, molecule_core: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    seed_catalog = _ensure_columns(seed_catalog.copy().fillna("") if seed_catalog is not None else pd.DataFrame(), ["seed_id", "r_number"])
    molecule_core = _ensure_columns(
        molecule_core.copy().fillna("") if molecule_core is not None else pd.DataFrame(),
        ["seed_id", "mol_id", "canonical_smiles", "isomeric_smiles"],
    )
    molecule_records = _best_molecule_by_seed_id(molecule_core)
    candidates: dict[str, list[dict[str, Any]]] = {}
    for index, seed in enumerate(seed_catalog.to_dict(orient="records")):
        r_number = _clean(seed.get("r_number"))
        seed_id = _clean(seed.get("seed_id"))
        if not r_number or not seed_id:
            continue
        mol = molecule_records.get(seed_id, {})
        record = {**seed, **mol, "_seed_order": index}
        candidates.setdefault(r_number, []).append(record)
    for r_number, records in candidates.items():
        records.sort(key=_seed_candidate_rank)
    return candidates


def _best_molecule_by_seed_id(molecule_core: pd.DataFrame) -> dict[str, dict[str, Any]]:
    records_by_seed: dict[str, list[dict[str, Any]]] = {}
    for index, record in enumerate(molecule_core.to_dict(orient="records")):
        seed_id = _clean(record.get("seed_id"))
        if not seed_id:
            continue
        row = dict(record)
        row["_molecule_order"] = index
        records_by_seed.setdefault(seed_id, []).append(row)
    output: dict[str, dict[str, Any]] = {}
    for seed_id, records in records_by_seed.items():
        records.sort(
            key=lambda row: (
                0 if _clean(row.get("status")) == "resolved" else 1,
                0 if _clean(row.get("canonical_smiles")) or _clean(row.get("isomeric_smiles")) else 1,
                _clean(row.get("mol_id")),
                int(row.get("_molecule_order", 0)),
            )
        )
        output[seed_id] = records[0]
    return output


def _seed_candidate_rank(record: dict[str, Any]) -> tuple[Any, ...]:
    has_mol = bool(_clean(record.get("mol_id")))
    has_smiles = bool(_clean(record.get("canonical_smiles")) or _clean(record.get("isomeric_smiles")))
    model_inclusion = _clean(record.get("model_inclusion"))
    entity_scope = _clean(record.get("entity_scope"))
    priority_tier = _int_or_default(record.get("priority_tier"), 999)
    return (
        0 if has_mol and has_smiles else (1 if has_mol else 2),
        0 if model_inclusion == "yes" else 1,
        0 if entity_scope == "refrigerant" else 1,
        priority_tier,
        _clean(record.get("seed_id")),
        int(record.get("_seed_order", 0)),
    )


def _enrich_mapping(
    mapping: pd.DataFrame,
    completed_xtb: set[str],
    completed_psi4: set[str],
    xtb_geometry_by_mol: dict[str, Path],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for record in mapping.fillna("").to_dict(orient="records"):
        row = dict(record)
        mol_id = _clean(row.get("mol_id"))
        smiles = _clean(row.get("isomeric_smiles")) or _clean(row.get("canonical_smiles"))
        mapped = _clean(row.get("mapping_status")) == "mapped" and bool(mol_id)
        has_xtb = mapped and mol_id in completed_xtb
        has_psi4 = mapped and mol_id in completed_psi4
        xtb_request_id = governance_xtb_request_id(mol_id, smiles) if mapped else ""
        dft_request_id = governance_dft_request_id(mol_id, smiles) if mapped else ""
        xtb_geometry = xtb_geometry_by_mol.get(mol_id)
        if not mapped:
            xtb_enqueue_status = "unmapped"
            dft_enqueue_status = "unmapped"
            dft_blocking_issue = _clean(row.get("mapping_reason"))
        else:
            xtb_enqueue_status = "already_completed" if has_xtb else "queued"
            if has_psi4:
                dft_enqueue_status = "already_completed"
                dft_blocking_issue = ""
            elif xtb_geometry is not None and xtb_geometry.exists():
                dft_enqueue_status = "queued"
                dft_blocking_issue = ""
            else:
                dft_enqueue_status = "blocked_missing_xtb_geometry"
                dft_blocking_issue = "missing_completed_xtb_geometry"
        row.update(
            {
                "has_completed_xtb": int(bool(has_xtb)),
                "has_completed_psi4_dft": int(bool(has_psi4)),
                "xtb_request_id": xtb_request_id,
                "dft_request_id": dft_request_id,
                "xtb_enqueue_status": xtb_enqueue_status,
                "dft_enqueue_status": dft_enqueue_status,
                "dft_xyz_source_path": str(xtb_geometry) if xtb_geometry is not None else "",
                "dft_blocking_issue": dft_blocking_issue,
            }
        )
        rows.append(row)
    return _ensure_columns(pd.DataFrame(rows), GOVERNANCE_MAPPING_COLUMNS)


def _build_xtb_manifests(mapping: pd.DataFrame, *, xyz_dir: Path, executor_available: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    request_rows: list[dict[str, Any]] = []
    xyz_rows: list[dict[str, Any]] = []
    candidates = mapping.loc[mapping["xtb_enqueue_status"].astype(str).eq("queued")].copy()
    for record in candidates.to_dict(orient="records"):
        mol_id = _clean(record.get("mol_id"))
        smiles = _clean(record.get("isomeric_smiles")) or _clean(record.get("canonical_smiles"))
        request_id = _clean(record.get("xtb_request_id")) or governance_xtb_request_id(mol_id, smiles)
        xyz_path = xyz_dir / f"{request_id}.xyz"
        xyz_generated, xyz_note = _write_xyz_from_smiles(smiles, xyz_path, request_id=request_id, mol_id=mol_id)
        status = "ready_for_executor" if executor_available else "pending_executor_unavailable"
        recommended_next_action = "run_quantum"
        if not xyz_generated:
            status = "blocked_xyz_generation_failed"
            recommended_next_action = "curate_structure"
        notes = (
            "governance DFT backlog pregeometry request; "
            "first-stage scalar route only; "
            f"substance_id={_clean(record.get('substance_id'))}; "
            f"requested_properties={_clean(record.get('requested_properties'))}"
        )
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
                "recommended_next_action": recommended_next_action,
                "notes": notes,
            }
        )
        xyz_rows.append(
            {
                "request_id": request_id,
                "mol_id": mol_id,
                "xyz_path": str(xyz_path),
                "xyz_status": "generated" if xyz_generated else "generation_failed",
                "notes": f"{xyz_note}; xTB executor {'available' if executor_available else 'unavailable'}",
            }
        )
    return _ensure_columns(pd.DataFrame(request_rows), QUANTUM_REQUEST_COLUMNS), _ensure_columns(pd.DataFrame(xyz_rows), QUANTUM_XYZ_MANIFEST_COLUMNS)


def _build_dft_manifests(mapping: pd.DataFrame, *, xyz_dir: Path, executor_available: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    request_rows: list[dict[str, Any]] = []
    xyz_rows: list[dict[str, Any]] = []
    candidates = mapping.loc[mapping["dft_enqueue_status"].astype(str).eq("queued")].copy()
    for record in candidates.to_dict(orient="records"):
        mol_id = _clean(record.get("mol_id"))
        smiles = _clean(record.get("isomeric_smiles")) or _clean(record.get("canonical_smiles"))
        request_id = _clean(record.get("dft_request_id")) or governance_dft_request_id(mol_id, smiles)
        source_path = Path(_clean(record.get("dft_xyz_source_path")))
        xyz_path = xyz_dir / f"{request_id}.xyz"
        xyz_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, xyz_path)
        status = "ready_for_executor" if executor_available else "pending_executor_unavailable"
        notes = (
            "governance DFT backlog singlepoint request; "
            "phase1 scalar features only: "
            f"{','.join(PHASE1_DFT_FEATURE_KEYS)}; "
            f"substance_id={_clean(record.get('substance_id'))}; "
            f"requested_properties={_clean(record.get('requested_properties'))}"
        )
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
                "notes": notes,
            }
        )
        xyz_rows.append(
            {
                "request_id": request_id,
                "mol_id": mol_id,
                "xyz_path": str(xyz_path),
                "xyz_status": "generated",
                "notes": f"XYZ copied from completed xTB optimized geometry for Psi4 singlepoint: {source_path}",
            }
        )
    return _ensure_columns(pd.DataFrame(request_rows), QUANTUM_REQUEST_COLUMNS), _ensure_columns(pd.DataFrame(xyz_rows), QUANTUM_XYZ_MANIFEST_COLUMNS)


def _write_manifest_batches(
    request_manifest: pd.DataFrame,
    xyz_manifest: pd.DataFrame,
    *,
    output_dir: Path,
    request_prefix: str,
    xyz_prefix: str,
    batch_size: int,
) -> tuple[list[str], list[str]]:
    request_manifest = _ensure_columns(request_manifest.copy().fillna(""), QUANTUM_REQUEST_COLUMNS)
    xyz_manifest = _ensure_columns(xyz_manifest.copy().fillna(""), QUANTUM_XYZ_MANIFEST_COLUMNS)
    request_files: list[str] = []
    xyz_files: list[str] = []
    if request_manifest.empty:
        request_path = output_dir / f"{request_prefix}_batch001.csv"
        xyz_path = output_dir / f"{xyz_prefix}_batch001.csv"
        request_manifest.to_csv(request_path, index=False)
        xyz_manifest.to_csv(xyz_path, index=False)
        return [str(request_path)], [str(xyz_path)]

    for batch_index, start in enumerate(range(0, len(request_manifest), batch_size), start=1):
        request_batch = request_manifest.iloc[start : start + batch_size].copy()
        request_ids = request_batch["request_id"].astype(str).tolist()
        xyz_batch = xyz_manifest.loc[xyz_manifest["request_id"].astype(str).isin(request_ids)].copy()
        xyz_batch["_request_order"] = xyz_batch["request_id"].astype(str).map({request_id: idx for idx, request_id in enumerate(request_ids)})
        xyz_batch = xyz_batch.sort_values("_request_order", kind="stable").drop(columns=["_request_order"])
        request_path = output_dir / f"{request_prefix}_batch{batch_index:03d}.csv"
        xyz_path = output_dir / f"{xyz_prefix}_batch{batch_index:03d}.csv"
        request_batch.to_csv(request_path, index=False)
        xyz_batch.to_csv(xyz_path, index=False)
        request_files.append(str(request_path))
        xyz_files.append(str(xyz_path))
    return request_files, xyz_files


def _summary(
    *,
    mode: str,
    queue: pd.DataFrame,
    mapping: pd.DataFrame,
    request_manifest: pd.DataFrame,
    xyz_manifest: pd.DataFrame,
    batch_size: int,
    executor_available: bool,
    request_files: list[str],
    xyz_files: list[str],
    mapping_report_path: Path,
) -> dict[str, Any]:
    mapped = mapping["mapping_status"].astype(str).eq("mapped") if not mapping.empty else pd.Series(dtype=bool)
    return {
        "mode": mode,
        "queue_row_count": int(len(queue)) if queue is not None else 0,
        "governance_substance_count": int(len(mapping)),
        "mapped_substance_count": int(mapped.sum()) if not mapping.empty else 0,
        "unmapped_substance_count": int((~mapped).sum()) if not mapping.empty else 0,
        "completed_xtb_molecule_count": int(mapping.loc[mapped, "has_completed_xtb"].astype(int).sum()) if not mapping.empty else 0,
        "completed_psi4_dft_molecule_count": int(mapping.loc[mapped, "has_completed_psi4_dft"].astype(int).sum()) if not mapping.empty else 0,
        "pending_xtb_request_count": int(mapping["xtb_enqueue_status"].astype(str).eq("queued").sum()) if not mapping.empty else 0,
        "pending_dft_request_count": int(mapping["dft_enqueue_status"].astype(str).eq("queued").sum()) if not mapping.empty else 0,
        "dft_blocked_missing_xtb_geometry_count": int(mapping["dft_enqueue_status"].astype(str).eq("blocked_missing_xtb_geometry").sum()) if not mapping.empty else 0,
        "request_batch_count": len(request_files),
        "request_count": int(len(request_manifest)),
        "xyz_manifest_count": int(len(xyz_manifest)),
        "xyz_generated_count": int(xyz_manifest["xyz_status"].astype(str).eq("generated").sum()) if not xyz_manifest.empty else 0,
        "batch_size": int(batch_size),
        "executor_available": bool(executor_available),
        "executor_status": "available" if executor_available else "unavailable",
        "phase1_dft_feature_keys": list(PHASE1_DFT_FEATURE_KEYS),
        "phase2_deferred_scope": list(PHASE2_DEFERRED_SCOPE),
        "dft_program": "psi4",
        "dft_theory_level": PSI4_DFT_THEORY_LEVEL,
        "dft_basis_set": PSI4_DFT_BASIS_SET,
        "request_files": request_files,
        "xyz_manifest_files": xyz_files,
        "mapping_report": str(mapping_report_path),
    }


def _completed_mol_ids(
    quantum_job: pd.DataFrame,
    quantum_results: pd.DataFrame,
    *,
    program: str,
    theory_level: str,
    basis_set: str,
) -> set[str]:
    completed: set[str] = set()
    if quantum_job is not None and not quantum_job.empty:
        job = _ensure_columns(quantum_job.copy().fillna(""), ["mol_id", "status", "program", "theory_level", "basis_set", "derived_observation_count"])
        mask = (
            job["program"].astype(str).str.lower().eq(program.lower())
            & job["status"].astype(str).eq("succeeded")
            & job["theory_level"].astype(str).eq(theory_level)
            & job["basis_set"].astype(str).eq(basis_set)
            & (pd.to_numeric(job["derived_observation_count"], errors="coerce").fillna(0).astype(int) >= len(QUANTUM_CANONICAL_FEATURE_KEYS))
        )
        completed.update(_clean(value) for value in job.loc[mask, "mol_id"].tolist() if _clean(value))

    if quantum_results is not None and not quantum_results.empty:
        rows = _ensure_columns(quantum_results.copy().fillna(""), QUANTUM_INPUT_COLUMNS)
        rows = rows.loc[
            rows["program"].astype(str).str.lower().eq(program.lower())
            & rows["theory_level"].astype(str).eq(theory_level)
            & rows["basis_set"].astype(str).eq(basis_set)
            & rows["canonical_feature_key"].astype(str).isin(QUANTUM_CANONICAL_FEATURE_KEYS)
            & rows["converged"].map(_truthy)
            & pd.to_numeric(rows["value_num"], errors="coerce").notna()
        ].copy()
        if not rows.empty:
            feature_counts = rows.groupby("mol_id")["canonical_feature_key"].nunique()
            completed.update(_clean(mol_id) for mol_id, count in feature_counts.items() if int(count) >= len(QUANTUM_CANONICAL_FEATURE_KEYS))
    return completed


def _completed_xtb_geometry_by_mol_id(
    quantum_results: pd.DataFrame,
    quantum_artifact: pd.DataFrame,
    quantum_job: pd.DataFrame,
) -> dict[str, Path]:
    completed_xtb = _completed_mol_ids(quantum_job, quantum_results, program="xtb", theory_level="GFN2-xTB", basis_set="")
    completed_xtb_request_ids = _completed_request_ids(quantum_job, quantum_results, program="xtb", theory_level="GFN2-xTB", basis_set="")
    candidates: dict[str, list[str]] = {}
    if quantum_results is not None and not quantum_results.empty:
        rows = _ensure_columns(quantum_results.copy().fillna(""), QUANTUM_INPUT_COLUMNS)
        for record in rows.to_dict(orient="records"):
            mol_id = _clean(record.get("mol_id"))
            if mol_id in completed_xtb:
                candidates.setdefault(mol_id, []).append(_clean(record.get("artifact_uri")))
    if quantum_artifact is not None and not quantum_artifact.empty:
        artifacts = _ensure_columns(quantum_artifact.copy().fillna(""), ["request_id", "mol_id", "artifact_uri"])
        for record in artifacts.to_dict(orient="records"):
            mol_id = _clean(record.get("mol_id"))
            request_id = _clean(record.get("request_id"))
            if mol_id in completed_xtb or request_id in completed_xtb_request_ids:
                candidates.setdefault(mol_id, []).append(_clean(record.get("artifact_uri")))
    output: dict[str, Path] = {}
    for mol_id, uris in candidates.items():
        for uri in list(dict.fromkeys(uris)):
            for path in _xtb_geometry_candidates(uri):
                if path.exists():
                    output[mol_id] = path
                    break
            if mol_id in output:
                break
    return output


def _completed_request_ids(
    quantum_job: pd.DataFrame,
    quantum_results: pd.DataFrame,
    *,
    program: str,
    theory_level: str,
    basis_set: str,
) -> set[str]:
    completed: set[str] = set()
    if quantum_job is not None and not quantum_job.empty:
        job = _ensure_columns(quantum_job.copy().fillna(""), ["request_id", "status", "program", "theory_level", "basis_set", "derived_observation_count"])
        mask = (
            job["program"].astype(str).str.lower().eq(program.lower())
            & job["status"].astype(str).eq("succeeded")
            & job["theory_level"].astype(str).eq(theory_level)
            & job["basis_set"].astype(str).eq(basis_set)
            & (pd.to_numeric(job["derived_observation_count"], errors="coerce").fillna(0).astype(int) >= len(QUANTUM_CANONICAL_FEATURE_KEYS))
        )
        completed.update(_clean(value) for value in job.loc[mask, "request_id"].tolist() if _clean(value))
    if quantum_results is not None and not quantum_results.empty:
        rows = _ensure_columns(quantum_results.copy().fillna(""), QUANTUM_INPUT_COLUMNS)
        rows = rows.loc[
            rows["program"].astype(str).str.lower().eq(program.lower())
            & rows["theory_level"].astype(str).eq(theory_level)
            & rows["basis_set"].astype(str).eq(basis_set)
            & rows["canonical_feature_key"].astype(str).isin(QUANTUM_CANONICAL_FEATURE_KEYS)
            & rows["converged"].map(_truthy)
            & pd.to_numeric(rows["value_num"], errors="coerce").notna()
        ].copy()
        if not rows.empty:
            feature_counts = rows.groupby("request_id")["canonical_feature_key"].nunique()
            completed.update(_clean(request_id) for request_id, count in feature_counts.items() if int(count) >= len(QUANTUM_CANONICAL_FEATURE_KEYS))
    return completed


def _xtb_geometry_candidates(artifact_uri: str) -> list[Path]:
    uri = _clean(artifact_uri)
    if not uri:
        return []
    if "://" in uri and not uri.startswith("file://"):
        return []
    path = Path(uri.removeprefix("file://"))
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    roots = [path] if path.is_dir() else [path.parent]
    candidates: list[Path] = []
    for root in roots:
        candidates.extend(
            [
                root / "xtbopt.xyz",
                root / "gfn2_opt" / "xtbopt.xyz",
            ]
        )
        candidates.extend(sorted(root.glob("attempt_*_gfn2_opt/xtbopt.xyz")))
    return candidates


def _ensure_columns(frame: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    out = frame.copy()
    for column in columns:
        if column not in out.columns:
            out[column] = ""
    if columns:
        ordered = list(columns) + [column for column in out.columns if column not in columns]
        out = out[ordered]
    return out


def _clean(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _first_nonblank(values: Iterable[Any]) -> str:
    for value in values:
        cleaned = _clean(value)
        if cleaned:
            return cleaned
    return ""


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    cleaned = _clean(value).lower()
    return cleaned in {"1", "true", "yes", "y", "succeeded", "success"}


def _int_or_default(value: Any, default: int) -> int:
    try:
        return int(float(_clean(value)))
    except ValueError:
        return default
