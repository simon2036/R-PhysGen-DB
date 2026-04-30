"""Phase-2 governance quantum manifests, parsers, and local runners."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import tarfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from r_physgen_db.constants import DATA_DIR, PROJECT_ROOT
from r_physgen_db.governance_quantum_batches import (
    DEFAULT_OUTPUT_DIR,
    GOVERNANCE_QUEUE_PATH,
    MOLECULE_CORE_PATH,
    QUANTUM_ARTIFACT_PATH,
    QUANTUM_JOB_PATH,
    QUANTUM_RESULTS_PATH,
    SEED_CATALOG_PATH,
    build_governance_mapping,
)
from r_physgen_db.governance_quantum_batches import _completed_xtb_geometry_by_mol_id as _governance_completed_xtb_geometry_by_mol_id
from r_physgen_db.psi4_quantum import _charge_multiplicity_from_request
from r_physgen_db.quantum_pilot import (
    PHASE2_QUANTUM_FEATURES,
    QUANTUM_INPUT_COLUMNS,
    QUANTUM_REQUEST_COLUMNS,
    QUANTUM_XYZ_MANIFEST_COLUMNS,
    merge_quantum_result_rows,
)

PHASE2_XTB_HESSIAN_FEATURE_KEYS = (
    "quantum.zpe",
    "quantum.lowest_real_frequency",
    "quantum.thermal_enthalpy_correction",
    "quantum.thermal_gibbs_correction",
)
PHASE2_ORCA_FEATURE_KEYS = PHASE2_XTB_HESSIAN_FEATURE_KEYS
PHASE2_CONFORMER_FEATURE_KEYS = (
    "quantum.conformer_count",
    "quantum.conformer_energy_window",
)

PHASE2_REQUEST_COLUMNS = [
    *QUANTUM_REQUEST_COLUMNS,
    "phase2_task",
    "source_request_id",
    "source_xyz_path",
    "heavy_atom_count",
    "execution_kind",
    "blocker_reason",
]
PHASE2_MAPPING_COLUMNS = [
    "substance_id",
    "refrigerant_number",
    "requested_property_count",
    "requested_properties",
    "seed_id",
    "mol_id",
    "canonical_smiles",
    "isomeric_smiles",
    "heavy_atom_count",
    "mapping_status",
    "mapping_reason",
    "xtb_source_xyz_path",
    "phase2_blocking_issue",
]
PHASE2_BLOCKER_COLUMNS = [
    "substance_id",
    "refrigerant_number",
    "mol_id",
    "target_output",
    "blocker_type",
    "blocker_reason",
    "recommended_next_action",
    "artifact_uri",
    "notes",
]
VIBRATIONAL_MODE_COLUMNS = [
    "request_id",
    "mol_id",
    "program",
    "theory_level",
    "mode_index",
    "frequency_cm_inv",
    "ir_intensity_km_mol",
    "artifact_uri",
    "artifact_sha256",
    "notes",
]
ORCA_DETAIL_COLUMNS = VIBRATIONAL_MODE_COLUMNS
ORCA_ATOMIC_CHARGE_COLUMNS = [
    "request_id",
    "mol_id",
    "program",
    "theory_level",
    "atom_index",
    "element",
    "charge_scheme",
    "partial_charge",
    "artifact_uri",
    "artifact_sha256",
    "notes",
]
CREST_DETAIL_COLUMNS = [
    "request_id",
    "mol_id",
    "conformer_index",
    "relative_energy_kcal_mol",
    "relative_energy_kj_mol",
    "boltzmann_weight",
    "source_xyz",
    "artifact_uri",
    "artifact_sha256",
    "notes",
]

DEFAULT_PHASE2_XYZ_DIR = DEFAULT_OUTPUT_DIR / "governance_phase2_xyz"
DEFAULT_PHASE2_ARTIFACT_DIR = DATA_DIR / "raw" / "manual" / "quantum_phase2_artifacts"
DEFAULT_PHASE2_VIBRATIONAL_MODES_PATH = DATA_DIR / "raw" / "manual" / "quantum_phase2_vibrational_modes.csv"
DEFAULT_PHASE2_ATOMIC_CHARGES_PATH = DATA_DIR / "raw" / "manual" / "quantum_phase2_atomic_charges.csv"
DEFAULT_PHASE2_CONFORMER_DETAIL_PATH = DATA_DIR / "raw" / "manual" / "quantum_phase2_conformer_ensemble.csv"


@dataclass(slots=True)
class ParsedHessian:
    scalars: dict[str, float]
    modes: list[dict[str, Any]]
    imaginary_frequency_count: int
    program_version: str = ""


@dataclass(slots=True)
class ParsedCrest:
    scalars: dict[str, float]
    conformers: list[dict[str, Any]]
    program_version: str = ""


@dataclass(slots=True)
class ParsedOrca:
    scalars: dict[str, float]
    modes: list[dict[str, Any]]
    atomic_charges: list[dict[str, Any]]
    imaginary_frequency_count: int
    normal_termination: bool
    optimization_converged: bool
    program_version: str = ""


@dataclass(slots=True)
class Phase2JobResult:
    request_id: str
    status: str
    rows: list[dict[str, Any]]
    vibrational_modes: list[dict[str, Any]]
    atomic_charges: list[dict[str, Any]]
    conformers: list[dict[str, Any]]


def governance_phase2_request_id(mol_id: str, smiles: str, *, program: str, task: str) -> str:
    """Return a stable request id for a phase-2 governance calculation."""

    payload = "|".join([_clean(mol_id), _clean(smiles), "phase2", _clean(program).lower(), _clean(task).lower()])
    return "qreq_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def materialize_governance_phase2_batches(
    *,
    queue: pd.DataFrame,
    seed_catalog: pd.DataFrame,
    molecule_core: pd.DataFrame,
    quantum_job: pd.DataFrame | None = None,
    quantum_results: pd.DataFrame | None = None,
    quantum_artifact: pd.DataFrame | None = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    xyz_dir: Path = DEFAULT_PHASE2_XYZ_DIR,
    crest_heavy_atom_min: int = 6,
    orca_smoke_size: int = 3,
    batch_size_orca: int = 20,
    mapped_only: bool = True,
    xtb_available: bool | None = None,
    crest_available: bool | None = None,
    orca_available: bool | None = None,
) -> dict[str, Any]:
    """Write phase-2 manifests for mapped governance molecules and blockers for gaps."""

    output_dir = Path(output_dir)
    xyz_dir = Path(xyz_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    xyz_dir.mkdir(parents=True, exist_ok=True)
    quantum_job = _ensure_columns((quantum_job if quantum_job is not None else pd.DataFrame()).copy().fillna(""), [])
    quantum_results = _ensure_columns((quantum_results if quantum_results is not None else pd.DataFrame()).copy().fillna(""), QUANTUM_INPUT_COLUMNS)
    quantum_artifact = _ensure_columns((quantum_artifact if quantum_artifact is not None else pd.DataFrame()).copy().fillna(""), [])

    mapping = build_governance_mapping(queue, seed_catalog, molecule_core)
    mapping = _attach_molecule_phase2_fields(mapping, molecule_core)
    xtb_geometry_by_mol = _governance_completed_xtb_geometry_by_mol_id(quantum_results, quantum_artifact, quantum_job)
    mapping = _attach_phase2_source_geometry(mapping, xtb_geometry_by_mol)

    if mapped_only:
        candidates = mapping.loc[mapping["mapping_status"].astype(str).eq("mapped")].copy()
    else:
        candidates = mapping.copy()
    candidates = candidates.loc[candidates["phase2_blocking_issue"].astype(str).eq("")].copy()

    completed_xtb_hessian = _completed_phase2_request_ids(quantum_results, program="xtb", feature_keys=PHASE2_XTB_HESSIAN_FEATURE_KEYS)
    completed_crest = _completed_phase2_request_ids(quantum_results, program="crest", feature_keys=PHASE2_CONFORMER_FEATURE_KEYS)
    completed_orca = _completed_phase2_request_ids(quantum_results, program="orca", feature_keys=PHASE2_ORCA_FEATURE_KEYS)

    xtb_requests, xtb_xyz = _phase2_request_manifests(
        candidates,
        xyz_dir=xyz_dir / "xtb_hessian",
        program="xtb",
        task="hessian",
        method_family="semiempirical",
        theory_level="GFN2-xTB+hessian",
        basis_set="",
        completed_request_ids=completed_xtb_hessian,
        executor_available=_tool_available("xtb", xtb_available),
        execution_kind="executor",
    )
    crest_requests, crest_xyz = _crest_request_manifests(
        candidates,
        xyz_dir=xyz_dir / "crest",
        crest_heavy_atom_min=crest_heavy_atom_min,
        completed_request_ids=completed_crest,
        executor_available=_tool_available("crest", crest_available),
    )
    orca_requests_all, orca_xyz_all = _phase2_request_manifests(
        candidates,
        xyz_dir=xyz_dir / "orca_optfreq",
        program="orca",
        task="optfreq",
        method_family="DFT",
        theory_level="B3LYP-D3BJ",
        basis_set="def2-SVP",
        completed_request_ids=completed_orca,
        executor_available=_orca_available(orca_available),
        execution_kind="executor",
    )

    smoke_size = max(0, int(orca_smoke_size or 0))
    orca_smoke = orca_requests_all.head(smoke_size).copy()
    orca_full = orca_requests_all.iloc[smoke_size:].copy()
    orca_smoke_xyz = _xyz_subset(orca_xyz_all, orca_smoke)
    orca_full_xyz = _xyz_subset(orca_xyz_all, orca_full)

    _write_manifest(xtb_requests, output_dir / "governance_phase2_xtb_hessian_requests.csv", PHASE2_REQUEST_COLUMNS)
    _write_manifest(xtb_xyz, output_dir / "governance_phase2_xtb_hessian_xyz_manifest.csv", QUANTUM_XYZ_MANIFEST_COLUMNS)
    _write_manifest(crest_requests, output_dir / "governance_phase2_crest_requests.csv", PHASE2_REQUEST_COLUMNS)
    _write_manifest(crest_xyz, output_dir / "governance_phase2_crest_xyz_manifest.csv", QUANTUM_XYZ_MANIFEST_COLUMNS)
    _write_manifest(orca_smoke, output_dir / "governance_phase2_orca_optfreq_smoke_requests.csv", PHASE2_REQUEST_COLUMNS)
    _write_manifest(orca_smoke_xyz, output_dir / "governance_phase2_orca_optfreq_smoke_xyz_manifest.csv", QUANTUM_XYZ_MANIFEST_COLUMNS)
    orca_request_files, orca_xyz_files = _write_orca_batches(orca_full, orca_full_xyz, output_dir=output_dir, batch_size=batch_size_orca)

    mapping_report = _ensure_columns(mapping, PHASE2_MAPPING_COLUMNS)
    mapping_report_path = output_dir / "governance_phase2_mapping_report.csv"
    mapping_report.to_csv(mapping_report_path, index=False)
    blockers = _phase2_blockers(mapping_report)
    blocker_path = output_dir / "governance_phase2_blockers.csv"
    blockers.to_csv(blocker_path, index=False)

    summary = {
        "governance_substance_count": int(len(mapping_report)),
        "mapped_substance_count": int(mapping_report["mapping_status"].astype(str).eq("mapped").sum()) if not mapping_report.empty else 0,
        "unmapped_substance_count": int(mapping_report["mapping_status"].astype(str).ne("mapped").sum()) if not mapping_report.empty else 0,
        "xtb_hessian_request_count": int(len(xtb_requests)),
        "crest_request_count": int(len(crest_requests)),
        "crest_external_request_count": int(crest_requests["execution_kind"].astype(str).eq("crest").sum()) if not crest_requests.empty else 0,
        "crest_singleton_request_count": int(crest_requests["execution_kind"].astype(str).eq("singleton").sum()) if not crest_requests.empty else 0,
        "orca_smoke_request_count": int(len(orca_smoke)),
        "orca_full_request_count": int(len(orca_full)),
        "orca_batch_count": int(len(orca_request_files)),
        "blocker_count": int(len(blockers)),
        "mapping_report": str(mapping_report_path),
        "blocker_report": str(blocker_path),
        "xtb_hessian_requests": str(output_dir / "governance_phase2_xtb_hessian_requests.csv"),
        "xtb_hessian_xyz_manifest": str(output_dir / "governance_phase2_xtb_hessian_xyz_manifest.csv"),
        "crest_requests": str(output_dir / "governance_phase2_crest_requests.csv"),
        "crest_xyz_manifest": str(output_dir / "governance_phase2_crest_xyz_manifest.csv"),
        "orca_smoke_requests": str(output_dir / "governance_phase2_orca_optfreq_smoke_requests.csv"),
        "orca_smoke_xyz_manifest": str(output_dir / "governance_phase2_orca_optfreq_smoke_xyz_manifest.csv"),
        "orca_request_files": orca_request_files,
        "orca_xyz_manifest_files": orca_xyz_files,
        "phase2_feature_keys": sorted(PHASE2_QUANTUM_FEATURES),
    }
    (output_dir / "governance_phase2_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def parse_xtb_hessian_output(stdout_path: Path) -> ParsedHessian:
    stdout_path = Path(stdout_path)
    text = stdout_path.read_text(encoding="utf-8", errors="replace")
    scalars = _parse_thermochemistry_scalars(text)
    scalars.update({key: value for key, value in _parse_xtb_thermo_table(text).items() if key not in scalars})
    vibspectrum_path = stdout_path.parent / "vibspectrum"
    modes = _parse_xtb_vibspectrum(vibspectrum_path) if vibspectrum_path.exists() else _parse_mode_table(text)
    if "quantum.lowest_real_frequency" not in scalars:
        lowest = _lowest_real_frequency(modes)
        if lowest is not None:
            scalars["quantum.lowest_real_frequency"] = lowest
    imaginary = sum(1 for mode in modes if _float_or_none(mode.get("frequency_cm_inv")) is not None and float(mode["frequency_cm_inv"]) < 0)
    version_match = re.search(r"xtb\s+version\s+([^\n]+)", text, flags=re.IGNORECASE)
    return ParsedHessian(scalars=scalars, modes=modes, imaginary_frequency_count=imaginary, program_version=_clean(version_match.group(1)) if version_match else "")


def parse_crest_conformer_ensemble(energies_path: Path) -> ParsedCrest:
    text = Path(energies_path).read_text(encoding="utf-8", errors="replace") if Path(energies_path).exists() else ""
    conformers: list[dict[str, Any]] = []
    for line in text.splitlines():
        numbers = _numbers(line)
        if len(numbers) < 2:
            continue
        if len(numbers) >= 3 and abs(numbers[0]) < 500 and numbers[1] < 0:
            rel_kcal = numbers[0]
            total_eh = numbers[1]
            weight = numbers[2]
        elif len(numbers) >= 3 and numbers[0] < 0:
            total_eh = numbers[0]
            rel_kcal = numbers[1]
            weight = numbers[2]
        elif len(numbers) >= 3 and 0 <= numbers[2] <= 1:
            total_eh = math.nan
            rel_kcal = numbers[0]
            weight = numbers[2]
        elif numbers[0] < 0:
            total_eh = numbers[0]
            rel_kcal = numbers[1]
            weight = math.nan
        else:
            total_eh = math.nan
            rel_kcal = numbers[0]
            weight = numbers[1] if len(numbers) > 1 and 0 <= numbers[1] <= 1 else math.nan
        conformers.append(
            {
                "conformer_index": len(conformers) + 1,
                "relative_energy_kcal_mol": float(rel_kcal),
                "relative_energy_kj_mol": float(rel_kcal) * 4.184,
                "boltzmann_weight": float(weight) if math.isfinite(weight) else None,
                "total_energy_eh": float(total_eh) if math.isfinite(total_eh) else None,
            }
        )
    if conformers and any(row.get("boltzmann_weight") is None for row in conformers):
        _fill_boltzmann_weights(conformers)
    if not conformers:
        scalars = {}
    else:
        energies = [float(row["relative_energy_kcal_mol"]) for row in conformers]
        scalars = {
            "quantum.conformer_count": float(len(conformers)),
            "quantum.conformer_energy_window": float(max(energies) - min(energies)),
        }
    return ParsedCrest(scalars=scalars, conformers=conformers)


def parse_orca_optfreq_output(stdout_path: Path) -> ParsedOrca:
    text = Path(stdout_path).read_text(encoding="utf-8", errors="replace")
    normal = "ORCA TERMINATED NORMALLY" in text.upper()
    opt_converged = bool(re.search(r"OPTIMIZATION\s+(?:HAS\s+)?CONVERGED|OPTIMIZATION RUN DONE", text, flags=re.IGNORECASE))
    scalars = _parse_thermochemistry_scalars(text)
    modes = _parse_orca_modes(text)
    if "quantum.lowest_real_frequency" not in scalars:
        lowest = _lowest_real_frequency(modes)
        if lowest is not None:
            scalars["quantum.lowest_real_frequency"] = lowest
    charges = _parse_orca_atomic_charges(text)
    imaginary = sum(1 for mode in modes if _float_or_none(mode.get("frequency_cm_inv")) is not None and float(mode["frequency_cm_inv"]) < 0)
    version_match = re.search(r"Program Version\s+([^\n]+)|ORCA VERSION\s+([^\n]+)", text, flags=re.IGNORECASE)
    version = ""
    if version_match:
        version = _clean(next(group for group in version_match.groups() if group))
    return ParsedOrca(
        scalars=scalars,
        modes=modes,
        atomic_charges=charges,
        imaginary_frequency_count=imaginary,
        normal_termination=normal,
        optimization_converged=opt_converged,
        program_version=version,
    )


def run_xtb_phase2_hessian(
    *,
    requests_path: Path,
    xyz_manifest_path: Path,
    output_path: Path = QUANTUM_RESULTS_PATH,
    artifact_dir: Path = DEFAULT_PHASE2_ARTIFACT_DIR / "xtb_hessian",
    vibrational_modes_path: Path = DEFAULT_PHASE2_VIBRATIONAL_MODES_PATH,
    xtb_bin: Path | str | None = None,
    limit: int | None = None,
    jobs: int = 1,
    threads_per_job: int = 1,
    resume: bool = True,
    allow_missing_executor: bool = False,
    retry_failed_only: bool = False,
    completion_required: bool = False,
) -> dict[str, int]:
    resolved = _resolve_bin(xtb_bin, env_var="R_PHYSGEN_XTB_BIN", default_name="xtb")
    if resolved is None and not allow_missing_executor:
        raise FileNotFoundError("xTB executable not found; set R_PHYSGEN_XTB_BIN or pass --xtb-bin")
    return _run_phase2_jobs(
        requests_path=requests_path,
        xyz_manifest_path=xyz_manifest_path,
        output_path=output_path,
        artifact_dir=artifact_dir,
        vibrational_modes_path=vibrational_modes_path,
        atomic_charges_path=None,
        conformer_detail_path=None,
        feature_keys=PHASE2_XTB_HESSIAN_FEATURE_KEYS,
        program="xtb",
        limit=limit,
        jobs=jobs,
        resume=resume,
        retry_failed_only=retry_failed_only,
        completion_required=completion_required,
        runner=lambda row, xyz: _run_one_xtb_hessian(
            row,
            xyz,
            xtb_bin=resolved,
            artifact_dir=artifact_dir,
            threads_per_job=threads_per_job,
            allow_missing_executor=allow_missing_executor,
        ),
    )


def run_crest_conformer_phase2(
    *,
    requests_path: Path,
    xyz_manifest_path: Path,
    output_path: Path = QUANTUM_RESULTS_PATH,
    artifact_dir: Path = DEFAULT_PHASE2_ARTIFACT_DIR / "crest_conformer",
    conformer_detail_path: Path = DEFAULT_PHASE2_CONFORMER_DETAIL_PATH,
    crest_bin: Path | str | None = None,
    limit: int | None = None,
    jobs: int = 1,
    threads_per_job: int = 1,
    resume: bool = True,
    allow_missing_executor: bool = False,
    retry_failed_only: bool = False,
    completion_required: bool = False,
) -> dict[str, int]:
    resolved = _resolve_bin(crest_bin, env_var="R_PHYSGEN_CREST_BIN", default_name="crest") if crest_bin is not None else _resolve_bin(None, env_var="R_PHYSGEN_CREST_BIN", default_name="crest")
    return _run_phase2_jobs(
        requests_path=requests_path,
        xyz_manifest_path=xyz_manifest_path,
        output_path=output_path,
        artifact_dir=artifact_dir,
        vibrational_modes_path=None,
        atomic_charges_path=None,
        conformer_detail_path=conformer_detail_path,
        feature_keys=PHASE2_CONFORMER_FEATURE_KEYS,
        program="crest",
        limit=limit,
        jobs=jobs,
        resume=resume,
        retry_failed_only=retry_failed_only,
        completion_required=completion_required,
        runner=lambda row, xyz: _run_one_crest(
            row,
            xyz,
            crest_bin=resolved,
            artifact_dir=artifact_dir,
            threads_per_job=threads_per_job,
            allow_missing_executor=allow_missing_executor,
        ),
    )


def run_orca_phase2_optfreq(
    *,
    requests_path: Path,
    xyz_manifest_path: Path,
    output_path: Path = QUANTUM_RESULTS_PATH,
    artifact_dir: Path = DEFAULT_PHASE2_ARTIFACT_DIR / "orca_optfreq",
    vibrational_modes_path: Path = DEFAULT_PHASE2_VIBRATIONAL_MODES_PATH,
    atomic_charges_path: Path = DEFAULT_PHASE2_ATOMIC_CHARGES_PATH,
    orca_bin: Path | str | None = None,
    limit: int | None = None,
    jobs: int = 1,
    nprocs_per_job: int = 8,
    resume: bool = True,
    allow_missing_executor: bool = False,
    retry_failed_only: bool = False,
    completion_required: bool = False,
) -> dict[str, int]:
    resolved = _resolve_orca_bin(orca_bin)
    if resolved is None and not allow_missing_executor:
        raise FileNotFoundError(
            "ORCA quantum-chemistry executable not found; set R_PHYSGEN_ORCA_BIN or pass --orca-bin. "
            "Note that /usr/bin/orca is often the GNOME screen-reader binary, not the ORCA QC package."
        )
    return _run_phase2_jobs(
        requests_path=requests_path,
        xyz_manifest_path=xyz_manifest_path,
        output_path=output_path,
        artifact_dir=artifact_dir,
        vibrational_modes_path=vibrational_modes_path,
        atomic_charges_path=atomic_charges_path,
        conformer_detail_path=None,
        feature_keys=PHASE2_ORCA_FEATURE_KEYS,
        program="orca",
        limit=limit,
        jobs=jobs,
        resume=resume,
        retry_failed_only=retry_failed_only,
        completion_required=completion_required,
        runner=lambda row, xyz: _run_one_orca(
            row,
            xyz,
            orca_bin=resolved,
            artifact_dir=artifact_dir,
            nprocs_per_job=nprocs_per_job,
            allow_missing_executor=allow_missing_executor,
        ),
    )


def _run_phase2_jobs(
    *,
    requests_path: Path,
    xyz_manifest_path: Path,
    output_path: Path,
    artifact_dir: Path,
    vibrational_modes_path: Path | None,
    atomic_charges_path: Path | None,
    conformer_detail_path: Path | None,
    feature_keys: tuple[str, ...],
    program: str,
    limit: int | None,
    jobs: int,
    resume: bool,
    retry_failed_only: bool,
    completion_required: bool,
    runner,
) -> dict[str, int]:
    requests = _ensure_columns(pd.read_csv(requests_path).fillna(""), PHASE2_REQUEST_COLUMNS)
    if limit is not None:
        requests = requests.head(int(limit)).copy()
    xyz_manifest = _ensure_columns(pd.read_csv(xyz_manifest_path).fillna(""), QUANTUM_XYZ_MANIFEST_COLUMNS)
    xyz_by_request = {str(row["request_id"]): Path(str(row["xyz_path"])) for row in xyz_manifest.to_dict(orient="records") if _clean(row.get("request_id"))}
    existing = pd.read_csv(output_path).fillna("") if Path(output_path).exists() else pd.DataFrame(columns=QUANTUM_INPUT_COLUMNS)
    completed = _completed_phase2_request_ids(existing, program=program, feature_keys=feature_keys) if resume else set()
    failed_targets = _failed_phase2_request_ids(existing, program=program, feature_keys=feature_keys) if retry_failed_only else set()

    tasks: list[tuple[dict[str, Any], Path | None]] = []
    for row in requests.to_dict(orient="records"):
        request_id = _clean(row.get("request_id"))
        if not request_id:
            continue
        if retry_failed_only:
            if request_id not in failed_targets:
                continue
        elif request_id in completed:
            continue
        tasks.append((row, xyz_by_request.get(request_id)))

    artifact_dir.mkdir(parents=True, exist_ok=True)
    workers = max(1, int(jobs or 1))
    if workers == 1:
        results = [runner(row, xyz) for row, xyz in tasks]
    else:
        results = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(runner, row, xyz) for row, xyz in tasks]
            for future in as_completed(futures):
                results.append(future.result())

    rows: list[dict[str, Any]] = []
    modes: list[dict[str, Any]] = []
    charges: list[dict[str, Any]] = []
    conformers: list[dict[str, Any]] = []
    summary = {
        "requested": len(tasks),
        "succeeded": 0,
        "failed": 0,
        "resumed": 0 if retry_failed_only else len(completed),
        "rows_written": 0,
        "merged_rows": len(existing),
        "retry_failed_only": int(retry_failed_only),
        "completion_required": int(completion_required),
        "failed_target_count": len(failed_targets),
    }
    for result in sorted(results, key=lambda item: item.request_id):
        rows.extend(result.rows)
        modes.extend(result.vibrational_modes)
        charges.extend(result.atomic_charges)
        conformers.extend(result.conformers)
        if result.status == "succeeded":
            summary["succeeded"] += 1
        else:
            summary["failed"] += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged = merge_quantum_result_rows(existing, pd.DataFrame(rows, columns=QUANTUM_INPUT_COLUMNS))
    merged.to_csv(output_path, index=False)
    summary["rows_written"] = len(rows)
    summary["merged_rows"] = len(merged)
    if vibrational_modes_path is not None:
        _merge_detail_rows(vibrational_modes_path, modes, VIBRATIONAL_MODE_COLUMNS, ["request_id", "mode_index", "program"])
    if atomic_charges_path is not None:
        _merge_detail_rows(atomic_charges_path, charges, ORCA_ATOMIC_CHARGE_COLUMNS, ["request_id", "charge_scheme", "atom_index"])
    if conformer_detail_path is not None:
        _merge_detail_rows(conformer_detail_path, conformers, CREST_DETAIL_COLUMNS, ["request_id", "conformer_index"])

    if completion_required:
        target_request_ids = {_clean(row.get("request_id")) for row, _ in tasks if _clean(row.get("request_id"))}
        if retry_failed_only:
            incomplete = sorted(_failed_phase2_request_ids(merged, program=program, feature_keys=feature_keys) & target_request_ids)
        else:
            completed_after = _completed_phase2_request_ids(merged, program=program, feature_keys=feature_keys)
            incomplete = sorted(target_request_ids - completed_after)
        if incomplete:
            label = {"xtb": "xTB Hessian", "crest": "CREST", "orca": "ORCA"}.get(program, program)
            raise RuntimeError(f"{label} completion required but {len(incomplete)} request(s) remain incomplete: {', '.join(incomplete[:10])}")
    return summary


def _run_one_xtb_hessian(
    request_row: dict[str, Any],
    xyz_path: Path | None,
    *,
    xtb_bin: Path | None,
    artifact_dir: Path,
    threads_per_job: int,
    allow_missing_executor: bool,
) -> Phase2JobResult:
    request_id = _clean(request_row.get("request_id"))
    job_dir = artifact_dir / request_id
    job_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = job_dir / "xtb.stdout"
    stderr_path = job_dir / "xtb.stderr"
    returncode_path = job_dir / "xtb.returncode"
    if xyz_path is None or not xyz_path.exists():
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text(f"XYZ input missing for request_id={request_id}: {xyz_path}\n", encoding="utf-8")
        returncode_path.write_text("127", encoding="utf-8")
    elif xtb_bin is None:
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text("executor_unavailable: xTB executable not found\n", encoding="utf-8")
        returncode_path.write_text("127", encoding="utf-8")
    else:
        local_xyz = job_dir / "input.xyz"
        shutil.copy2(xyz_path, local_xyz)
        env = _thread_env(threads_per_job)
        completed = subprocess.run(
            [str(xtb_bin), str(local_xyz.resolve()), "--gfn", "2", "--ohess"],
            cwd=job_dir,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        stdout_path.write_text(completed.stdout, encoding="utf-8")
        stderr_path.write_text(completed.stderr, encoding="utf-8")
        returncode_path.write_text(str(completed.returncode), encoding="utf-8")
    artifact_path = _write_artifact_bundle(job_dir, request_id, "xtb_hessian")
    return _xtb_hessian_result(request_row, stdout_path, returncode_path, artifact_path, allow_missing_executor=allow_missing_executor)


def _xtb_hessian_result(request_row: dict[str, Any], stdout_path: Path, returncode_path: Path, artifact_path: Path, *, allow_missing_executor: bool) -> Phase2JobResult:
    request_id = _clean(request_row.get("request_id"))
    artifact_sha = _sha256_file(artifact_path)
    returncode = _returncode(returncode_path)
    try:
        parsed = parse_xtb_hessian_output(stdout_path)
    except Exception as exc:
        return _failed_result(request_row, program="xtb", artifact_path=artifact_path, artifact_sha=artifact_sha, notes=f"xTB Hessian parse failed after returncode={returncode}: {exc}")
    missing = [key for key in PHASE2_XTB_HESSIAN_FEATURE_KEYS if key not in parsed.scalars]
    if returncode != 0 or missing:
        notes = f"xTB Hessian failed or incomplete: returncode={returncode}; missing_features={','.join(missing)}"
        if allow_missing_executor:
            notes = f"executor_unavailable; {notes}"
        return _failed_result(request_row, program="xtb", artifact_path=artifact_path, artifact_sha=artifact_sha, notes=notes)
    rows = [
        _result_row(
            request_row,
            canonical_feature_key=key,
            value_num=parsed.scalars[key],
            unit=PHASE2_QUANTUM_FEATURES[key]["unit"],
            program="xtb",
            program_version=parsed.program_version,
            converged=1,
            imaginary_frequency_count=parsed.imaginary_frequency_count,
            artifact_uri=artifact_path.as_posix(),
            artifact_sha256=artifact_sha,
            quality_level="calculated_open_source",
            notes="xTB GFN2 --ohess phase-2 Hessian parsed from stdout/artifact bundle",
        )
        for key in PHASE2_XTB_HESSIAN_FEATURE_KEYS
    ]
    modes = [_mode_detail_row(request_row, mode, program="xtb", artifact_path=artifact_path, artifact_sha=artifact_sha) for mode in parsed.modes]
    return Phase2JobResult(request_id=request_id, status="succeeded", rows=rows, vibrational_modes=modes, atomic_charges=[], conformers=[])


def _run_one_crest(
    request_row: dict[str, Any],
    xyz_path: Path | None,
    *,
    crest_bin: Path | None,
    artifact_dir: Path,
    threads_per_job: int,
    allow_missing_executor: bool,
) -> Phase2JobResult:
    request_id = _clean(request_row.get("request_id"))
    job_dir = artifact_dir / request_id
    job_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = job_dir / "crest.stdout"
    stderr_path = job_dir / "crest.stderr"
    returncode_path = job_dir / "crest.returncode"
    energies_path = job_dir / "crest.energies"
    execution_kind = _clean(request_row.get("execution_kind")) or "crest"
    if xyz_path is None or not xyz_path.exists():
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text(f"XYZ input missing for request_id={request_id}: {xyz_path}\n", encoding="utf-8")
        returncode_path.write_text("127", encoding="utf-8")
    elif execution_kind == "singleton":
        shutil.copy2(xyz_path, job_dir / "singleton_conformer.xyz")
        energies_path.write_text("0.000 0.0 1.000\n", encoding="utf-8")
        stdout_path.write_text("singleton conformer artifact; CREST not required for low-flexibility molecule\n", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        returncode_path.write_text("0", encoding="utf-8")
    elif crest_bin is None:
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text("executor_unavailable: CREST executable not found\n", encoding="utf-8")
        returncode_path.write_text("127", encoding="utf-8")
    else:
        local_xyz = job_dir / "input.xyz"
        shutil.copy2(xyz_path, local_xyz)
        env = _thread_env(threads_per_job)
        completed = subprocess.run(
            [str(crest_bin), str(local_xyz.resolve()), "--gfn2", "-T", str(max(1, int(threads_per_job or 1)))],
            cwd=job_dir,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        stdout_path.write_text(completed.stdout, encoding="utf-8")
        stderr_path.write_text(completed.stderr, encoding="utf-8")
        returncode_path.write_text(str(completed.returncode), encoding="utf-8")
    artifact_path = _write_artifact_bundle(job_dir, request_id, "crest")
    return _crest_result(request_row, energies_path, returncode_path, artifact_path, allow_missing_executor=allow_missing_executor)


def _crest_result(request_row: dict[str, Any], energies_path: Path, returncode_path: Path, artifact_path: Path, *, allow_missing_executor: bool) -> Phase2JobResult:
    request_id = _clean(request_row.get("request_id"))
    artifact_sha = _sha256_file(artifact_path)
    returncode = _returncode(returncode_path)
    try:
        parsed = parse_crest_conformer_ensemble(energies_path)
    except Exception as exc:
        return _failed_result(request_row, program="crest", artifact_path=artifact_path, artifact_sha=artifact_sha, notes=f"CREST parse failed after returncode={returncode}: {exc}")
    missing = [key for key in PHASE2_CONFORMER_FEATURE_KEYS if key not in parsed.scalars]
    if returncode != 0 or missing:
        notes = f"CREST failed or incomplete: returncode={returncode}; missing_features={','.join(missing)}"
        if allow_missing_executor:
            notes = f"executor_unavailable; {notes}"
        return _failed_result(request_row, program="crest", artifact_path=artifact_path, artifact_sha=artifact_sha, notes=notes)
    rows = [
        _result_row(
            request_row,
            canonical_feature_key=key,
            value_num=parsed.scalars[key],
            unit=PHASE2_QUANTUM_FEATURES[key]["unit"],
            program="crest",
            program_version=parsed.program_version,
            converged=1,
            imaginary_frequency_count=0,
            artifact_uri=artifact_path.as_posix(),
            artifact_sha256=artifact_sha,
            quality_level="calculated_open_source",
            notes=f"CREST phase-2 conformer summary; execution_kind={_clean(request_row.get('execution_kind')) or 'crest'}",
        )
        for key in PHASE2_CONFORMER_FEATURE_KEYS
    ]
    conformers = [
        _conformer_detail_row(request_row, conformer, artifact_path=artifact_path, artifact_sha=artifact_sha)
        for conformer in parsed.conformers
    ]
    return Phase2JobResult(request_id=request_id, status="succeeded", rows=rows, vibrational_modes=[], atomic_charges=[], conformers=conformers)


def _run_one_orca(
    request_row: dict[str, Any],
    xyz_path: Path | None,
    *,
    orca_bin: Path | None,
    artifact_dir: Path,
    nprocs_per_job: int,
    allow_missing_executor: bool,
) -> Phase2JobResult:
    request_id = _clean(request_row.get("request_id"))
    job_dir = artifact_dir / request_id
    job_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = job_dir / "orca.stdout"
    stderr_path = job_dir / "orca.stderr"
    returncode_path = job_dir / "orca.returncode"
    input_path = job_dir / "orca.inp"
    if xyz_path is None or not xyz_path.exists():
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text(f"XYZ input missing for request_id={request_id}: {xyz_path}\n", encoding="utf-8")
        returncode_path.write_text("127", encoding="utf-8")
    elif orca_bin is None:
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text("executor_unavailable: ORCA executable not found\n", encoding="utf-8")
        returncode_path.write_text("127", encoding="utf-8")
    else:
        local_xyz = job_dir / "input.xyz"
        shutil.copy2(xyz_path, local_xyz)
        _write_orca_input(input_path, local_xyz, request_row, nprocs_per_job=nprocs_per_job)
        env = _thread_env(nprocs_per_job)
        completed = subprocess.run(
            [str(orca_bin), str(input_path.name)],
            cwd=job_dir,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        stdout_path.write_text(completed.stdout, encoding="utf-8")
        stderr_path.write_text(completed.stderr, encoding="utf-8")
        returncode_path.write_text(str(completed.returncode), encoding="utf-8")
    artifact_path = _write_artifact_bundle(job_dir, request_id, "orca")
    return _orca_result(request_row, stdout_path, returncode_path, artifact_path, allow_missing_executor=allow_missing_executor)


def _orca_result(request_row: dict[str, Any], stdout_path: Path, returncode_path: Path, artifact_path: Path, *, allow_missing_executor: bool) -> Phase2JobResult:
    request_id = _clean(request_row.get("request_id"))
    artifact_sha = _sha256_file(artifact_path)
    returncode = _returncode(returncode_path)
    try:
        parsed = parse_orca_optfreq_output(stdout_path)
    except Exception as exc:
        return _failed_result(request_row, program="orca", artifact_path=artifact_path, artifact_sha=artifact_sha, notes=f"ORCA parse failed after returncode={returncode}: {exc}")
    missing = [key for key in PHASE2_ORCA_FEATURE_KEYS if key not in parsed.scalars]
    if returncode != 0 or missing or not parsed.normal_termination or not parsed.optimization_converged:
        notes = (
            "ORCA opt/freq failed or incomplete: "
            f"returncode={returncode}; normal_termination={int(parsed.normal_termination)}; "
            f"optimization_converged={int(parsed.optimization_converged)}; missing_features={','.join(missing)}"
        )
        if allow_missing_executor:
            notes = f"executor_unavailable; {notes}"
        return _failed_result(request_row, program="orca", artifact_path=artifact_path, artifact_sha=artifact_sha, notes=notes)
    rows = [
        _result_row(
            request_row,
            canonical_feature_key=key,
            value_num=parsed.scalars[key],
            unit=PHASE2_QUANTUM_FEATURES[key]["unit"],
            program="orca",
            program_version=parsed.program_version,
            converged=1,
            imaginary_frequency_count=parsed.imaginary_frequency_count,
            artifact_uri=artifact_path.as_posix(),
            artifact_sha256=artifact_sha,
            quality_level="computed_high",
            notes="ORCA B3LYP-D3BJ/def2-SVP Opt Freq phase-2 thermochemistry parsed from stdout/artifact bundle",
        )
        for key in PHASE2_ORCA_FEATURE_KEYS
    ]
    modes = [_mode_detail_row(request_row, mode, program="orca", artifact_path=artifact_path, artifact_sha=artifact_sha) for mode in parsed.modes]
    charges = [_charge_detail_row(request_row, charge, artifact_path=artifact_path, artifact_sha=artifact_sha) for charge in parsed.atomic_charges]
    return Phase2JobResult(request_id=request_id, status="succeeded", rows=rows, vibrational_modes=modes, atomic_charges=charges, conformers=[])


def _parse_thermochemistry_scalars(text: str) -> dict[str, float]:
    patterns = {
        "quantum.zpe": [
            r"zero\s+point\s+energy[^\n]*?([-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?)\s*(?:Eh|hartree)?",
            r"\bZPE\b[^\n]*?([-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?)\s*(?:Eh|hartree)?",
        ],
        "quantum.thermal_enthalpy_correction": [
            r"Thermal\s+correction\s+to\s+Enthalpy[^\n]*?([-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?)\s*(?:Eh|hartree)?",
            r"H\(T\)\s*-\s*H\(0\)[^\n]*?([-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?)\s*(?:Eh|hartree)?",
        ],
        "quantum.thermal_gibbs_correction": [
            r"Thermal\s+correction\s+to\s+Gibbs(?:\s+Free)?\s+Energy[^\n]*?([-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?)\s*(?:Eh|hartree)?",
            r"G\(RRHO\)\s+contrib\.[^\n]*?([-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?)\s*(?:Eh|hartree)?",
        ],
    }
    scalars: dict[str, float] = {}
    for key, key_patterns in patterns.items():
        for pattern in key_patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = _float_or_none(match.group(1))
                if value is not None:
                    scalars[key] = value
                    break
    return scalars


def _parse_mode_table(text: str) -> list[dict[str, Any]]:
    modes: list[dict[str, Any]] = []
    in_table = False
    for line in text.splitlines():
        lower = line.lower()
        if "frequency" in lower and ("mode" in lower or "ir" in lower or "cm" in lower):
            in_table = True
            continue
        if not in_table:
            continue
        match = re.match(
            r"^\s*(\d+)\s+([-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?)\s*(?:cm\S*)?\s*([-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?)?",
            line,
        )
        if not match:
            if line.strip() and not re.search(r"[-+]?\d", line):
                in_table = False
            continue
        modes.append(
            {
                "mode_index": int(match.group(1)),
                "frequency_cm_inv": float(match.group(2)),
                "ir_intensity_km_mol": _float_or_none(match.group(3)),
            }
        )
    return modes


def _parse_xtb_thermo_table(text: str) -> dict[str, float]:
    scalars: dict[str, float] = {}
    in_correction_table = False
    for line in text.splitlines():
        if "H(0)-H(T)+PV" in line and "H(T)/Eh" in line:
            in_correction_table = True
            continue
        if not in_correction_table:
            continue
        numbers = _numbers(line)
        if len(numbers) >= 5 and abs(numbers[0] - 298.15) < 1e-3:
            scalars["quantum.thermal_enthalpy_correction"] = float(numbers[2])
            scalars.setdefault("quantum.thermal_gibbs_correction", float(numbers[4]))
            break
        if line.strip() and not numbers and "-" not in line:
            in_correction_table = False
    return scalars


def _parse_xtb_vibspectrum(path: Path) -> list[dict[str, Any]]:
    modes: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip() or line.lstrip().startswith(("#", "$")):
            continue
        match = re.match(
            r"^\s*(\d+)\s+(?:(?![-+]?\d)\S+\s+)?([-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?)\s+([-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?)",
            line,
        )
        if not match:
            continue
        modes.append(
            {
                "mode_index": int(match.group(1)),
                "frequency_cm_inv": float(match.group(2)),
                "ir_intensity_km_mol": float(match.group(3)),
            }
        )
    return modes


def _parse_orca_modes(text: str) -> list[dict[str, Any]]:
    frequencies: dict[int, float] = {}
    for match in re.finditer(r"^\s*(\d+)\s*:\s*([-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?)\s+cm", text, flags=re.MULTILINE):
        frequencies[int(match.group(1))] = float(match.group(2))
    intensities: dict[int, float] = {}
    in_ir = False
    for line in text.splitlines():
        if "IR SPECTRUM" in line.upper():
            in_ir = True
            continue
        if not in_ir:
            continue
        if line.strip() == "" and intensities:
            break
        numbers = _numbers(line)
        if len(numbers) >= 4:
            mode = int(numbers[0])
            intensities[mode] = float(numbers[-1])
            frequencies.setdefault(mode, float(numbers[1]))
    return [
        {
            "mode_index": mode,
            "frequency_cm_inv": frequencies[mode],
            "ir_intensity_km_mol": intensities.get(mode),
        }
        for mode in sorted(frequencies)
    ]


def _parse_orca_atomic_charges(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    scheme: str | None = None
    for line in text.splitlines():
        upper = line.upper()
        if "MULLIKEN ATOMIC CHARGES" in upper:
            scheme = "mulliken"
            continue
        if "LOEWDIN ATOMIC CHARGES" in upper or "LÖWDIN ATOMIC CHARGES" in upper:
            scheme = "loewdin"
            continue
        if scheme is None:
            continue
        if "SUM OF ATOMIC CHARGES" in upper or "CARTESIAN" in upper or "MAYER" in upper:
            scheme = None
            continue
        match = re.match(r"^\s*(\d+)\s+([A-Za-z]{1,3})\s*:\s*([-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?)", line)
        if match:
            rows.append(
                {
                    "atom_index": int(match.group(1)),
                    "element": match.group(2),
                    "charge_scheme": scheme,
                    "partial_charge": float(match.group(3)),
                }
            )
    return rows


def _attach_molecule_phase2_fields(mapping: pd.DataFrame, molecule_core: pd.DataFrame) -> pd.DataFrame:
    molecule_core = _ensure_columns(molecule_core.copy().fillna("") if molecule_core is not None else pd.DataFrame(), ["mol_id", "heavy_atom_count"])
    info = molecule_core[["mol_id", "heavy_atom_count"]].drop_duplicates("mol_id")
    merged = pd.merge(mapping.copy().fillna(""), info, on="mol_id", how="left")
    merged["heavy_atom_count"] = pd.to_numeric(merged["heavy_atom_count"], errors="coerce").fillna(0).astype(int)
    return merged


def _attach_phase2_source_geometry(mapping: pd.DataFrame, xtb_geometry_by_mol: dict[str, Path]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for record in mapping.to_dict(orient="records"):
        row = dict(record)
        mol_id = _clean(row.get("mol_id"))
        mapped = _clean(row.get("mapping_status")) == "mapped" and bool(mol_id)
        source = xtb_geometry_by_mol.get(mol_id)
        if not mapped:
            blocking = _clean(row.get("mapping_reason")) or "unmapped"
        elif source is None or not Path(source).exists():
            blocking = "missing_completed_xtb_geometry"
        else:
            blocking = ""
        row["xtb_source_xyz_path"] = str(source) if source is not None else ""
        row["phase2_blocking_issue"] = blocking
        rows.append(row)
    return _ensure_columns(pd.DataFrame(rows), PHASE2_MAPPING_COLUMNS)


def _phase2_request_manifests(
    candidates: pd.DataFrame,
    *,
    xyz_dir: Path,
    program: str,
    task: str,
    method_family: str,
    theory_level: str,
    basis_set: str,
    completed_request_ids: set[str],
    executor_available: bool,
    execution_kind: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    request_rows: list[dict[str, Any]] = []
    xyz_rows: list[dict[str, Any]] = []
    for record in candidates.to_dict(orient="records"):
        mol_id = _clean(record.get("mol_id"))
        smiles = _clean(record.get("isomeric_smiles")) or _clean(record.get("canonical_smiles"))
        if not mol_id or not smiles:
            continue
        request_id = governance_phase2_request_id(mol_id, smiles, program=program, task=task)
        if request_id in completed_request_ids:
            continue
        source_xyz = Path(_clean(record.get("xtb_source_xyz_path")))
        if not source_xyz.exists():
            continue
        xyz_path = xyz_dir / f"{request_id}.xyz"
        xyz_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_xyz, xyz_path)
        status = "ready_for_executor" if executor_available else "pending_executor_unavailable"
        request_rows.append(
            {
                "request_id": request_id,
                "mol_id": mol_id,
                "canonical_smiles": _clean(record.get("canonical_smiles")) or smiles,
                "isomeric_smiles": smiles,
                "program": program,
                "method_family": method_family,
                "theory_level": theory_level,
                "basis_set": basis_set,
                "solvation_model": "gas_phase",
                "status": status,
                "recommended_next_action": "run_quantum",
                "notes": f"phase-2 governance {task} request; source_substance_id={_clean(record.get('substance_id'))}",
                "phase2_task": task,
                "source_request_id": "",
                "source_xyz_path": str(source_xyz),
                "heavy_atom_count": int(record.get("heavy_atom_count", 0) or 0),
                "execution_kind": execution_kind,
                "blocker_reason": "" if executor_available else f"{program}_executor_unavailable",
            }
        )
        xyz_rows.append(
            {
                "request_id": request_id,
                "mol_id": mol_id,
                "xyz_path": str(xyz_path),
                "xyz_status": "generated",
                "notes": f"XYZ copied from completed governance xTB optimized geometry: {source_xyz}",
            }
        )
    return _ensure_columns(pd.DataFrame(request_rows), PHASE2_REQUEST_COLUMNS), _ensure_columns(pd.DataFrame(xyz_rows), QUANTUM_XYZ_MANIFEST_COLUMNS)


def _crest_request_manifests(
    candidates: pd.DataFrame,
    *,
    xyz_dir: Path,
    crest_heavy_atom_min: int,
    completed_request_ids: set[str],
    executor_available: bool,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    request_rows: list[dict[str, Any]] = []
    xyz_rows: list[dict[str, Any]] = []
    for record in candidates.to_dict(orient="records"):
        mol_id = _clean(record.get("mol_id"))
        smiles = _clean(record.get("isomeric_smiles")) or _clean(record.get("canonical_smiles"))
        if not mol_id or not smiles:
            continue
        request_id = governance_phase2_request_id(mol_id, smiles, program="crest", task="conformer")
        if request_id in completed_request_ids:
            continue
        source_xyz = Path(_clean(record.get("xtb_source_xyz_path")))
        if not source_xyz.exists():
            continue
        heavy = int(record.get("heavy_atom_count", 0) or 0)
        execution_kind = "crest" if heavy >= int(crest_heavy_atom_min or 0) else "singleton"
        status = "ready_for_executor" if execution_kind == "singleton" or executor_available else "pending_executor_unavailable"
        xyz_path = xyz_dir / f"{request_id}.xyz"
        xyz_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_xyz, xyz_path)
        request_rows.append(
            {
                "request_id": request_id,
                "mol_id": mol_id,
                "canonical_smiles": _clean(record.get("canonical_smiles")) or smiles,
                "isomeric_smiles": smiles,
                "program": "crest",
                "method_family": "conformer_search",
                "theory_level": "CREST-GFN2-xTB",
                "basis_set": "",
                "solvation_model": "gas_phase",
                "status": status,
                "recommended_next_action": "run_quantum",
                "notes": f"phase-2 governance conformer request; source_substance_id={_clean(record.get('substance_id'))}",
                "phase2_task": "conformer",
                "source_request_id": "",
                "source_xyz_path": str(source_xyz),
                "heavy_atom_count": heavy,
                "execution_kind": execution_kind,
                "blocker_reason": "" if status == "ready_for_executor" else "crest_executor_unavailable",
            }
        )
        xyz_rows.append(
            {
                "request_id": request_id,
                "mol_id": mol_id,
                "xyz_path": str(xyz_path),
                "xyz_status": "generated",
                "notes": f"XYZ copied from completed governance xTB optimized geometry: {source_xyz}",
            }
        )
    return _ensure_columns(pd.DataFrame(request_rows), PHASE2_REQUEST_COLUMNS), _ensure_columns(pd.DataFrame(xyz_rows), QUANTUM_XYZ_MANIFEST_COLUMNS)


def _phase2_blockers(mapping_report: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for record in mapping_report.to_dict(orient="records"):
        if _clean(record.get("mapping_status")) != "mapped":
            rows.append(
                _blocker_row(
                    record,
                    target_output="phase2_quantum_compute",
                    blocker_type="unmapped_governance_substance",
                    blocker_reason=_clean(record.get("mapping_reason")) or "unmapped",
                    recommended_next_action="curate_governance_mapping",
                )
            )
            continue
        if _clean(record.get("phase2_blocking_issue")):
            rows.append(
                _blocker_row(
                    record,
                    target_output="phase2_quantum_compute",
                    blocker_type="missing_prerequisite_artifact",
                    blocker_reason=_clean(record.get("phase2_blocking_issue")),
                    recommended_next_action="rerun_or_recover_xtb_geometry",
                )
            )
        for target in ("nbo_resp_charges", "standard_enthalpy_of_formation"):
            rows.append(
                _blocker_row(
                    record,
                    target_output=target,
                    blocker_type="postprocessor_unavailable",
                    blocker_reason=(
                        "NBO/RESP/formation-enthalpy postprocessor or reference scheme is not configured; "
                        "do not fabricate derived charges or formation enthalpy"
                    ),
                    recommended_next_action="implement_and_verify_postprocessor_before_population",
                )
            )
    return _ensure_columns(pd.DataFrame(rows), PHASE2_BLOCKER_COLUMNS)


def _blocker_row(record: dict[str, Any], *, target_output: str, blocker_type: str, blocker_reason: str, recommended_next_action: str) -> dict[str, Any]:
    return {
        "substance_id": _clean(record.get("substance_id")),
        "refrigerant_number": _clean(record.get("refrigerant_number")),
        "mol_id": _clean(record.get("mol_id")),
        "target_output": target_output,
        "blocker_type": blocker_type,
        "blocker_reason": blocker_reason,
        "recommended_next_action": recommended_next_action,
        "artifact_uri": "",
        "notes": "phase-2 explicit blocker; no synthetic value emitted",
    }


def _write_orca_batches(orca_full: pd.DataFrame, orca_xyz: pd.DataFrame, *, output_dir: Path, batch_size: int) -> tuple[list[str], list[str]]:
    request_files: list[str] = []
    xyz_files: list[str] = []
    batch_size = max(1, int(batch_size or 1))
    if orca_full.empty:
        request_path = output_dir / "governance_phase2_orca_optfreq_batch001.csv"
        xyz_path = output_dir / "governance_phase2_orca_optfreq_xyz_manifest_batch001.csv"
        _write_manifest(orca_full, request_path, PHASE2_REQUEST_COLUMNS)
        _write_manifest(orca_xyz, xyz_path, QUANTUM_XYZ_MANIFEST_COLUMNS)
        return [str(request_path)], [str(xyz_path)]
    for index, start in enumerate(range(0, len(orca_full), batch_size), start=1):
        batch = orca_full.iloc[start : start + batch_size].copy()
        xyz_batch = _xyz_subset(orca_xyz, batch)
        request_path = output_dir / f"governance_phase2_orca_optfreq_batch{index:03d}.csv"
        xyz_path = output_dir / f"governance_phase2_orca_optfreq_xyz_manifest_batch{index:03d}.csv"
        _write_manifest(batch, request_path, PHASE2_REQUEST_COLUMNS)
        _write_manifest(xyz_batch, xyz_path, QUANTUM_XYZ_MANIFEST_COLUMNS)
        request_files.append(str(request_path))
        xyz_files.append(str(xyz_path))
    return request_files, xyz_files


def _xyz_subset(xyz_manifest: pd.DataFrame, request_manifest: pd.DataFrame) -> pd.DataFrame:
    request_ids = request_manifest.get("request_id", pd.Series(dtype="object")).astype(str).tolist() if not request_manifest.empty else []
    subset = xyz_manifest.loc[xyz_manifest.get("request_id", pd.Series(dtype="object")).astype(str).isin(request_ids)].copy() if not xyz_manifest.empty else pd.DataFrame(columns=QUANTUM_XYZ_MANIFEST_COLUMNS)
    if request_ids and not subset.empty:
        order = {request_id: idx for idx, request_id in enumerate(request_ids)}
        subset["_request_order"] = subset["request_id"].astype(str).map(order)
        subset = subset.sort_values("_request_order", kind="stable").drop(columns=["_request_order"])
    return _ensure_columns(subset, QUANTUM_XYZ_MANIFEST_COLUMNS)


def _write_manifest(frame: pd.DataFrame, path: Path, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _ensure_columns(frame.copy(), columns).to_csv(path, index=False)


def _completed_phase2_request_ids(rows: pd.DataFrame, *, program: str, feature_keys: tuple[str, ...]) -> set[str]:
    if rows is None or rows.empty:
        return set()
    working = _ensure_columns(rows.copy().fillna(""), QUANTUM_INPUT_COLUMNS)
    working = working.loc[
        working["program"].astype(str).str.lower().eq(program.lower())
        & working["canonical_feature_key"].astype(str).isin(feature_keys)
        & working["converged"].map(_truthy)
        & pd.to_numeric(working["value_num"], errors="coerce").notna()
    ].copy()
    if working.empty:
        return set()
    counts = working.groupby("request_id")["canonical_feature_key"].nunique()
    return {_clean(request_id) for request_id, count in counts.items() if int(count) >= len(feature_keys) and _clean(request_id)}


def _failed_phase2_request_ids(rows: pd.DataFrame, *, program: str, feature_keys: tuple[str, ...]) -> set[str]:
    if rows is None or rows.empty:
        return set()
    working = _ensure_columns(rows.copy().fillna(""), QUANTUM_INPUT_COLUMNS)
    program_rows = working.loc[working["program"].astype(str).str.lower().eq(program.lower())].copy()
    if program_rows.empty:
        return set()
    completed = _completed_phase2_request_ids(program_rows, program=program, feature_keys=feature_keys)
    feature_keys_series = program_rows["canonical_feature_key"].astype(str).str.strip()
    values = pd.to_numeric(program_rows["value_num"], errors="coerce")
    failure_mask = feature_keys_series.eq("") | ~program_rows["converged"].map(_truthy) | values.isna()
    return {_clean(value) for value in program_rows.loc[failure_mask, "request_id"].tolist() if _clean(value)} - completed


def _result_row(
    request_row: dict[str, Any],
    *,
    canonical_feature_key: str,
    value_num: float | str,
    unit: str,
    program: str,
    program_version: str,
    converged: int,
    imaginary_frequency_count: int,
    artifact_uri: str,
    artifact_sha256: str,
    quality_level: str,
    notes: str,
) -> dict[str, Any]:
    return {
        "request_id": _clean(request_row.get("request_id")),
        "mol_id": _clean(request_row.get("mol_id")),
        "canonical_feature_key": canonical_feature_key,
        "value_num": value_num,
        "unit": unit,
        "program": program,
        "program_version": program_version,
        "method_family": _clean(request_row.get("method_family")) or ("DFT" if program == "orca" else "semiempirical"),
        "theory_level": _clean(request_row.get("theory_level")),
        "basis_set": _clean(request_row.get("basis_set")),
        "solvation_model": _clean(request_row.get("solvation_model")) or "gas_phase",
        "converged": int(converged),
        "imaginary_frequency_count": int(imaginary_frequency_count),
        "artifact_uri": artifact_uri,
        "artifact_sha256": artifact_sha256,
        "quality_level": quality_level,
        "notes": notes,
    }


def _failed_result(request_row: dict[str, Any], *, program: str, artifact_path: Path, artifact_sha: str, notes: str) -> Phase2JobResult:
    row = _result_row(
        request_row,
        canonical_feature_key="",
        value_num="",
        unit="",
        program=program,
        program_version="",
        converged=0,
        imaginary_frequency_count=0,
        artifact_uri=artifact_path.as_posix(),
        artifact_sha256=artifact_sha,
        quality_level="computed_high" if program == "orca" else "calculated_open_source",
        notes=notes,
    )
    return Phase2JobResult(
        request_id=_clean(request_row.get("request_id")),
        status="failed",
        rows=[row],
        vibrational_modes=[],
        atomic_charges=[],
        conformers=[],
    )


def _mode_detail_row(request_row: dict[str, Any], mode: dict[str, Any], *, program: str, artifact_path: Path, artifact_sha: str) -> dict[str, Any]:
    return {
        "request_id": _clean(request_row.get("request_id")),
        "mol_id": _clean(request_row.get("mol_id")),
        "program": program,
        "theory_level": _clean(request_row.get("theory_level")),
        "mode_index": int(mode.get("mode_index", 0) or 0),
        "frequency_cm_inv": mode.get("frequency_cm_inv"),
        "ir_intensity_km_mol": mode.get("ir_intensity_km_mol"),
        "artifact_uri": artifact_path.as_posix(),
        "artifact_sha256": artifact_sha,
        "notes": "phase-2 vibrational mode detail parsed from executor output",
    }


def _charge_detail_row(request_row: dict[str, Any], charge: dict[str, Any], *, artifact_path: Path, artifact_sha: str) -> dict[str, Any]:
    return {
        "request_id": _clean(request_row.get("request_id")),
        "mol_id": _clean(request_row.get("mol_id")),
        "program": "orca",
        "theory_level": _clean(request_row.get("theory_level")),
        "atom_index": int(charge.get("atom_index", 0) or 0),
        "element": _clean(charge.get("element")),
        "charge_scheme": _clean(charge.get("charge_scheme")),
        "partial_charge": charge.get("partial_charge"),
        "artifact_uri": artifact_path.as_posix(),
        "artifact_sha256": artifact_sha,
        "notes": "available ORCA atomic charge detail; not NBO/RESP",
    }


def _conformer_detail_row(request_row: dict[str, Any], conformer: dict[str, Any], *, artifact_path: Path, artifact_sha: str) -> dict[str, Any]:
    return {
        "request_id": _clean(request_row.get("request_id")),
        "mol_id": _clean(request_row.get("mol_id")),
        "conformer_index": int(conformer.get("conformer_index", 0) or 0),
        "relative_energy_kcal_mol": conformer.get("relative_energy_kcal_mol"),
        "relative_energy_kj_mol": conformer.get("relative_energy_kj_mol"),
        "boltzmann_weight": conformer.get("boltzmann_weight"),
        "source_xyz": _clean(request_row.get("source_xyz_path")),
        "artifact_uri": artifact_path.as_posix(),
        "artifact_sha256": artifact_sha,
        "notes": f"phase-2 conformer detail; execution_kind={_clean(request_row.get('execution_kind'))}",
    }


def _write_orca_input(input_path: Path, xyz_path: Path, request_row: dict[str, Any], *, nprocs_per_job: int) -> None:
    charge, multiplicity = _charge_multiplicity_from_request(request_row)
    theory = _clean(request_row.get("theory_level")) or "B3LYP-D3BJ"
    basis = _clean(request_row.get("basis_set")) or "def2-SVP"
    method = theory.replace("-D3BJ", " D3BJ").replace("D3(BJ)", "D3BJ")
    atoms = []
    lines = xyz_path.read_text(encoding="utf-8", errors="replace").splitlines()
    for line in lines[2:]:
        parts = line.split()
        if len(parts) >= 4:
            atoms.append(" ".join(parts[:4]))
    input_path.write_text(
        "\n".join(
            [
                f"! {method} {basis} Opt Freq TightSCF",
                f"%pal nprocs {max(1, int(nprocs_per_job or 1))} end",
                "",
                f"* xyz {charge} {multiplicity}",
                *atoms,
                "*",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _write_artifact_bundle(job_dir: Path, request_id: str, suffix: str) -> Path:
    _write_sha256_manifest(job_dir, suffix=suffix)
    artifact_path = job_dir / f"{request_id}_{suffix}_artifact.tar.gz"
    with tarfile.open(artifact_path, "w:gz") as archive:
        for path in sorted(job_dir.rglob("*")):
            if not path.is_file() or path == artifact_path or path.name.endswith("_artifact.tar.gz"):
                continue
            archive.add(path, arcname=path.relative_to(job_dir).as_posix())
    return artifact_path


def _write_sha256_manifest(job_dir: Path, *, suffix: str) -> Path:
    manifest_path = job_dir / "manifest.sha256"
    lines = []
    for path in sorted(job_dir.rglob("*")):
        if not path.is_file() or path == manifest_path or path.name.endswith("_artifact.tar.gz"):
            continue
        lines.append(f"{_sha256_file(path)}  {path.relative_to(job_dir).as_posix()}")
    manifest_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return manifest_path


def _merge_detail_rows(path: Path, rows: list[dict[str, Any]], columns: list[str], dedupe_columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = pd.read_csv(path).fillna("") if path.exists() else pd.DataFrame(columns=columns)
    incoming = pd.DataFrame(rows, columns=columns)
    if existing.empty and incoming.empty:
        _ensure_columns(pd.DataFrame(), columns).to_csv(path, index=False)
        return
    merged = pd.concat([_ensure_columns(existing, columns), _ensure_columns(incoming, columns)], ignore_index=True)
    if not merged.empty:
        merged["_merge_order"] = range(len(merged))
        merged = merged.sort_values("_merge_order", kind="stable").drop_duplicates(subset=dedupe_columns, keep="last")
        merged = merged.sort_values("_merge_order", kind="stable").drop(columns=["_merge_order"])
    _ensure_columns(merged, columns).to_csv(path, index=False)


def _tool_available(name: str, override: bool | None, *, fallback_path: Path | None = None) -> bool:
    if override is not None:
        return bool(override)
    env_var = f"R_PHYSGEN_{name.upper()}_BIN"
    return _resolve_bin(None, env_var=env_var, default_name=name, fallback_path=fallback_path) is not None


def _orca_available(override: bool | None) -> bool:
    if override is not None:
        return bool(override)
    return _resolve_orca_bin(None) is not None


def _resolve_orca_bin(orca_bin: Path | str | None) -> Path | None:
    path = _resolve_bin(orca_bin, env_var="R_PHYSGEN_ORCA_BIN", default_name="orca")
    if path is None:
        return None
    if _is_known_non_quantum_orca(path):
        return None
    return path


def _is_known_non_quantum_orca(path: Path) -> bool:
    """Reject the GNOME screen-reader binary that is commonly installed as /usr/bin/orca."""

    try:
        header = path.read_bytes()[:8192].decode("utf-8", errors="ignore").lower()
    except Exception:
        header = ""
    if "orca team" in header or "gi.repository" in header or "atspi" in header:
        return True
    try:
        completed = subprocess.run(
            [str(path), "--version"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5,
            check=False,
        )
    except Exception:
        return False
    output = f"{completed.stdout}\n{completed.stderr}".lower()
    return "screen reader" in output or "cannot connect to the desktop" in output


def _resolve_bin(bin_path: Path | str | None, *, env_var: str, default_name: str, fallback_path: Path | None = None) -> Path | None:
    if bin_path is not None:
        path = Path(bin_path)
        if path.exists():
            return path
        resolved = shutil.which(str(bin_path))
        return Path(resolved) if resolved else None
    env_value = os.getenv(env_var, "").strip()
    if env_value:
        return _resolve_bin(env_value, env_var=env_var, default_name=default_name, fallback_path=fallback_path)
    resolved = shutil.which(default_name)
    if resolved:
        return Path(resolved)
    if fallback_path is not None and fallback_path.exists():
        return fallback_path
    return None


def _thread_env(threads: int) -> dict[str, str]:
    env = os.environ.copy()
    value = str(max(1, int(threads or 1)))
    env["OMP_NUM_THREADS"] = value
    env["MKL_NUM_THREADS"] = value
    env["OPENBLAS_NUM_THREADS"] = value
    return env


def _fill_boltzmann_weights(conformers: list[dict[str, Any]]) -> None:
    beta = 1.0 / (0.00198720425864083 * 298.15)
    weights = [math.exp(-float(row["relative_energy_kcal_mol"]) * beta) for row in conformers]
    total = sum(weights) or 1.0
    for row, weight in zip(conformers, weights, strict=False):
        row["boltzmann_weight"] = weight / total


def _lowest_real_frequency(modes: list[dict[str, Any]]) -> float | None:
    values = [float(value) for value in (mode.get("frequency_cm_inv") for mode in modes) if _float_or_none(value) is not None and float(value) > 0]
    return min(values) if values else None


def _numbers(text: str) -> list[float]:
    return [float(match) for match in re.findall(r"[-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?", text)]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _returncode(path: Path) -> int:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except Exception:
        return 1


def _ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    columns = list(columns)
    for column in columns:
        if column not in df.columns:
            df[column] = ""
    return df[columns] if columns else df


def _clean(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()


def _truthy(value: Any) -> bool:
    return _clean(value).lower() in {"1", "true", "yes", "y", "converged", "succeeded", "success"}


def _float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed):
        return None
    return parsed
