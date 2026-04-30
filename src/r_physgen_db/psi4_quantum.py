"""Psi4/DFT executor and parser helpers for the quantum pilot."""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import subprocess
import tarfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from r_physgen_db.quantum_pilot import (
    PSI4_DFT_BASIS_SET,
    PSI4_DFT_THEORY_LEVEL,
    QUANTUM_FEATURES,
    QUANTUM_INPUT_COLUMNS,
    merge_quantum_result_rows,
)


PSI4_FEATURE_UNITS = {key: value["unit"] for key, value in QUANTUM_FEATURES.items()}
HARTREE_TO_EV = 27.211386245988
AU_DIPOLE_TO_DEBYE = 2.541746473


@dataclass(slots=True)
class Psi4ScalarFeatures:
    values: dict[str, float]
    program_version: str
    theory_level: str
    basis_set: str


@dataclass(slots=True)
class Psi4JobResult:
    request_id: str
    mol_id: str
    status: str
    rows: list[dict[str, Any]]
    notes: str


def parse_psi4_scalar_features(result_path: Path) -> Psi4ScalarFeatures:
    """Parse scalar DFT features from the JSON sidecar written by the Psi4 input."""

    data = json.loads(result_path.read_text(encoding="utf-8"))
    values_raw = data.get("values", data)
    variables_raw = data.get("variables", {})
    variables = variables_raw if isinstance(variables_raw, dict) else {}
    wavefunction_raw = data.get("wavefunction", {})
    wavefunction = wavefunction_raw if isinstance(wavefunction_raw, dict) else {}
    values: dict[str, float] = {}
    total_energy = _float_or_none(_first_present(values_raw, "total_energy_eh", "total_energy", "CURRENT ENERGY"))
    if total_energy is not None:
        values["quantum.total_energy"] = total_energy
    homo = _float_or_none(_first_present(values_raw, "homo_ev", "homo_energy_ev", "HOMO_EV"))
    lumo = _float_or_none(_first_present(values_raw, "lumo_ev", "lumo_energy_ev", "LUMO_EV"))
    if homo is None or lumo is None:
        wfn_homo, wfn_lumo = _homo_lumo_from_wavefunction(wavefunction)
        homo = homo if homo is not None else wfn_homo
        lumo = lumo if lumo is not None else wfn_lumo
    if homo is not None:
        values["quantum.homo_energy"] = homo
    if lumo is not None:
        values["quantum.lumo_energy"] = lumo
    gap = _float_or_none(_first_present(values_raw, "gap_ev", "homo_lumo_gap_ev", "HOMO_LUMO_GAP_EV"))
    if gap is None and homo is not None and lumo is not None:
        gap = lumo - homo
    if gap is not None:
        values["quantum.homo_lumo_gap"] = gap
    dipole = _float_or_none(_first_present(values_raw, "dipole_moment_debye", "dipole_debye", "DIPOLE_DEBYE"))
    if dipole is None:
        dipole = _dipole_debye_from_variables(variables)
    if dipole is not None:
        values["quantum.dipole_moment"] = dipole
    polarizability = _float_or_none(_first_present(values_raw, "polarizability_au", "POLARIZABILITY_AU"))
    if polarizability is None:
        polarizability = _polarizability_au_from_variables(variables)
    if polarizability is not None:
        values["quantum.polarizability"] = polarizability
    return Psi4ScalarFeatures(
        values={key: values[key] for key in QUANTUM_FEATURES if key in values},
        program_version=str(data.get("program_version", "")),
        theory_level=str(data.get("theory_level", PSI4_DFT_THEORY_LEVEL) or PSI4_DFT_THEORY_LEVEL),
        basis_set=str(data.get("basis_set", PSI4_DFT_BASIS_SET) or PSI4_DFT_BASIS_SET),
    )


def run_psi4_quantum_pilot(
    *,
    requests_path: Path,
    xyz_manifest_path: Path,
    output_path: Path,
    artifact_dir: Path,
    psi4_bin: Path | str | None,
    limit: int | None = None,
    resume: bool = True,
    scratch_dir: Path | None = None,
    allow_missing_executor: bool = False,
    jobs: int = 1,
    retry_failed_only: bool = False,
    completion_required: bool = False,
) -> dict[str, int]:
    """Run Psi4 DFT jobs and merge rows into the shared quantum result CSV."""

    requests_all = pd.read_csv(requests_path).fillna("")
    manifest_request_ids = {
        str(request_id).strip()
        for request_id in requests_all.get("request_id", pd.Series(dtype=str)).tolist()
        if str(request_id).strip()
    }
    requests = requests_all
    xyz_manifest = pd.read_csv(xyz_manifest_path).fillna("")
    if limit is not None:
        requests = requests.head(int(limit)).copy()

    xyz_by_request = {
        str(row["request_id"]): Path(str(row["xyz_path"]))
        for row in xyz_manifest.to_dict(orient="records")
        if str(row.get("request_id", "")).strip()
    }
    existing = pd.read_csv(output_path).fillna("") if output_path.exists() else pd.DataFrame(columns=QUANTUM_INPUT_COLUMNS)
    pruned_stale_rows = 0
    if completion_required and manifest_request_ids and not existing.empty:
        psi4_request_ids = existing["request_id"].astype(str).str.strip()
        feature_keys = existing.get("canonical_feature_key", pd.Series("", index=existing.index)).astype(str).str.strip()
        converged_values = existing.get("converged", pd.Series("", index=existing.index)).astype(str).str.strip().str.lower()
        not_converged = ~converged_values.isin({"1", "true", "yes", "y", "succeeded", "success"})
        stale_psi4_mask = (
            existing["program"].astype(str).str.lower().eq("psi4")
            & ~psi4_request_ids.isin(manifest_request_ids)
            & feature_keys.eq("")
            & not_converged
        )
        pruned_stale_rows = int(stale_psi4_mask.sum())
        if pruned_stale_rows:
            existing = existing.loc[~stale_psi4_mask].reset_index(drop=True)
    completed = _completed_psi4_request_ids(existing) if resume else set()
    failed_targets = _failed_psi4_request_ids(existing) if retry_failed_only else set()

    tasks = []
    for row in requests.to_dict(orient="records"):
        request_id = str(row.get("request_id", "")).strip()
        if not request_id:
            continue
        if retry_failed_only:
            if request_id not in failed_targets:
                continue
        elif request_id in completed:
            continue
        tasks.append((row, xyz_by_request.get(request_id)))

    artifact_dir.mkdir(parents=True, exist_ok=True)
    scratch_dir = scratch_dir or artifact_dir / "_psi4_scratch"
    scratch_dir.mkdir(parents=True, exist_ok=True)
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
        "pruned_stale_rows": pruned_stale_rows,
    }
    rows: list[dict[str, Any]] = []

    resolved_psi4 = _resolve_psi4_bin(psi4_bin)
    if resolved_psi4 is None and not allow_missing_executor:
        raise FileNotFoundError("Psi4 executable not found; set R_PHYSGEN_PSI4_BIN, pass psi4_bin, or allow_missing_executor=True")

    workers = max(1, int(jobs or 1))
    if workers == 1:
        results = [
            _run_one_psi4_job(
                row,
                xyz_path,
                psi4_bin=resolved_psi4,
                artifact_dir=artifact_dir,
                scratch_dir=scratch_dir,
                allow_missing_executor=allow_missing_executor,
                use_attempt_dir=retry_failed_only,
            )
            for row, xyz_path in tasks
        ]
    else:
        results = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(
                    _run_one_psi4_job,
                    row,
                    xyz_path,
                    psi4_bin=resolved_psi4,
                    artifact_dir=artifact_dir,
                    scratch_dir=scratch_dir,
                    allow_missing_executor=allow_missing_executor,
                    use_attempt_dir=retry_failed_only,
                )
                for row, xyz_path in tasks
            ]
            for future in as_completed(futures):
                results.append(future.result())

    for result in sorted(results, key=lambda item: item.request_id):
        rows.extend(result.rows)
        if result.status == "succeeded":
            summary["succeeded"] += 1
        else:
            summary["failed"] += 1

    merged = merge_quantum_result_rows(existing, pd.DataFrame(rows, columns=QUANTUM_INPUT_COLUMNS))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_path, index=False)
    summary["rows_written"] = len(rows)
    summary["merged_rows"] = len(merged)
    if completion_required:
        target_request_ids = {str(row[0].get("request_id", "")).strip() for row in tasks if str(row[0].get("request_id", "")).strip()}
        if retry_failed_only:
            incomplete = sorted(_failed_psi4_request_ids(merged) & target_request_ids)
        else:
            completed_after = _completed_psi4_request_ids(merged)
            incomplete = sorted(target_request_ids - completed_after)
        if incomplete:
            sample = ", ".join(incomplete[:10])
            raise RuntimeError(f"Psi4 completion required but {len(incomplete)} request(s) remain incomplete: {sample}")
    return summary


def _run_one_psi4_job(
    request_row: dict[str, Any],
    xyz_path: Path | None,
    *,
    psi4_bin: Path | None,
    artifact_dir: Path,
    scratch_dir: Path,
    allow_missing_executor: bool,
    use_attempt_dir: bool = False,
) -> Psi4JobResult:
    request_id = str(request_row.get("request_id", "")).strip()
    mol_id = str(request_row.get("mol_id", "")).strip()
    job_root = artifact_dir / request_id
    if use_attempt_dir:
        job_dir = job_root / f"attempt_{_next_attempt_index(job_root):02d}_psi4_dft_sp"
    else:
        job_dir = job_root
    job_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = job_dir / "psi4.stdout"
    stderr_path = job_dir / "psi4.stderr"
    returncode_path = job_dir / "psi4.returncode"
    result_path = job_dir / "psi4_result.json"
    input_path = job_dir / "psi4_input.py"

    if xyz_path is None or not xyz_path.exists():
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text(f"XYZ input unavailable for request_id={request_id}\n", encoding="utf-8")
        returncode_path.write_text("127", encoding="utf-8")
    elif psi4_bin is None:
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text("executor_unavailable: Psi4 executable unavailable\n", encoding="utf-8")
        returncode_path.write_text("127", encoding="utf-8")
    else:
        local_xyz = job_dir / "input.xyz"
        shutil.copy2(xyz_path, local_xyz)
        _write_psi4_input(input_path, local_xyz, request_row)
        env = os.environ.copy()
        env["PSI_SCRATCH"] = str(scratch_dir.resolve())
        env["PATH"] = f"{psi4_bin.parent}:{env.get('PATH', '')}"
        completed = subprocess.run(
            [str(psi4_bin), str(input_path.name)],
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

    _write_sha256_manifest(job_dir)
    artifact_path = _write_artifact_bundle(job_root, request_id, xyz_path)
    artifact_sha256 = _sha256_file(artifact_path)
    artifact_uri = artifact_path.as_posix()
    returncode = _returncode(returncode_path)
    try:
        parsed = parse_psi4_scalar_features(result_path)
    except Exception as exc:
        notes = f"Psi4 parse failed after returncode={returncode}: {exc}"
        if psi4_bin is None and allow_missing_executor:
            notes = f"executor_unavailable; {notes}"
        return Psi4JobResult(
            request_id=request_id,
            mol_id=mol_id,
            status="failed",
            rows=[
                _result_row(
                    request_row,
                    canonical_feature_key="",
                    value_num="",
                    unit="",
                    converged=0,
                    program_version="",
                    artifact_uri=artifact_uri,
                    artifact_sha256=artifact_sha256,
                    notes=notes,
                )
            ],
            notes=notes,
        )

    missing = sorted(set(QUANTUM_FEATURES) - set(parsed.values))
    if returncode != 0 or missing:
        notes = f"Psi4 failed or incomplete: returncode={returncode}; missing_features={','.join(missing)}"
        return Psi4JobResult(
            request_id=request_id,
            mol_id=mol_id,
            status="failed",
            rows=[
                _result_row(
                    request_row,
                    canonical_feature_key="",
                    value_num="",
                    unit="",
                    converged=0,
                    program_version=parsed.program_version,
                    artifact_uri=artifact_uri,
                    artifact_sha256=artifact_sha256,
                    notes=notes,
                )
            ],
            notes=notes,
        )

    rows = [
        _result_row(
            request_row,
            canonical_feature_key=feature_key,
            value_num=value,
            unit=PSI4_FEATURE_UNITS[feature_key],
            converged=1,
            program_version=parsed.program_version,
            artifact_uri=artifact_uri,
            artifact_sha256=artifact_sha256,
            notes=(
                "Psi4 DFT scalar parsed from psi4_result.json; artifact retains input/output/scratch manifest"
                + ("; retry_profile=psi4_dft_sp" if use_attempt_dir else "")
            ),
        )
        for feature_key, value in parsed.values.items()
    ]
    return Psi4JobResult(request_id=request_id, mol_id=mol_id, status="succeeded", rows=rows, notes="succeeded")


def _write_psi4_input(input_path: Path, xyz_path: Path, request_row: dict[str, Any]) -> None:
    charge, multiplicity = _charge_multiplicity_from_request(request_row)
    geometry = _xyz_to_psi4_geometry(xyz_path, charge=charge, multiplicity=multiplicity)
    reference = "uhf" if multiplicity != 1 else "rhf"
    theory = str(request_row.get("theory_level", PSI4_DFT_THEORY_LEVEL) or PSI4_DFT_THEORY_LEVEL)
    basis = str(request_row.get("basis_set", PSI4_DFT_BASIS_SET) or PSI4_DFT_BASIS_SET)
    input_path.write_text(
        f'''from __future__ import annotations
import json
import math
import psi4

HARTREE_TO_EV = {HARTREE_TO_EV!r}
AU_DIPOLE_TO_DEBYE = {AU_DIPOLE_TO_DEBYE!r}

psi4.set_memory("2 GB")
psi4.set_num_threads(1)
psi4.set_options({{"basis": {basis!r}, "scf_type": "df", "reference": {reference!r}, "guess": "sad", "maxiter": 300}})
mol = psi4.geometry({geometry!r})
property_error = ""
try:
    energy, wavefunction = psi4.properties({theory!r}, properties=["DIPOLE", "DIPOLE_POLARIZABILITIES"], molecule=mol, return_wfn=True)
except Exception as exc:
    property_error = str(exc)
    energy, wavefunction = psi4.energy({theory!r}, molecule=mol, return_wfn=True)
    try:
        psi4.oeprop(wavefunction, "DIPOLE", title="SCF")
    except Exception as dipole_exc:
        property_error = property_error + "; dipole_error=" + str(dipole_exc)

def serialize(value):
    try:
        return float(value)
    except Exception:
        pass
    for attr in ("to_array",):
        try:
            array = getattr(value, attr)()
            return [float(item) for item in array.ravel().tolist()]
        except Exception:
            pass
    try:
        array = value.np
        return [float(item) for item in array.ravel().tolist()]
    except Exception:
        pass
    try:
        return [float(item) for item in value]
    except Exception:
        return None

def serializable_variables():
    output = {{}}
    for key, value in psi4.core.variables().items():
        converted = serialize(value)
        if isinstance(converted, float) and math.isfinite(converted):
            output[str(key)] = converted
        elif isinstance(converted, list) and converted and all(math.isfinite(item) for item in converted):
            output[str(key)] = converted
    return output

def orbital_payload(wfn):
    payload = {{}}
    try:
        raw_epsilon_a = wfn.epsilon_a().to_array()
        try:
            epsilon_items = raw_epsilon_a.ravel().tolist()
        except Exception:
            epsilon_items = []
            for block in raw_epsilon_a:
                try:
                    epsilon_items.extend(block.ravel().tolist())
                except Exception:
                    try:
                        epsilon_items.extend(list(block))
                    except Exception:
                        epsilon_items.append(block)
        epsilon_a = [float(value) for value in epsilon_items]
        payload["epsilon_a_hartree"] = epsilon_a
        payload["nalpha"] = int(wfn.nalpha())
        if payload["nalpha"] > 0 and len(epsilon_a) > payload["nalpha"]:
            payload["homo_ev"] = epsilon_a[payload["nalpha"] - 1] * HARTREE_TO_EV
            payload["lumo_ev"] = epsilon_a[payload["nalpha"]] * HARTREE_TO_EV
    except Exception as exc:
        payload["orbital_error"] = str(exc)
    return payload

variables = serializable_variables()
wavefunction_payload = orbital_payload(wavefunction)
dipole_debye = None
dipole_components = [variables.get(name) for name in ("CURRENT DIPOLE X", "CURRENT DIPOLE Y", "CURRENT DIPOLE Z")]
if all(component is not None for component in dipole_components):
    dipole_debye = math.sqrt(sum(float(component) ** 2 for component in dipole_components)) * AU_DIPOLE_TO_DEBYE
if dipole_debye is None:
    for name in ("CURRENT DIPOLE", "SCF DIPOLE", "B3LYP DIPOLE"):
        vector = variables.get(name)
        if isinstance(vector, list) and len(vector) >= 3:
            dipole_debye = math.sqrt(sum(float(component) ** 2 for component in vector[:3])) * AU_DIPOLE_TO_DEBYE
            break
polarizability_au = variables.get("CURRENT POLARIZABILITY")
if polarizability_au is None:
    tensor_diag = [
        variables.get(name)
        for name in (
            "CURRENT POLARIZABILITY XX",
            "CURRENT POLARIZABILITY YY",
            "CURRENT POLARIZABILITY ZZ",
        )
    ]
    if not all(component is not None for component in tensor_diag):
        tensor_diag = [
            variables.get(name)
            for name in (
                "DIPOLE POLARIZABILITY XX",
                "DIPOLE POLARIZABILITY YY",
                "DIPOLE POLARIZABILITY ZZ",
            )
        ]
    if all(component is not None for component in tensor_diag):
        polarizability_au = sum(float(component) for component in tensor_diag) / 3.0
result = {{
    "program_version": psi4.__version__,
    "theory_level": {theory!r},
    "basis_set": {basis!r},
    "variables": variables,
    "wavefunction": wavefunction_payload,
    "property_error": property_error,
    "values": {{
        "total_energy_eh": float(energy),
        "homo_ev": wavefunction_payload.get("homo_ev"),
        "lumo_ev": wavefunction_payload.get("lumo_ev"),
        "dipole_moment_debye": dipole_debye,
        "polarizability_au": polarizability_au,
    }},
}}
with open("psi4_result.json", "w", encoding="utf-8") as handle:
    json.dump(result, handle, sort_keys=True)
''',
        encoding="utf-8",
    )


def _xyz_to_psi4_geometry(xyz_path: Path, *, charge: int = 0, multiplicity: int = 1) -> str:
    lines = xyz_path.read_text(encoding="utf-8", errors="replace").splitlines()
    atoms = [line.strip() for line in lines[2:] if line.strip()]
    return f"{charge} {multiplicity}\n" + "\n".join(atoms) + "\n"


def _charge_multiplicity_from_request(request_row: dict[str, Any]) -> tuple[int, int]:
    charge = _int_or_none(
        _first_present(
            request_row,
            "formal_charge",
            "molecular_charge",
            "charge",
        )
    )
    multiplicity = _int_or_none(
        _first_present(
            request_row,
            "spin_multiplicity",
            "multiplicity",
        )
    )
    smiles = str(_first_present(request_row, "canonical_smiles", "isomeric_smiles", "smiles") or "").strip()
    if smiles and (charge is None or multiplicity is None):
        try:
            from rdkit import Chem

            molecule = Chem.MolFromSmiles(smiles)
        except Exception:
            molecule = None
        if molecule is not None:
            if charge is None:
                charge = int(sum(atom.GetFormalCharge() for atom in molecule.GetAtoms()))
            if multiplicity is None:
                radical_electrons = int(sum(atom.GetNumRadicalElectrons() for atom in molecule.GetAtoms()))
                multiplicity = max(1, radical_electrons + 1)
    return int(charge if charge is not None else 0), int(multiplicity if multiplicity is not None else 1)


def _result_row(
    request_row: dict[str, Any],
    *,
    canonical_feature_key: str,
    value_num: float | str,
    unit: str,
    converged: int,
    program_version: str,
    artifact_uri: str,
    artifact_sha256: str,
    notes: str,
) -> dict[str, Any]:
    return {
        "request_id": str(request_row.get("request_id", "")).strip(),
        "mol_id": str(request_row.get("mol_id", "")).strip(),
        "canonical_feature_key": canonical_feature_key,
        "value_num": value_num,
        "unit": unit,
        "program": "psi4",
        "program_version": program_version,
        "method_family": str(request_row.get("method_family", "DFT") or "DFT").strip(),
        "theory_level": str(request_row.get("theory_level", PSI4_DFT_THEORY_LEVEL) or PSI4_DFT_THEORY_LEVEL).strip(),
        "basis_set": str(request_row.get("basis_set", PSI4_DFT_BASIS_SET) or PSI4_DFT_BASIS_SET).strip(),
        "solvation_model": str(request_row.get("solvation_model", "gas_phase") or "gas_phase").strip(),
        "converged": int(converged),
        "imaginary_frequency_count": 0,
        "artifact_uri": artifact_uri,
        "artifact_sha256": artifact_sha256,
        "quality_level": "computed_high",
        "notes": notes,
    }


def _write_artifact_bundle(job_dir: Path, request_id: str, xyz_path: Path | None) -> Path:
    if xyz_path is not None and xyz_path.exists():
        local_xyz = job_dir / "input.xyz"
        if not local_xyz.exists():
            local_xyz.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(xyz_path, local_xyz)
    _write_sha256_manifest(job_dir)
    artifact_path = job_dir / f"{request_id}_psi4_artifact.tar.gz"
    with tarfile.open(artifact_path, "w:gz") as archive:
        for path in sorted(job_dir.rglob("*")):
            if not path.is_file() or path == artifact_path:
                continue
            if path.name.endswith("_psi4_artifact.tar.gz"):
                continue
            archive.add(path, arcname=path.relative_to(job_dir).as_posix())
    return artifact_path


def _write_sha256_manifest(job_dir: Path) -> Path:
    manifest_path = job_dir / "manifest.sha256"
    lines = []
    for path in sorted(job_dir.rglob("*")):
        if not path.is_file() or path.name.endswith("_psi4_artifact.tar.gz") or path == manifest_path:
            continue
        lines.append(f"{_sha256_file(path)}  {path.relative_to(job_dir).as_posix()}")
    manifest_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return manifest_path


def _completed_psi4_request_ids(rows: pd.DataFrame) -> set[str]:
    if rows.empty:
        return set()
    rows = rows.copy().fillna("")
    mask = (
        rows["program"].astype(str).str.lower().eq("psi4")
        & rows["canonical_feature_key"].astype(str).ne("")
        & rows["converged"].astype(str).isin({"1", "true", "True", "yes"})
    )
    return set(rows.loc[mask, "request_id"].astype(str))


def _failed_psi4_request_ids(rows: pd.DataFrame) -> set[str]:
    if rows.empty:
        return set()
    rows = rows.copy().fillna("")
    psi4_rows = rows.loc[rows["program"].astype(str).str.lower().eq("psi4")].copy()
    if psi4_rows.empty:
        return set()
    completed = _completed_psi4_request_ids(psi4_rows)
    feature_keys = psi4_rows["canonical_feature_key"].astype(str).str.strip()
    values = pd.to_numeric(psi4_rows["value_num"], errors="coerce")
    converged = psi4_rows["converged"].map(lambda value: str(value).strip().lower() in {"1", "true", "yes", "y", "converged", "succeeded"})
    failure_mask = feature_keys.eq("") | ~converged | values.isna()
    failed = set(psi4_rows.loc[failure_mask, "request_id"].astype(str))
    return failed - completed


def _next_attempt_index(job_dir: Path) -> int:
    max_seen = 0
    for path in job_dir.glob("attempt_*"):
        if not path.is_dir():
            continue
        parts = path.name.split("_", 2)
        if len(parts) < 2:
            continue
        try:
            max_seen = max(max_seen, int(parts[1]))
        except ValueError:
            continue
    return max_seen + 1


def _resolve_psi4_bin(psi4_bin: Path | str | None) -> Path | None:
    if psi4_bin is not None:
        path = Path(psi4_bin)
        if path.exists():
            return path
        resolved = shutil.which(str(psi4_bin))
        return Path(resolved) if resolved else None
    env_value = os.getenv("R_PHYSGEN_PSI4_BIN", "").strip()
    if env_value:
        return _resolve_psi4_bin(env_value)
    resolved = shutil.which("psi4")
    return Path(resolved) if resolved else None


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


def _homo_lumo_from_wavefunction(wavefunction: dict[str, Any]) -> tuple[float | None, float | None]:
    energies = _numeric_list(_first_present(wavefunction, "epsilon_a_ev", "orbital_energies_ev"))
    multiplier = 1.0
    if not energies:
        energies = _numeric_list(_first_present(wavefunction, "epsilon_a_hartree", "orbital_energies_hartree"))
        multiplier = HARTREE_TO_EV
    if not energies:
        return None, None
    nalpha = _int_or_none(_first_present(wavefunction, "nalpha", "occupied_orbital_count", "num_occupied"))
    if nalpha is None:
        occupations = _numeric_list(_first_present(wavefunction, "occupations", "occupation_a"))
        occupied = [idx for idx, occupation in enumerate(occupations[: len(energies)]) if occupation > 1e-8]
        nalpha = max(occupied) + 1 if occupied else None
    if nalpha is None or nalpha <= 0:
        return None, None
    homo_index = nalpha - 1
    lumo_index = nalpha
    homo = energies[homo_index] * multiplier if homo_index < len(energies) else None
    lumo = energies[lumo_index] * multiplier if lumo_index < len(energies) else None
    return homo, lumo


def _dipole_debye_from_variables(variables: dict[str, Any]) -> float | None:
    direct = _float_or_none(
        _first_present(
            variables,
            "DIPOLE_DEBYE",
            "DIPOLE MOMENT",
            "CURRENT DIPOLE",
        )
    )
    if direct is not None:
        return direct
    vector = _first_present(variables, "dipole_au", "CURRENT DIPOLE AU", "SCF DIPOLE AU", "CURRENT DIPOLE", "SCF DIPOLE")
    if not isinstance(vector, list):
        for key, value in variables.items():
            if str(key).upper().endswith(" DIPOLE") and isinstance(value, list):
                vector = value
                break
    if isinstance(vector, list) and len(vector) >= 3:
        components = [_float_or_none(value) for value in vector[:3]]
    else:
        components = [
            _float_or_none(_first_present(variables, "CURRENT DIPOLE X", "SCF DIPOLE X", "DIPOLE X")),
            _float_or_none(_first_present(variables, "CURRENT DIPOLE Y", "SCF DIPOLE Y", "DIPOLE Y")),
            _float_or_none(_first_present(variables, "CURRENT DIPOLE Z", "SCF DIPOLE Z", "DIPOLE Z")),
        ]
    if all(component is not None for component in components):
        return math.sqrt(sum(float(component) ** 2 for component in components if component is not None)) * AU_DIPOLE_TO_DEBYE
    return None


def _polarizability_au_from_variables(variables: dict[str, Any]) -> float | None:
    direct = _float_or_none(
        _first_present(
            variables,
            "POLARIZABILITY_AU",
            "CURRENT POLARIZABILITY",
            "SCF POLARIZABILITY",
            "DIPOLE POLARIZABILITY",
        )
    )
    if direct is not None:
        return direct
    components = [
        _float_or_none(_first_present(variables, "CURRENT POLARIZABILITY XX", "SCF POLARIZABILITY XX", "DIPOLE POLARIZABILITY XX")),
        _float_or_none(_first_present(variables, "CURRENT POLARIZABILITY YY", "SCF POLARIZABILITY YY", "DIPOLE POLARIZABILITY YY")),
        _float_or_none(_first_present(variables, "CURRENT POLARIZABILITY ZZ", "SCF POLARIZABILITY ZZ", "DIPOLE POLARIZABILITY ZZ")),
    ]
    if all(component is not None for component in components):
        return sum(float(component) for component in components if component is not None) / 3.0
    return None


def _numeric_list(value: Any) -> list[float]:
    if not isinstance(value, list):
        return []
    values: list[float] = []
    for item in value:
        if isinstance(item, list):
            values.extend(_numeric_list(item))
        else:
            parsed = _float_or_none(item)
            if parsed is not None:
                values.append(parsed)
    return values


def _int_or_none(value: Any) -> int | None:
    parsed = _float_or_none(value)
    if parsed is None:
        return None
    return int(parsed)


def _first_present(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in data:
            return data[key]
    return None


def _float_or_none(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric):
        return None
    return numeric
