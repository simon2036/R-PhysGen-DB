"""xTB executor and parser helpers for the quantum pilot."""

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
from typing import Any

import pandas as pd
from rdkit import Chem, RDLogger
from rdkit.Chem import AllChem

from r_physgen_db.quantum_pilot import (
    QUANTUM_FEATURES,
    QUANTUM_INPUT_COLUMNS,
    completed_xtb_request_ids,
    merge_quantum_result_rows,
)


XTB_FEATURE_UNITS = {key: value["unit"] for key, value in QUANTUM_FEATURES.items()}
AU_DIPOLE_TO_DEBYE = 2.541746473
XTB_RETRY_PROFILE_NAMES = [
    "gfn2_opt",
    "rdkit_mmff_uff_gfn2_opt",
    "gfnff_preopt_gfn2_opt",
    "gfn1_preopt_gfn2_sp",
    "sf5_spread_gfn2_sp",
    "gfn2_sp_scc_aids",
    "gfn1_sp_fallback",
]
XTB_PROFILE_THEORY_LEVEL = {
    "gfn2_opt": "GFN2-xTB",
    "rdkit_mmff_uff_gfn2_opt": "GFN2-xTB",
    "gfnff_preopt_gfn2_opt": "GFN2-xTB",
    "gfn1_preopt_gfn2_sp": "GFN2-xTB",
    "sf5_spread_gfn2_sp": "GFN2-xTB",
    "gfn2_sp_scc_aids": "GFN2-xTB",
    "gfn1_sp_fallback": "GFN1-xTB",
}


@dataclass(slots=True)
class XtbScalarFeatures:
    values: dict[str, float]
    program_version: str
    method: str


@dataclass(slots=True)
class XtbJobResult:
    request_id: str
    mol_id: str
    status: str
    resumed: bool
    rows: list[dict[str, Any]]
    notes: str


def parse_xtb_scalar_features(json_path: Path, stdout_path: Path | None = None) -> XtbScalarFeatures:
    """Parse scalar quantum features from xTB JSON plus stdout-only sections."""

    data = _load_xtb_json(json_path)
    stdout_text = stdout_path.read_text(encoding="utf-8", errors="replace") if stdout_path and stdout_path.exists() else ""

    values: dict[str, float] = {}
    total_energy = _float_or_none(_first_present(data, "total energy", "total_energy", "energy"))
    if total_energy is not None:
        values["quantum.total_energy"] = total_energy

    homo, lumo = _homo_lumo_from_json(data)
    if homo is not None:
        values["quantum.homo_energy"] = homo
    if lumo is not None:
        values["quantum.lumo_energy"] = lumo

    gap = _float_or_none(_first_present(data, "HOMO-LUMO gap / eV", "HOMO-LUMO gap/eV", "HOMO-LUMO gap", "gap"))
    if gap is None and homo is not None and lumo is not None:
        gap = lumo - homo
    if gap is not None:
        values["quantum.homo_lumo_gap"] = gap

    dipole = _dipole_debye(stdout_text)
    if dipole is None:
        dipole = _dipole_debye_from_json(data)
    if dipole is not None:
        values["quantum.dipole_moment"] = dipole

    polarizability = _polarizability_au(stdout_text)
    if polarizability is None:
        polarizability = _float_or_none(_first_present(data, "polarizability", "alpha", "Mol. alpha(0) /au", "Mol. α(0) /au"))
    if polarizability is not None:
        values["quantum.polarizability"] = polarizability

    return XtbScalarFeatures(
        values={key: values[key] for key in QUANTUM_FEATURES if key in values},
        program_version=str(_first_present(data, "xtb version", "program_version") or ""),
        method=str(_first_present(data, "method") or "GFN2-xTB"),
    )


def _load_xtb_json(json_path: Path) -> dict[str, Any]:
    text = json_path.read_text(encoding="utf-8")
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        # xTB can print overflowed numeric fields as Fortran-style "******",
        # making the JSON invalid even though lower occupied orbital values
        # and scalar properties remain usable. Treat those overflow sentinels
        # as null so the parser can recover auditable real values.
        sanitized = re.sub(r"(?<![\"])\*{3,}(?![\"])", "null", text)
        data = json.loads(sanitized)
    if not isinstance(data, dict):
        raise ValueError(f"xTB JSON root must be an object: {json_path}")
    return data


def run_xtb_quantum_pilot(
    *,
    requests_path: Path,
    xyz_manifest_path: Path,
    output_path: Path,
    artifact_dir: Path,
    xtb_bin: Path | str | None,
    limit: int | None = None,
    jobs: int = 1,
    threads_per_job: int = 1,
    resume: bool = True,
    allow_missing_executor: bool = False,
    retry_failed_only: bool = False,
    completion_required: bool = False,
    retry_profiles: list[str] | tuple[str, ...] | None = None,
) -> dict[str, int]:
    """Run xTB for a request/XYZ manifest and write ingestion-ready CSV rows."""

    requests = pd.read_csv(requests_path).fillna("")
    xyz_manifest = pd.read_csv(xyz_manifest_path).fillna("")
    if limit is not None:
        requests = requests.head(int(limit)).copy()

    xyz_by_request = {
        str(row["request_id"]): Path(str(row["xyz_path"]))
        for row in xyz_manifest.to_dict(orient="records")
        if str(row.get("request_id", "")).strip()
    }

    existing = pd.read_csv(output_path).fillna("") if output_path.exists() else pd.DataFrame(columns=QUANTUM_INPUT_COLUMNS)
    completed = _completed_xtb_request_ids(existing) if resume else set()
    failed_targets = _failed_xtb_request_ids(existing) if retry_failed_only else set()
    profile_names = _xtb_profile_names(retry_profiles, retry_failed_only=retry_failed_only)
    use_attempt_dirs = bool(retry_failed_only or retry_profiles)

    tasks = []
    target_request_ids: set[str] = set()
    for row in requests.to_dict(orient="records"):
        request_id = str(row.get("request_id", "")).strip()
        if not request_id:
            continue
        target_request_ids.add(request_id)
        if retry_failed_only:
            if request_id not in failed_targets:
                continue
        elif request_id in completed:
            continue
        tasks.append((row, xyz_by_request.get(request_id)))
    task_request_ids = {str(row[0].get("request_id", "")).strip() for row in tasks if str(row[0].get("request_id", "")).strip()}

    artifact_dir.mkdir(parents=True, exist_ok=True)
    workers = max(1, int(jobs or 1))
    rows: list[dict[str, Any]] = []
    summary = {
        "requested": len(tasks),
        "succeeded": 0,
        "failed": 0,
        "resumed": 0 if retry_failed_only else len(completed & target_request_ids),
        "rows_written": 0,
        "retry_failed_only": int(retry_failed_only),
        "completion_required": int(completion_required),
        "failed_target_count": len(failed_targets),
    }
    resolved_xtb = _resolve_xtb_bin(xtb_bin)
    if resolved_xtb is None and not allow_missing_executor:
        raise FileNotFoundError("xTB executable not found; set R_PHYSGEN_XTB_BIN, pass xtb_bin, or allow_missing_executor=True")

    if workers == 1:
        results = [
            _run_one_xtb_job(
                row,
                xyz_path,
                xtb_bin=resolved_xtb,
                artifact_dir=artifact_dir,
                threads_per_job=threads_per_job,
                resume=resume,
                allow_missing_executor=allow_missing_executor,
                profile_names=profile_names,
                use_attempt_dirs=use_attempt_dirs,
            )
            for row, xyz_path in tasks
        ]
    else:
        results = []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(
                    _run_one_xtb_job,
                    row,
                    xyz_path,
                    xtb_bin=resolved_xtb,
                    artifact_dir=artifact_dir,
                    threads_per_job=threads_per_job,
                    resume=resume,
                    allow_missing_executor=allow_missing_executor,
                    profile_names=profile_names,
                    use_attempt_dirs=use_attempt_dirs,
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
        if result.resumed:
            summary["resumed"] += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged = merge_quantum_result_rows(existing, pd.DataFrame(rows, columns=QUANTUM_INPUT_COLUMNS))
    merged.to_csv(output_path, index=False)
    summary["rows_written"] = len(rows)
    summary["merged_rows"] = len(merged)
    if completion_required:
        if retry_failed_only:
            incomplete = sorted(_failed_xtb_request_ids(merged) & task_request_ids)
        else:
            completed_after = _completed_xtb_request_ids(merged)
            incomplete = sorted(target_request_ids - completed_after)
        if incomplete:
            sample = ", ".join(incomplete[:10])
            raise RuntimeError(f"xTB completion required but {len(incomplete)} request(s) remain incomplete: {sample}")
    return summary


def _run_one_xtb_job(
    request_row: dict[str, Any],
    xyz_path: Path | None,
    *,
    xtb_bin: Path | None,
    artifact_dir: Path,
    threads_per_job: int,
    resume: bool,
    allow_missing_executor: bool,
    profile_names: list[str],
    use_attempt_dirs: bool,
) -> XtbJobResult:
    request_id = str(request_row.get("request_id", "")).strip()
    mol_id = str(request_row.get("mol_id", "")).strip()
    job_dir = artifact_dir / request_id
    job_dir.mkdir(parents=True, exist_ok=True)
    if use_attempt_dirs:
        return _run_one_xtb_retry_job(
            request_row,
            xyz_path,
            xtb_bin=xtb_bin,
            job_dir=job_dir,
            threads_per_job=threads_per_job,
            allow_missing_executor=allow_missing_executor,
            profile_names=profile_names,
        )

    stdout_path = job_dir / "xtb.stdout"
    stderr_path = job_dir / "xtb.stderr"
    returncode_path = job_dir / "xtb.returncode"
    json_path = job_dir / "xtbout.json"
    resumed = bool(resume and returncode_path.exists() and (stdout_path.exists() or stderr_path.exists()))

    if not resumed:
        if xyz_path is None or not xyz_path.exists():
            stdout_path.write_text("", encoding="utf-8")
            stderr_path.write_text(f"XYZ input missing for request_id={request_id}: {xyz_path}\n", encoding="utf-8")
            returncode_path.write_text("127", encoding="utf-8")
        elif xtb_bin is None and allow_missing_executor:
            stdout_path.write_text("", encoding="utf-8")
            stderr_path.write_text("executor_unavailable: xTB executable not found\n", encoding="utf-8")
            returncode_path.write_text("127", encoding="utf-8")
        else:
            env = os.environ.copy()
            env["OMP_NUM_THREADS"] = str(max(1, int(threads_per_job or 1)))
            env["MKL_NUM_THREADS"] = str(max(1, int(threads_per_job or 1)))
            env["OPENBLAS_NUM_THREADS"] = str(max(1, int(threads_per_job or 1)))
            command = [str(xtb_bin), str(xyz_path.resolve()), "--gfn", "2", "--opt", "loose", "--json", "--alpha"]
            completed = subprocess.run(
                command,
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

    artifact_path = _write_artifact_bundle(job_dir, request_id)
    artifact_sha256 = _sha256_file(artifact_path)
    artifact_uri = artifact_path.as_posix()
    returncode = _returncode(returncode_path)

    try:
        parsed = parse_xtb_scalar_features(json_path, stdout_path)
    except Exception as exc:
        notes = f"xTB parse failed after returncode={returncode}: {exc}"
        if xtb_bin is None and allow_missing_executor:
            notes = f"executor_unavailable; {notes}"
        return XtbJobResult(
            request_id=request_id,
            mol_id=mol_id,
            status="failed",
            resumed=resumed,
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

    required = set(QUANTUM_FEATURES)
    missing = sorted(required - set(parsed.values))
    if returncode != 0 or missing:
        notes = f"xTB failed or incomplete: returncode={returncode}; missing_features={','.join(missing)}"
        return XtbJobResult(
            request_id=request_id,
            mol_id=mol_id,
            status="failed",
            resumed=resumed,
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
            unit=XTB_FEATURE_UNITS[feature_key],
            converged=1,
            program_version=parsed.program_version,
            artifact_uri=artifact_uri,
            artifact_sha256=artifact_sha256,
            notes=f"xTB GFN2-xTB scalar parsed from xtbout.json/stdout; artifact retains charges/WBO files; resumed={int(resumed)}",
        )
        for feature_key, value in parsed.values.items()
    ]
    return XtbJobResult(request_id=request_id, mol_id=mol_id, status="succeeded", resumed=resumed, rows=rows, notes="succeeded")


def _run_one_xtb_retry_job(
    request_row: dict[str, Any],
    xyz_path: Path | None,
    *,
    xtb_bin: Path | None,
    job_dir: Path,
    threads_per_job: int,
    allow_missing_executor: bool,
    profile_names: list[str],
) -> XtbJobResult:
    request_id = str(request_row.get("request_id", "")).strip()
    mol_id = str(request_row.get("mol_id", "")).strip()
    next_attempt = _next_attempt_index(job_dir)
    last_notes = "no retry attempts executed"
    last_program_version = ""
    last_profile = ""

    for profile_name in profile_names:
        attempt_dir = job_dir / f"attempt_{next_attempt:02d}_{profile_name}"
        next_attempt += 1
        attempt_dir.mkdir(parents=True, exist_ok=True)
        stdout_path = attempt_dir / "xtb.stdout"
        stderr_path = attempt_dir / "xtb.stderr"
        returncode_path = attempt_dir / "xtb.returncode"
        json_path = attempt_dir / "xtbout.json"
        last_profile = profile_name

        if xyz_path is None or not xyz_path.exists():
            stdout_path.write_text("", encoding="utf-8")
            stderr_path.write_text(f"XYZ input unavailable for request_id={request_id}\n", encoding="utf-8")
            returncode_path.write_text("127", encoding="utf-8")
        elif xtb_bin is None and allow_missing_executor:
            stdout_path.write_text("", encoding="utf-8")
            stderr_path.write_text("executor_unavailable: xTB executable unavailable\n", encoding="utf-8")
            returncode_path.write_text("127", encoding="utf-8")
        else:
            local_input = attempt_dir / "input.xyz"
            shutil.copy2(xyz_path, local_input)
            _run_xtb_profile(
                profile_name,
                request_row,
                local_input,
                xtb_bin=xtb_bin,
                attempt_dir=attempt_dir,
                threads_per_job=threads_per_job,
            )
        _write_sha256_manifest(attempt_dir)
        returncode = _returncode(returncode_path)
        theory_level = XTB_PROFILE_THEORY_LEVEL[profile_name]
        try:
            parsed = parse_xtb_scalar_features(json_path, stdout_path)
            last_program_version = parsed.program_version
        except Exception as exc:
            last_notes = f"xTB retry profile {profile_name} parse failed after returncode={returncode}: {exc}"
            continue
        missing = sorted(set(QUANTUM_FEATURES) - set(parsed.values))
        if returncode != 0 or missing:
            last_notes = f"xTB retry profile {profile_name} incomplete: returncode={returncode}; missing_features={','.join(missing)}"
            continue

        artifact_path = _write_artifact_bundle(job_dir, request_id)
        artifact_sha256 = _sha256_file(artifact_path)
        notes = (
            f"xTB scalar retry succeeded; retry_profile={profile_name}; "
            f"theory_level={theory_level}; artifact retains all attempts"
        )
        if theory_level == "GFN1-xTB":
            notes = f"{notes}; fallback_method=GFN1-xTB values are not labeled as GFN2"
        rows = [
            _result_row(
                request_row,
                canonical_feature_key=feature_key,
                value_num=value,
                unit=XTB_FEATURE_UNITS[feature_key],
                converged=1,
                program_version=parsed.program_version,
                artifact_uri=artifact_path.as_posix(),
                artifact_sha256=artifact_sha256,
                notes=notes,
                theory_level=theory_level,
            )
            for feature_key, value in parsed.values.items()
        ]
        return XtbJobResult(request_id=request_id, mol_id=mol_id, status="succeeded", resumed=False, rows=rows, notes=notes)

    artifact_path = _write_artifact_bundle(job_dir, request_id)
    artifact_sha256 = _sha256_file(artifact_path)
    if xtb_bin is None and allow_missing_executor:
        last_notes = f"executor_unavailable; {last_notes}"
    return XtbJobResult(
        request_id=request_id,
        mol_id=mol_id,
        status="failed",
        resumed=False,
        rows=[
            _result_row(
                request_row,
                canonical_feature_key="",
                value_num="",
                unit="",
                converged=0,
                program_version=last_program_version,
                artifact_uri=artifact_path.as_posix(),
                artifact_sha256=artifact_sha256,
                notes=last_notes,
                theory_level=XTB_PROFILE_THEORY_LEVEL.get(last_profile, "GFN2-xTB"),
            )
        ],
        notes=last_notes,
    )


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
    theory_level: str = "GFN2-xTB",
) -> dict[str, Any]:
    return {
        "request_id": str(request_row.get("request_id", "")).strip(),
        "mol_id": str(request_row.get("mol_id", "")).strip(),
        "canonical_feature_key": canonical_feature_key,
        "value_num": value_num,
        "unit": unit,
        "program": "xtb",
        "program_version": program_version,
        "method_family": "semiempirical",
        "theory_level": theory_level,
        "basis_set": "",
        "solvation_model": "gas_phase",
        "converged": int(converged),
        "imaginary_frequency_count": 0,
        "artifact_uri": artifact_uri,
        "artifact_sha256": artifact_sha256,
        "quality_level": "calculated_open_source",
        "notes": notes,
    }


def _xtb_profile_names(profile_names: list[str] | tuple[str, ...] | None, *, retry_failed_only: bool) -> list[str]:
    if profile_names is None:
        return list(XTB_RETRY_PROFILE_NAMES if retry_failed_only else ["gfn2_opt"])
    resolved = [str(name).strip() for name in profile_names if str(name).strip()]
    unknown = sorted(set(resolved) - set(XTB_RETRY_PROFILE_NAMES))
    if unknown:
        raise ValueError(f"Unknown xTB retry profile(s): {', '.join(unknown)}")
    return resolved or ["gfn2_opt"]


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


def _run_xtb_profile(
    profile_name: str,
    request_row: dict[str, Any],
    input_xyz: Path,
    *,
    xtb_bin: Path,
    attempt_dir: Path,
    threads_per_job: int,
) -> None:
    if profile_name == "gfn2_opt":
        _run_xtb_command(
            xtb_bin,
            [input_xyz.resolve(), "--gfn", "2", "--opt", "loose", "--json", "--alpha"],
            cwd=attempt_dir,
            stdout_path=attempt_dir / "xtb.stdout",
            stderr_path=attempt_dir / "xtb.stderr",
            returncode_path=attempt_dir / "xtb.returncode",
            threads_per_job=threads_per_job,
        )
        return

    if profile_name == "rdkit_mmff_uff_gfn2_opt":
        rdkit_input = attempt_dir / "input_rdkit_preopt.xyz"
        _write_rdkit_preoptimized_xyz(request_row, input_xyz, rdkit_input)
        _run_xtb_command(
            xtb_bin,
            [rdkit_input.resolve(), "--gfn", "2", "--opt", "loose", "--json", "--alpha"],
            cwd=attempt_dir,
            stdout_path=attempt_dir / "xtb.stdout",
            stderr_path=attempt_dir / "xtb.stderr",
            returncode_path=attempt_dir / "xtb.returncode",
            threads_per_job=threads_per_job,
        )
        return

    if profile_name == "gfnff_preopt_gfn2_opt":
        preopt_xyz = _xtb_preopt(
            xtb_bin,
            input_xyz,
            attempt_dir=attempt_dir,
            label="gfnff_preopt",
            args=["--gfnff", "--opt", "loose"],
            threads_per_job=threads_per_job,
        )
        if preopt_xyz is None:
            return
        _run_xtb_command(
            xtb_bin,
            [preopt_xyz.resolve(), "--gfn", "2", "--opt", "loose", "--json", "--alpha"],
            cwd=attempt_dir,
            stdout_path=attempt_dir / "xtb.stdout",
            stderr_path=attempt_dir / "xtb.stderr",
            returncode_path=attempt_dir / "xtb.returncode",
            threads_per_job=threads_per_job,
        )
        return

    if profile_name == "gfn1_preopt_gfn2_sp":
        preopt_xyz = _xtb_preopt(
            xtb_bin,
            input_xyz,
            attempt_dir=attempt_dir,
            label="gfn1_preopt",
            args=["--gfn", "1", "--opt", "loose"],
            threads_per_job=threads_per_job,
        )
        if preopt_xyz is None:
            return
        _run_xtb_command(
            xtb_bin,
            [preopt_xyz.resolve(), "--gfn", "2", "--sp", "--json", "--alpha", "--acc", "5.0", "--etemp", "1000"],
            cwd=attempt_dir,
            stdout_path=attempt_dir / "xtb.stdout",
            stderr_path=attempt_dir / "xtb.stderr",
            returncode_path=attempt_dir / "xtb.returncode",
            threads_per_job=threads_per_job,
        )
        return

    if profile_name == "gfn2_sp_scc_aids":
        _run_xtb_command(
            xtb_bin,
            [input_xyz.resolve(), "--gfn", "2", "--sp", "--json", "--alpha", "--acc", "5.0", "--etemp", "1000"],
            cwd=attempt_dir,
            stdout_path=attempt_dir / "xtb.stdout",
            stderr_path=attempt_dir / "xtb.stderr",
            returncode_path=attempt_dir / "xtb.returncode",
            threads_per_job=threads_per_job,
        )
        return

    if profile_name == "sf5_spread_gfn2_sp":
        spread_input = attempt_dir / "input_sf5_spread.xyz"
        _write_sf5_spread_xyz(request_row, input_xyz, spread_input)
        _run_xtb_command(
            xtb_bin,
            [spread_input.resolve(), "--gfn", "2", "--sp", "--json", "--alpha", "--acc", "5.0", "--etemp", "1000"],
            cwd=attempt_dir,
            stdout_path=attempt_dir / "xtb.stdout",
            stderr_path=attempt_dir / "xtb.stderr",
            returncode_path=attempt_dir / "xtb.returncode",
            threads_per_job=threads_per_job,
        )
        return

    if profile_name == "gfn1_sp_fallback":
        _run_xtb_command(
            xtb_bin,
            [input_xyz.resolve(), "--gfn", "1", "--sp", "--json", "--alpha", "--acc", "5.0", "--etemp", "1000"],
            cwd=attempt_dir,
            stdout_path=attempt_dir / "xtb.stdout",
            stderr_path=attempt_dir / "xtb.stderr",
            returncode_path=attempt_dir / "xtb.returncode",
            threads_per_job=threads_per_job,
        )
        return

    raise ValueError(f"Unknown xTB retry profile: {profile_name}")


def _xtb_preopt(
    xtb_bin: Path,
    input_xyz: Path,
    *,
    attempt_dir: Path,
    label: str,
    args: list[str],
    threads_per_job: int,
) -> Path | None:
    _run_xtb_command(
        xtb_bin,
        [input_xyz.resolve(), *args],
        cwd=attempt_dir,
        stdout_path=attempt_dir / f"{label}.stdout",
        stderr_path=attempt_dir / f"{label}.stderr",
        returncode_path=attempt_dir / f"{label}.returncode",
        threads_per_job=threads_per_job,
    )
    optimized = attempt_dir / "xtbopt.xyz"
    if not optimized.exists():
        (attempt_dir / "xtb.returncode").write_text("1", encoding="utf-8")
        (attempt_dir / "xtb.stdout").write_text("", encoding="utf-8")
        (attempt_dir / "xtb.stderr").write_text(f"{label} did not produce xtbopt.xyz\n", encoding="utf-8")
        return None
    preopt_xyz = attempt_dir / f"{label}.xyz"
    shutil.copy2(optimized, preopt_xyz)
    return preopt_xyz


def _run_xtb_command(
    xtb_bin: Path,
    args: list[Path | str],
    *,
    cwd: Path,
    stdout_path: Path,
    stderr_path: Path,
    returncode_path: Path,
    threads_per_job: int,
) -> None:
    env = os.environ.copy()
    threads = str(max(1, int(threads_per_job or 1)))
    env["OMP_NUM_THREADS"] = threads
    env["MKL_NUM_THREADS"] = threads
    env["OPENBLAS_NUM_THREADS"] = threads
    env.setdefault("OMP_STACKSIZE", "4G")
    completed = subprocess.run(
        [str(xtb_bin), *[str(arg) for arg in args]],
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    stdout_path.write_text(completed.stdout, encoding="utf-8")
    stderr_path.write_text(completed.stderr, encoding="utf-8")
    returncode_path.write_text(str(completed.returncode), encoding="utf-8")


def _write_rdkit_preoptimized_xyz(request_row: dict[str, Any], fallback_xyz: Path, output_xyz: Path) -> None:
    smiles = str(request_row.get("isomeric_smiles") or request_row.get("canonical_smiles") or "").strip()
    if not smiles:
        shutil.copy2(fallback_xyz, output_xyz)
        return
    RDLogger.DisableLog("rdApp.warning")
    RDLogger.DisableLog("rdApp.error")
    try:
        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            shutil.copy2(fallback_xyz, output_xyz)
            return
        mol = Chem.AddHs(mol)
        if mol.GetNumAtoms() <= 1:
            shutil.copy2(fallback_xyz, output_xyz)
            return
        seed = int(hashlib.sha256(f"{request_row.get('request_id', '')}|{smiles}|retry".encode("utf-8")).hexdigest()[:8], 16) & 0x7FFFFFFF
        params = AllChem.ETKDGv3()
        params.randomSeed = seed
        conformer_ids = list(AllChem.EmbedMultipleConfs(mol, numConfs=16, params=params))
        if not conformer_ids:
            shutil.copy2(fallback_xyz, output_xyz)
            return
        energies: list[tuple[float, int]] = []
        for conf_id in conformer_ids:
            try:
                if AllChem.MMFFHasAllMoleculeParams(mol):
                    result = AllChem.MMFFOptimizeMolecule(mol, confId=conf_id, maxIters=1000)
                    props = AllChem.MMFFGetMoleculeProperties(mol)
                    forcefield = AllChem.MMFFGetMoleculeForceField(mol, props, confId=conf_id)
                else:
                    result = AllChem.UFFOptimizeMolecule(mol, confId=conf_id, maxIters=1000)
                    forcefield = AllChem.UFFGetMoleculeForceField(mol, confId=conf_id)
                energy = float(forcefield.CalcEnergy()) if forcefield is not None else float(result)
            except Exception:
                energy = float("inf")
            energies.append((energy, conf_id))
        _, best_conf_id = min(energies, key=lambda item: item[0])
        conf = mol.GetConformer(best_conf_id)
        lines = [
            str(mol.GetNumAtoms()),
            f"{request_row.get('request_id', '')} {request_row.get('mol_id', '')} rdkit_multiconformer_preopt smiles={smiles}",
        ]
        for atom in mol.GetAtoms():
            pos = conf.GetAtomPosition(atom.GetIdx())
            lines.append(f"{atom.GetSymbol()} {pos.x:.8f} {pos.y:.8f} {pos.z:.8f}")
        output_xyz.write_text("\n".join(lines) + "\n", encoding="utf-8")
    finally:
        RDLogger.EnableLog("rdApp.warning")
        RDLogger.EnableLog("rdApp.error")


def _write_sf5_spread_xyz(request_row: dict[str, Any], fallback_xyz: Path, output_xyz: Path) -> None:
    """Spread collapsed SF5 substituent fluorines into octahedral-like positions."""

    smiles = str(request_row.get("isomeric_smiles") or request_row.get("canonical_smiles") or "").strip()
    if not smiles or "S" not in smiles:
        shutil.copy2(fallback_xyz, output_xyz)
        return
    parsed = _read_xyz_atoms(fallback_xyz)
    if parsed is None:
        shutil.copy2(fallback_xyz, output_xyz)
        return
    symbols, coords, comment = parsed
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        shutil.copy2(fallback_xyz, output_xyz)
        return
    mol = Chem.AddHs(mol)
    if mol.GetNumAtoms() != len(symbols):
        shutil.copy2(fallback_xyz, output_xyz)
        return

    adjusted = [list(coord) for coord in coords]
    changed = False
    for atom in mol.GetAtoms():
        if atom.GetSymbol() != "S":
            continue
        sulfur_idx = atom.GetIdx()
        fluorine_neighbors = [nbr.GetIdx() for nbr in atom.GetNeighbors() if nbr.GetSymbol() == "F"]
        if len(fluorine_neighbors) < 5:
            continue
        anchor_neighbors = [nbr.GetIdx() for nbr in atom.GetNeighbors() if nbr.GetSymbol() != "F"]
        sulfur_coord = adjusted[sulfur_idx]
        if anchor_neighbors:
            anchor_vector = _unit_vector(_vector_sub(adjusted[anchor_neighbors[0]], sulfur_coord))
        else:
            anchor_vector = [1.0, 0.0, 0.0]
        if _vector_norm(anchor_vector) == 0.0:
            anchor_vector = [1.0, 0.0, 0.0]
        basis_a, basis_b = _perpendicular_basis(anchor_vector)
        sf_distance = 1.60
        positions = [_vector_add(sulfur_coord, _vector_scale(anchor_vector, -sf_distance))]
        for angle in (0.0, math.pi / 2.0, math.pi, 3.0 * math.pi / 2.0):
            radial = _vector_add(_vector_scale(basis_a, math.cos(angle)), _vector_scale(basis_b, math.sin(angle)))
            positions.append(_vector_add(sulfur_coord, _vector_scale(radial, sf_distance)))
        for neighbor_idx, new_coord in zip(fluorine_neighbors[:5], positions, strict=False):
            adjusted[neighbor_idx] = new_coord
            changed = True

    if not changed:
        shutil.copy2(fallback_xyz, output_xyz)
        return
    lines = [
        str(len(symbols)),
        f"{comment} sf5_spread_preprocessor=octahedral_decollision",
    ]
    for symbol, coord in zip(symbols, adjusted, strict=False):
        lines.append(f"{symbol} {coord[0]:.8f} {coord[1]:.8f} {coord[2]:.8f}")
    output_xyz.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _read_xyz_atoms(path: Path) -> tuple[list[str], list[list[float]], str] | None:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    if len(lines) < 2:
        return None
    try:
        atom_count = int(lines[0].strip())
    except ValueError:
        return None
    symbols: list[str] = []
    coords: list[list[float]] = []
    for line in lines[2 : 2 + atom_count]:
        parts = line.split()
        if len(parts) < 4:
            return None
        try:
            coord = [float(parts[1]), float(parts[2]), float(parts[3])]
        except ValueError:
            return None
        symbols.append(parts[0])
        coords.append(coord)
    if len(symbols) != atom_count:
        return None
    return symbols, coords, lines[1].strip()


def _vector_sub(left: list[float], right: list[float]) -> list[float]:
    return [left[idx] - right[idx] for idx in range(3)]


def _vector_add(left: list[float], right: list[float]) -> list[float]:
    return [left[idx] + right[idx] for idx in range(3)]


def _vector_scale(vector: list[float], scalar: float) -> list[float]:
    return [value * scalar for value in vector]


def _vector_norm(vector: list[float]) -> float:
    return math.sqrt(sum(value * value for value in vector))


def _unit_vector(vector: list[float]) -> list[float]:
    norm = _vector_norm(vector)
    if norm <= 1e-12:
        return [0.0, 0.0, 0.0]
    return [value / norm for value in vector]


def _cross(left: list[float], right: list[float]) -> list[float]:
    return [
        left[1] * right[2] - left[2] * right[1],
        left[2] * right[0] - left[0] * right[2],
        left[0] * right[1] - left[1] * right[0],
    ]


def _perpendicular_basis(axis: list[float]) -> tuple[list[float], list[float]]:
    reference = [0.0, 0.0, 1.0] if abs(axis[2]) < 0.9 else [0.0, 1.0, 0.0]
    first = _unit_vector(_cross(axis, reference))
    if _vector_norm(first) <= 1e-12:
        first = [1.0, 0.0, 0.0]
    second = _unit_vector(_cross(axis, first))
    return first, second


def _write_artifact_bundle(job_dir: Path, request_id: str) -> Path:
    artifact_path = job_dir / f"{request_id}_xtb_artifact.tar.gz"
    _write_sha256_manifest(job_dir)
    with tarfile.open(artifact_path, "w:gz") as archive:
        for path in sorted(job_dir.rglob("*")):
            if not path.is_file() or path == artifact_path:
                continue
            if path.name.endswith("_xtb_artifact.tar.gz"):
                continue
            archive.add(path, arcname=path.relative_to(job_dir).as_posix())
    return artifact_path


def _write_sha256_manifest(job_dir: Path) -> Path:
    manifest_path = job_dir / "manifest.sha256"
    lines = []
    for path in sorted(job_dir.rglob("*")):
        if not path.is_file() or path.name.endswith("_xtb_artifact.tar.gz") or path == manifest_path:
            continue
        lines.append(f"{_sha256_file(path)}  {path.relative_to(job_dir).as_posix()}")
    manifest_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return manifest_path


def _completed_xtb_request_ids(rows: pd.DataFrame) -> set[str]:
    return completed_xtb_request_ids(rows)


def _failed_xtb_request_ids(rows: pd.DataFrame) -> set[str]:
    if rows.empty:
        return set()
    rows = rows.copy().fillna("")
    xtb_rows = rows.loc[rows["program"].astype(str).str.lower().eq("xtb")].copy()
    if xtb_rows.empty:
        return set()
    completed = _completed_xtb_request_ids(xtb_rows)
    feature_keys = xtb_rows["canonical_feature_key"].astype(str).str.strip()
    values = pd.to_numeric(xtb_rows["value_num"], errors="coerce")
    converged = xtb_rows["converged"].map(lambda value: str(value).strip().lower() in {"1", "true", "yes", "y", "converged", "succeeded"})
    failure_mask = feature_keys.eq("") | ~converged | values.isna()
    failed = set(xtb_rows.loc[failure_mask, "request_id"].astype(str))
    return failed - completed


def _resolve_xtb_bin(xtb_bin: Path | str | None) -> Path | None:
    if xtb_bin is not None:
        path = Path(xtb_bin)
        if path.exists():
            return path
        resolved = shutil.which(str(xtb_bin))
        return Path(resolved) if resolved else None
    env_value = os.getenv("R_PHYSGEN_XTB_BIN", "").strip()
    if env_value:
        return _resolve_xtb_bin(env_value)
    resolved = shutil.which("xtb")
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


def _homo_lumo_from_json(data: dict[str, Any]) -> tuple[float | None, float | None]:
    energies_raw = _first_present(data, "orbital energies / eV", "orbital energies/eV", "orbital_energies_ev")
    if not isinstance(energies_raw, list):
        return None, None
    energies = [_float_or_none(value) for value in energies_raw]
    energies = [value for value in energies if value is not None]
    if not energies:
        return None, None

    occupations_raw = _first_present(data, "fractional occupation", "fractional occupations", "occupations")
    if isinstance(occupations_raw, list) and len(occupations_raw) >= len(energies):
        occupations = [_float_or_none(value) or 0.0 for value in occupations_raw[: len(energies)]]
        occupied_indices = [idx for idx, occupation in enumerate(occupations) if occupation > 1e-8]
        empty_indices = [idx for idx, occupation in enumerate(occupations) if occupation <= 1e-8]
        homo = energies[max(occupied_indices)] if occupied_indices else None
        lumo_candidates = [idx for idx in empty_indices if not occupied_indices or idx > max(occupied_indices)]
        lumo = energies[min(lumo_candidates)] if lumo_candidates else None
        return homo, lumo

    electron_count = _float_or_none(_first_present(data, "number of electrons", "electrons"))
    if electron_count is not None:
        homo_idx = max(0, int(math.ceil(electron_count / 2.0)) - 1)
        lumo_idx = homo_idx + 1
        homo = energies[homo_idx] if homo_idx < len(energies) else None
        lumo = energies[lumo_idx] if lumo_idx < len(energies) else None
        return homo, lumo
    return None, None


def _dipole_debye(stdout_text: str) -> float | None:
    if not stdout_text:
        return None
    match = re.search(
        r"^\s*full:\s+[-+]?\d+(?:\.\d+)?\s+[-+]?\d+(?:\.\d+)?\s+[-+]?\d+(?:\.\d+)?\s+([-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?)",
        stdout_text,
        flags=re.MULTILINE,
    )
    if match:
        return float(match.group(1))
    return None


def _dipole_debye_from_json(data: dict[str, Any]) -> float | None:
    vector = _first_present(data, "dipole / a.u.", "dipole/a.u.", "dipole")
    if isinstance(vector, list) and vector:
        components = [_float_or_none(value) for value in vector[:3]]
        if all(value is not None for value in components):
            return math.sqrt(sum(float(value) ** 2 for value in components if value is not None)) * AU_DIPOLE_TO_DEBYE
    return None


def _polarizability_au(stdout_text: str) -> float | None:
    if not stdout_text:
        return None
    match = re.search(
        r"Mol\.\s*(?:α|alpha)\(0\)\s*/\s*au\s*:\s*([-+]?\d+(?:\.\d+)?(?:[Ee][-+]?\d+)?)",
        stdout_text,
        flags=re.IGNORECASE,
    )
    if match:
        return float(match.group(1))
    return None


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
