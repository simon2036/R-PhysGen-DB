"""Retry remaining active-learning cycle computations with CoolProp/REFPROP backends."""

from __future__ import annotations

import json
import os
import platform
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal

import pandas as pd
from CoolProp.CoolProp import PropsSI

try:  # pragma: no cover - constant presence is CoolProp-version dependent
    import CoolProp.CoolProp as CP
except Exception:  # noqa: BLE001
    CP = None  # type: ignore[assignment]

from r_physgen_db.constants import DATA_DIR, STANDARD_CYCLE, TRANSCRITICAL_CO2_CYCLE
from r_physgen_db.cycle_conditions import operating_point_hash
from r_physgen_db.utils import ensure_directory, now_iso, sha256_file, slugify, write_json


BackendName = Literal["auto", "coolprop", "refprop"]
PropsFunction = Callable[..., float]

CYCLE_RESULT_SOURCE_ID = "source_r_physgen_cycle_backend_results"
CYCLE_RESULT_SOURCE_NAME = "R-PhysGen-DB Cycle Backend Results"
DEFAULT_RESULTS_PATH = DATA_DIR / "raw" / "manual" / "cycle_backend_results.csv"
DEFAULT_ARTIFACT_DIR = DATA_DIR / "raw" / "manual" / "cycle_backend_artifacts"

TRANSPORT_PROPERTIES = {
    "viscosity_liquid_pas": ("V", "Pa*s", "viscosity"),
    "thermal_conductivity_liquid_wmk": ("L", "W/(m*K)", "thermal conductivity"),
}
CYCLE_PROPERTIES = {
    "cop_standard_cycle": ("cop", "dimensionless"),
    "volumetric_cooling_mjm3": ("qvol", "MJ/m3"),
    "pressure_ratio": ("pressure_ratio", "dimensionless"),
    "discharge_temperature_c": ("discharge_temperature_c", "degC"),
}
RUN_CYCLE_REQUIRED_PROPERTIES = [
    "viscosity_liquid_pas",
    "thermal_conductivity_liquid_wmk",
    "cop_standard_cycle",
]

STANDARD_SUBCRITICAL_SPEC = {
    "cycle_case_id": "standard_subcritical_cycle",
    "case_name": "5 degC evaporating / 50 degC condensing",
    "evaporating_temperature_c": STANDARD_CYCLE["evaporating_temp_c"],
    "condensing_temperature_c": STANDARD_CYCLE["condensing_temp_c"],
    "superheat_k": STANDARD_CYCLE["superheat_k"],
    "subcooling_k": STANDARD_CYCLE["subcooling_k"],
    "compressor_isentropic_efficiency": STANDARD_CYCLE["compressor_isentropic_efficiency"],
}
TRANSCRITICAL_GENERALIZED_SPEC = {
    "cycle_case_id": "transcritical_generalized_cycle",
    "case_name": "-5 degC evaporating / 35 degC gas cooler / 9 MPa high side",
    "evaporating_temperature_c": TRANSCRITICAL_CO2_CYCLE["evaporating_temp_c"],
    "gas_cooler_outlet_temperature_c": TRANSCRITICAL_CO2_CYCLE["gas_cooler_outlet_temp_c"],
    "high_side_pressure_mpa": TRANSCRITICAL_CO2_CYCLE["high_side_pressure_mpa"],
    "superheat_k": TRANSCRITICAL_CO2_CYCLE["superheat_k"],
    "compressor_isentropic_efficiency": TRANSCRITICAL_CO2_CYCLE["compressor_isentropic_efficiency"],
}

MANIFEST_COLUMNS = [
    "queue_entry_id",
    "mol_id",
    "seed_id",
    "r_number",
    "missing_properties_json",
    "coolprop_alias_candidates_json",
    "refprop_alias_candidates_json",
    "queue_payload_json",
]

RESULT_COLUMNS = [
    "seed_id",
    "r_number",
    "property_name",
    "value",
    "value_num",
    "unit",
    "temperature",
    "pressure",
    "phase",
    "source_type",
    "source_name",
    "source_record_id",
    "method",
    "uncertainty",
    "quality_level",
    "assessment_version",
    "time_horizon",
    "year",
    "notes",
    "qc_status",
    "qc_flags",
    "cycle_case_id",
    "operating_point_hash",
    "operating_point_json",
    "cycle_model",
    "eos_source",
    "convergence_flag",
    "backend",
    "backend_fluid",
    "fluid_alias",
    "artifact_path",
    "artifact_sha256",
    "queue_entry_id",
    "mol_id",
]

ATTEMPT_COLUMNS = [
    "queue_entry_id",
    "mol_id",
    "backend",
    "candidate_alias",
    "backend_fluid",
    "attempt_type",
    "status",
    "detail",
    "artifact_path",
    "artifact_sha256",
]

BLOCKER_COLUMNS = [
    "queue_entry_id",
    "mol_id",
    "seed_id",
    "r_number",
    "backend",
    "status",
    "blocker_reason",
    "detail",
    "alias_candidates_json",
    "refprop_root",
]


@dataclass(slots=True)
class CycleRetryRun:
    """In-memory output bundle for a remaining-cycle retry run."""

    manifest: pd.DataFrame
    results: pd.DataFrame
    attempts: pd.DataFrame
    blockers: pd.DataFrame
    summary: dict[str, Any]


def build_run_cycle_retry_manifest(
    *,
    active_learning_queue: pd.DataFrame,
    molecule_core: pd.DataFrame,
    molecule_alias: pd.DataFrame,
    seed_catalog: pd.DataFrame | None = None,
    coolprop_aliases: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Build a retry manifest from open ``run_cycle`` active-learning rows only."""

    coolprop_aliases = coolprop_aliases or {}
    seed_catalog = seed_catalog if seed_catalog is not None else pd.DataFrame()
    if active_learning_queue.empty or "recommended_next_action" not in active_learning_queue.columns:
        return pd.DataFrame(columns=MANIFEST_COLUMNS)

    core_by_mol = _records_by_key(molecule_core, "mol_id")
    seeds_by_id = _records_by_key(seed_catalog, "seed_id")
    aliases_by_mol = _aliases_by_mol(molecule_alias)

    rows: list[dict[str, Any]] = []
    queue = active_learning_queue.fillna("")
    queue = queue.loc[queue["recommended_next_action"].astype(str).eq("run_cycle")]
    if "status" in queue.columns:
        queue = queue.loc[~queue["status"].astype(str).str.lower().isin({"completed", "rejected", "deferred"})]

    for record in queue.to_dict(orient="records"):
        mol_id = _clean(record.get("mol_id"))
        if not mol_id:
            continue
        payload = _json_object(record.get("payload_json"))
        core = core_by_mol.get(mol_id, {})
        seed_id = _first_clean(payload.get("seed_id"), record.get("seed_id"), core.get("seed_id"))
        seed = seeds_by_id.get(seed_id, {})
        r_number = _first_clean(payload.get("r_number"), record.get("r_number"), seed.get("r_number"))
        aliases = aliases_by_mol.get(mol_id, [])

        candidate_aliases = _candidate_aliases(
            seed=seed,
            aliases=aliases,
            r_number=r_number,
            payload=payload,
            coolprop_aliases=coolprop_aliases,
        )
        missing_properties = _missing_properties(payload)
        rows.append(
            {
                "queue_entry_id": _clean(record.get("queue_entry_id")),
                "mol_id": mol_id,
                "seed_id": seed_id,
                "r_number": r_number,
                "missing_properties_json": json.dumps(missing_properties, sort_keys=False),
                "coolprop_alias_candidates_json": json.dumps(candidate_aliases, sort_keys=False),
                "refprop_alias_candidates_json": json.dumps([_refprop_fluid(alias) for alias in candidate_aliases], sort_keys=False),
                "queue_payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True),
            }
        )

    return _ensure_columns(pd.DataFrame(rows), MANIFEST_COLUMNS)


def run_cycle_retry(
    manifest: pd.DataFrame,
    *,
    backend: BackendName = "auto",
    refprop_root: str | Path | None = None,
    artifact_dir: str | Path = DEFAULT_ARTIFACT_DIR,
    write_results: bool = False,
    results_path: str | Path = DEFAULT_RESULTS_PATH,
    props_si: PropsFunction = PropsSI,
) -> CycleRetryRun:
    """Run remaining cycle retries and return results, attempt log, blockers, and summary."""

    if backend not in {"auto", "coolprop", "refprop"}:
        raise ValueError("backend must be one of: auto, coolprop, refprop")

    manifest = _ensure_columns(manifest.copy(), MANIFEST_COLUMNS)
    artifact_dir = Path(artifact_dir)
    ensure_directory(artifact_dir)
    results_path = Path(results_path)

    resolved_refprop_root = Path(refprop_root).expanduser() if refprop_root else _env_refprop_root()
    refprop_probe = _probe_refprop_root(resolved_refprop_root)
    if refprop_probe["available"]:
        _configure_refprop_root(Path(refprop_probe["root"]))

    result_rows: list[dict[str, Any]] = []
    attempt_rows: list[dict[str, Any]] = []
    blocker_rows: list[dict[str, Any]] = []

    for record in manifest.fillna("").to_dict(orient="records"):
        row_success = False
        last_errors: list[str] = []
        for backend_name in _backend_order(backend):
            if backend_name == "refprop" and not refprop_probe["available"]:
                detail = refprop_probe["detail"]
                blocker_rows.append(_blocker_row(record, backend_name, refprop_probe["reason"], detail, refprop_probe.get("root", "")))
                last_errors.append(detail)
                continue

            candidates = _backend_candidates(record, backend_name)
            if not candidates:
                last_errors.append(f"{backend_name}:no_alias_candidates")
                continue

            for candidate in candidates:
                backend_fluid = _refprop_fluid(candidate) if backend_name == "refprop" else candidate
                try:
                    computed = _compute_backend_rows(
                        record=record,
                        backend_name=backend_name,
                        backend_fluid=backend_fluid,
                        fluid_alias=_strip_refprop_prefix(candidate),
                        props_si=props_si,
                    )
                    _require_targets_satisfied(record, computed)
                    artifact_path, artifact_sha = _write_result_artifact(
                        artifact_dir=artifact_dir,
                        record=record,
                        backend_name=backend_name,
                        backend_fluid=backend_fluid,
                        rows=computed,
                    )
                    for row in computed:
                        row["artifact_path"] = artifact_path
                        row["artifact_sha256"] = artifact_sha
                        row["notes"] = _join_notes(row.get("notes"), f"artifact_sha256={artifact_sha}")
                    result_rows.extend(computed)
                    attempt_rows.append(
                        _attempt_row(record, backend_name, candidate, backend_fluid, "compute", "succeeded", "", artifact_path, artifact_sha)
                    )
                    row_success = True
                    break
                except Exception as exc:  # noqa: BLE001 - failed aliases are expected in retry manifests
                    detail = f"{type(exc).__name__}: {exc}"
                    last_errors.append(f"{backend_name}:{candidate}:{detail}")
                    attempt_rows.append(_attempt_row(record, backend_name, candidate, backend_fluid, "compute", "failed", detail, "", ""))
            if row_success:
                break

        if not row_success and not _already_blocked(blocker_rows, record):
            blocker_rows.append(
                _blocker_row(
                    record,
                    backend if backend != "auto" else "auto",
                    "no_backend_candidate_succeeded",
                    "; ".join(last_errors),
                    refprop_probe.get("root", ""),
                )
            )

    results = _ensure_columns(pd.DataFrame(result_rows), RESULT_COLUMNS)
    attempts = _ensure_columns(pd.DataFrame(attempt_rows), ATTEMPT_COLUMNS)
    blockers = _ensure_columns(pd.DataFrame(blocker_rows), BLOCKER_COLUMNS)

    if write_results:
        ensure_directory(results_path.parent)
        results.to_csv(results_path, index=False)

    summary = {
        "run_at": now_iso(),
        "backend": backend,
        "queue_entry_count": int(len(manifest)),
        "result_row_count": int(len(results)),
        "attempt_count": int(len(attempts)),
        "blocker_count": int(len(blockers)),
        "succeeded_queue_entries": int(results["queue_entry_id"].nunique()) if not results.empty else 0,
        "blocked_queue_entries": int(blockers["queue_entry_id"].nunique()) if not blockers.empty else 0,
        "refprop_probe": refprop_probe,
        "results_path": str(results_path) if write_results else "",
        "artifact_dir": str(artifact_dir),
    }
    return CycleRetryRun(manifest=manifest, results=results, attempts=attempts, blockers=blockers, summary=summary)


def _compute_backend_rows(
    *,
    record: dict[str, Any],
    backend_name: str,
    backend_fluid: str,
    fluid_alias: str,
    props_si: PropsFunction,
) -> list[dict[str, Any]]:
    targets = _target_properties(record)
    rows: list[dict[str, Any]] = []
    for property_name in sorted(targets & set(TRANSPORT_PROPERTIES)):
        output_key, unit, label = TRANSPORT_PROPERTIES[property_name]
        value = props_si(output_key, "P", 101325.0, "Q", 0, backend_fluid)
        rows.append(
            _result_row(
                record=record,
                backend_name=backend_name,
                backend_fluid=backend_fluid,
                fluid_alias=fluid_alias,
                property_name=property_name,
                value_num=value,
                unit=unit,
                temperature="",
                pressure="0.101325 MPa",
                phase="saturated_liquid",
                method=f"{_backend_label(backend_name)} PropsSI({output_key}|P=101325 Pa,Q=0)",
                notes=f"resolved:{backend_name}:saturated_liquid_{label}",
            )
        )

    if targets & set(CYCLE_PROPERTIES):
        cycle = _compute_cycle_metrics(backend_fluid, backend_name=backend_name, props_si=props_si)
        for property_name, (metric_key, unit) in CYCLE_PROPERTIES.items():
            if property_name not in targets and "cop_standard_cycle" not in targets:
                continue
            rows.append(
                _result_row(
                    record=record,
                    backend_name=backend_name,
                    backend_fluid=backend_fluid,
                    fluid_alias=fluid_alias,
                    property_name=property_name,
                    value_num=cycle[metric_key],
                    unit=unit,
                    temperature=cycle["cycle_label"],
                    pressure=cycle["pressure_label"],
                    phase="cycle",
                    method=cycle["method"],
                    notes=cycle["status"],
                    cycle_case_id=cycle["cycle_case_id"],
                    operating_point_hash=cycle["operating_point_hash"],
                    operating_point_json=cycle["operating_point_json"],
                    cycle_model=cycle["cycle_model"],
                    eos_source=cycle["eos_source"],
                    convergence_flag=1,
                )
            )
    return rows


def _compute_cycle_metrics(backend_fluid: str, *, backend_name: str, props_si: PropsFunction) -> dict[str, Any]:
    tcrit = props_si("Tcrit", backend_fluid)
    tc_standard = float(STANDARD_SUBCRITICAL_SPEC["condensing_temperature_c"]) + 273.15
    if tcrit > tc_standard:
        try:
            return _subcritical_cycle(backend_fluid, backend_name=backend_name, props_si=props_si)
        except Exception:
            return _transcritical_cycle(backend_fluid, backend_name=backend_name, props_si=props_si)
    return _transcritical_cycle(backend_fluid, backend_name=backend_name, props_si=props_si)


def _subcritical_cycle(backend_fluid: str, *, backend_name: str, props_si: PropsFunction) -> dict[str, Any]:
    spec = STANDARD_SUBCRITICAL_SPEC
    te = float(spec["evaporating_temperature_c"]) + 273.15
    tc = float(spec["condensing_temperature_c"]) + 273.15
    sh = float(spec["superheat_k"])
    sc = float(spec["subcooling_k"])
    eta = float(spec["compressor_isentropic_efficiency"])

    pe = props_si("P", "T", te, "Q", 1, backend_fluid)
    pc = props_si("P", "T", tc, "Q", 0, backend_fluid)
    h1 = props_si("Hmass", "T", te + sh, "P", pe, backend_fluid)
    s1 = props_si("Smass", "T", te + sh, "P", pe, backend_fluid)
    rho1 = props_si("Dmass", "T", te + sh, "P", pe, backend_fluid)
    h2s = props_si("Hmass", "P", pc, "Smass", s1, backend_fluid)
    h2 = h1 + (h2s - h1) / eta
    h3 = props_si("Hmass", "T", tc - sc, "P", pc, backend_fluid)
    t2 = props_si("T", "P", pc, "Hmass", h2, backend_fluid)

    q_evap = h1 - h3
    w_comp = h2 - h1
    if q_evap <= 0 or w_comp <= 0:
        raise ValueError("non-positive evaporator heat or compressor work")

    op_hash, op_json = operating_point_hash(_operating_point_from_spec(spec))
    return {
        "cop": q_evap / w_comp,
        "qvol": q_evap * rho1 / 1e6,
        "pressure_ratio": pc / pe,
        "discharge_temperature_c": t2 - 273.15,
        "cycle_label": str(spec["case_name"]),
        "pressure_label": "",
        "method": f"{_backend_label(backend_name)} subcritical vapor-compression cycle",
        "status": f"resolved:{backend_name}:subcritical",
        "cycle_case_id": str(spec["cycle_case_id"]),
        "operating_point_hash": op_hash,
        "operating_point_json": op_json,
        "cycle_model": "subcritical_vapor_compression",
        "eos_source": _eos_source(backend_name),
    }


def _transcritical_cycle(backend_fluid: str, *, backend_name: str, props_si: PropsFunction) -> dict[str, Any]:
    spec = TRANSCRITICAL_GENERALIZED_SPEC
    te = float(spec["evaporating_temperature_c"]) + 273.15
    tg = float(spec["gas_cooler_outlet_temperature_c"]) + 273.15
    ph = float(spec["high_side_pressure_mpa"]) * 1e6
    sh = float(spec["superheat_k"])
    eta = float(spec["compressor_isentropic_efficiency"])

    pe = props_si("P", "T", te, "Q", 1, backend_fluid)
    h1 = props_si("Hmass", "T", te + sh, "P", pe, backend_fluid)
    s1 = props_si("Smass", "T", te + sh, "P", pe, backend_fluid)
    rho1 = props_si("Dmass", "T", te + sh, "P", pe, backend_fluid)
    h2s = props_si("Hmass", "P", ph, "Smass", s1, backend_fluid)
    h2 = h1 + (h2s - h1) / eta
    h3 = props_si("Hmass", "T", tg, "P", ph, backend_fluid)
    t2 = props_si("T", "P", ph, "Hmass", h2, backend_fluid)

    q_evap = h1 - h3
    w_comp = h2 - h1
    if q_evap <= 0 or w_comp <= 0:
        raise ValueError("non-positive evaporator heat or compressor work")

    op_hash, op_json = operating_point_hash(_operating_point_from_spec(spec))
    return {
        "cop": q_evap / w_comp,
        "qvol": q_evap * rho1 / 1e6,
        "pressure_ratio": ph / pe,
        "discharge_temperature_c": t2 - 273.15,
        "cycle_label": str(spec["case_name"]),
        "pressure_label": f"{float(spec['high_side_pressure_mpa']):g} MPa high side",
        "method": f"{_backend_label(backend_name)} generalized transcritical cycle",
        "status": f"resolved:{backend_name}:transcritical_generalized",
        "cycle_case_id": str(spec["cycle_case_id"]),
        "operating_point_hash": op_hash,
        "operating_point_json": op_json,
        "cycle_model": "transcritical_generalized",
        "eos_source": _eos_source(backend_name),
    }


def _result_row(
    *,
    record: dict[str, Any],
    backend_name: str,
    backend_fluid: str,
    fluid_alias: str,
    property_name: str,
    value_num: float,
    unit: str,
    temperature: str,
    pressure: str,
    phase: str,
    method: str,
    notes: str,
    cycle_case_id: str = "",
    operating_point_hash: str = "",
    operating_point_json: str = "",
    cycle_model: str = "",
    eos_source: str = "",
    convergence_flag: int | None = None,
) -> dict[str, Any]:
    queue_entry_id = _clean(record.get("queue_entry_id"))
    mol_id = _clean(record.get("mol_id"))
    source_record_id_parts = [queue_entry_id, backend_name, slugify(fluid_alias), property_name]
    if cycle_case_id:
        source_record_id_parts.append(cycle_case_id)
    return {
        "seed_id": _clean(record.get("seed_id")),
        "r_number": _clean(record.get("r_number")),
        "property_name": property_name,
        "value": f"{float(value_num):.8g}",
        "value_num": float(value_num),
        "unit": unit,
        "temperature": temperature,
        "pressure": pressure,
        "phase": phase,
        "source_type": "calculated_open_source",
        "source_name": _source_name(backend_name),
        "source_record_id": "_".join(part for part in source_record_id_parts if part),
        "method": method,
        "uncertainty": "",
        "quality_level": "computed_high" if backend_name == "refprop" else "calculated_open_source",
        "assessment_version": "",
        "time_horizon": "",
        "year": "",
        "notes": notes,
        "qc_status": "pass",
        "qc_flags": "",
        "cycle_case_id": cycle_case_id,
        "operating_point_hash": operating_point_hash,
        "operating_point_json": operating_point_json,
        "cycle_model": cycle_model,
        "eos_source": eos_source,
        "convergence_flag": convergence_flag,
        "backend": backend_name,
        "backend_fluid": backend_fluid,
        "fluid_alias": fluid_alias,
        "artifact_path": "",
        "artifact_sha256": "",
        "queue_entry_id": queue_entry_id,
        "mol_id": mol_id,
    }


def _target_properties(record: dict[str, Any]) -> set[str]:
    missing = _json_list(record.get("missing_properties_json")) or RUN_CYCLE_REQUIRED_PROPERTIES
    targets = set(missing)
    if targets & set(CYCLE_PROPERTIES):
        targets.update(CYCLE_PROPERTIES)
    return targets


def _require_targets_satisfied(record: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError("no numeric rows produced")
    produced = {row["property_name"] for row in rows if row.get("value_num") is not None}
    required = set(_json_list(record.get("missing_properties_json")) or RUN_CYCLE_REQUIRED_PROPERTIES)
    if "cop_standard_cycle" in required:
        required = (required - {"cop_standard_cycle"}) | set(CYCLE_PROPERTIES)
    missing = sorted(required - produced)
    if missing:
        raise ValueError(f"missing required output properties: {missing}")


def _write_result_artifact(
    *,
    artifact_dir: Path,
    record: dict[str, Any],
    backend_name: str,
    backend_fluid: str,
    rows: list[dict[str, Any]],
) -> tuple[str, str]:
    ensure_directory(artifact_dir)
    queue_entry_id = _clean(record.get("queue_entry_id")) or "queue_entry"
    mol_id = _clean(record.get("mol_id")) or "mol"
    path = artifact_dir / f"{slugify(queue_entry_id)}_{slugify(mol_id)}_{backend_name}.json"
    payload = {
        "created_at": now_iso(),
        "queue_entry": {key: _json_safe(value) for key, value in record.items()},
        "backend": backend_name,
        "backend_fluid": backend_fluid,
        "result_rows": [{key: _json_safe(value) for key, value in row.items() if key not in {"artifact_path", "artifact_sha256"}} for row in rows],
    }
    write_json(path, payload)
    return _relpath(path), sha256_file(path)


def _probe_refprop_root(root: Path | None) -> dict[str, Any]:
    if root is None:
        return {
            "available": False,
            "reason": "refprop_root_not_configured",
            "detail": "Set COOLPROP_REFPROP_ROOT or pass --refprop-root to enable REFPROP.",
            "root": "",
        }
    root = root.expanduser()
    if not root.exists() or not root.is_dir():
        return {
            "available": False,
            "reason": "refprop_root_missing",
            "detail": f"REFPROP root does not exist or is not a directory: {root}",
            "root": str(root),
        }
    fluids_dir = root / "FLUIDS"
    mixtures_dir = root / "MIXTURES"
    if not fluids_dir.is_dir():
        return {
            "available": False,
            "reason": "refprop_fluids_dir_missing",
            "detail": f"REFPROP FLUIDS directory is missing: {fluids_dir}",
            "root": str(root),
        }
    if not mixtures_dir.is_dir():
        return {
            "available": False,
            "reason": "refprop_mixtures_dir_missing",
            "detail": f"REFPROP MIXTURES directory is missing: {mixtures_dir}",
            "root": str(root),
        }
    library_names = _refprop_library_names()
    if library_names and not any((root / name).exists() for name in library_names):
        return {
            "available": False,
            "reason": "refprop_library_missing",
            "detail": f"REFPROP shared library not found under {root}; expected one of {library_names}",
            "root": str(root),
        }
    return {
        "available": True,
        "reason": "",
        "detail": "REFPROP root contains expected library and fluid directories.",
        "root": str(root),
    }


def _configure_refprop_root(root: Path) -> None:
    os.environ["COOLPROP_REFPROP_ROOT"] = str(root)
    if CP is not None and hasattr(CP, "set_config_string") and hasattr(CP, "ALTERNATIVE_REFPROP_PATH"):
        try:
            CP.set_config_string(CP.ALTERNATIVE_REFPROP_PATH, str(root))
        except Exception:  # noqa: BLE001 - environment variable still provides the official path hook
            pass


def _refprop_library_names() -> list[str]:
    system = platform.system().lower()
    machine = platform.machine().lower()
    if system == "linux":
        return ["librefprop.so"]
    if system == "darwin":
        return ["librefprop.dylib"]
    if system == "windows":
        if "32" in machine or machine in {"x86", "i386", "i686"}:
            return ["REFPROP.DLL"]
        return ["REFPRP64.DLL"]
    return []


def _backend_order(backend: BackendName) -> list[str]:
    if backend == "auto":
        return ["coolprop", "refprop"]
    return [backend]


def _backend_candidates(record: dict[str, Any], backend_name: str) -> list[str]:
    column = "refprop_alias_candidates_json" if backend_name == "refprop" else "coolprop_alias_candidates_json"
    return _json_list(record.get(column))


def _blocker_row(record: dict[str, Any], backend: str, reason: str, detail: str, refprop_root: Any) -> dict[str, Any]:
    aliases = record.get("refprop_alias_candidates_json") if backend == "refprop" else record.get("coolprop_alias_candidates_json")
    return {
        "queue_entry_id": _clean(record.get("queue_entry_id")),
        "mol_id": _clean(record.get("mol_id")),
        "seed_id": _clean(record.get("seed_id")),
        "r_number": _clean(record.get("r_number")),
        "backend": backend,
        "status": "blocked_on_external_backend" if str(reason).startswith("refprop_") else "failed",
        "blocker_reason": reason,
        "detail": detail,
        "alias_candidates_json": aliases or "[]",
        "refprop_root": _clean(refprop_root),
    }


def _attempt_row(
    record: dict[str, Any],
    backend: str,
    candidate: str,
    backend_fluid: str,
    attempt_type: str,
    status: str,
    detail: str,
    artifact_path: str,
    artifact_sha256: str,
) -> dict[str, Any]:
    return {
        "queue_entry_id": _clean(record.get("queue_entry_id")),
        "mol_id": _clean(record.get("mol_id")),
        "backend": backend,
        "candidate_alias": candidate,
        "backend_fluid": backend_fluid,
        "attempt_type": attempt_type,
        "status": status,
        "detail": detail,
        "artifact_path": artifact_path,
        "artifact_sha256": artifact_sha256,
    }


def _already_blocked(blockers: list[dict[str, Any]], record: dict[str, Any]) -> bool:
    queue_entry_id = _clean(record.get("queue_entry_id"))
    return any(_clean(blocker.get("queue_entry_id")) == queue_entry_id for blocker in blockers)


def _candidate_aliases(
    *,
    seed: dict[str, Any],
    aliases: list[dict[str, Any]],
    r_number: str,
    payload: dict[str, Any],
    coolprop_aliases: dict[str, str],
) -> list[str]:
    candidates: list[str] = []

    def add(value: Any) -> None:
        cleaned = _clean(value)
        if cleaned and cleaned not in candidates:
            candidates.append(cleaned)

    add(seed.get("coolprop_fluid"))
    for alias in aliases:
        if _clean(alias.get("alias_type")) == "coolprop_fluid":
            add(alias.get("alias_value"))

    r_numbers: list[str] = []
    for value in [payload.get("r_number"), r_number, seed.get("r_number")]:
        cleaned = _clean(value)
        if cleaned and cleaned not in r_numbers:
            r_numbers.append(cleaned)
    for alias in aliases:
        if _clean(alias.get("alias_type")) == "r_number":
            cleaned = _clean(alias.get("alias_value"))
            if cleaned and cleaned not in r_numbers:
                r_numbers.append(cleaned)

    for value in r_numbers:
        add(coolprop_aliases.get(value))
        add(value)
        if "-" in value:
            add(value.replace("-", ""))

    for alias in aliases:
        if _clean(alias.get("alias_type")) in {"synonym", "query_name"}:
            add(alias.get("alias_value"))
    return candidates


def _aliases_by_mol(molecule_alias: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    if molecule_alias.empty or "mol_id" not in molecule_alias.columns:
        return {}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in molecule_alias.fillna("").to_dict(orient="records"):
        grouped.setdefault(_clean(row.get("mol_id")), []).append(row)
    return grouped


def _records_by_key(frame: pd.DataFrame | None, key: str) -> dict[str, dict[str, Any]]:
    if frame is None or frame.empty or key not in frame.columns:
        return {}
    return {_clean(row.get(key)): row for row in frame.fillna("").to_dict(orient="records") if _clean(row.get(key))}


def _missing_properties(payload: dict[str, Any]) -> list[str]:
    raw = payload.get("missing_properties")
    if not isinstance(raw, list):
        return RUN_CYCLE_REQUIRED_PROPERTIES.copy()
    return [_clean(item) for item in raw if _clean(item)]


def _operating_point_from_spec(spec: dict[str, Any]) -> dict[str, Any]:
    return {
        "evaporating_temperature_c": spec.get("evaporating_temperature_c"),
        "condensing_temperature_c": spec.get("condensing_temperature_c"),
        "gas_cooler_outlet_temperature_c": spec.get("gas_cooler_outlet_temperature_c"),
        "high_side_pressure_mpa": spec.get("high_side_pressure_mpa"),
        "superheat_k": spec.get("superheat_k"),
        "subcooling_k": spec.get("subcooling_k"),
        "compressor_isentropic_efficiency": spec.get("compressor_isentropic_efficiency"),
    }


def _refprop_fluid(alias: str) -> str:
    alias = _strip_refprop_prefix(alias)
    return f"REFPROP::{alias}" if alias else ""


def _strip_refprop_prefix(alias: Any) -> str:
    text = _clean(alias)
    return text.removeprefix("REFPROP::")


def _source_name(backend_name: str) -> str:
    if backend_name == "refprop":
        return "NIST REFPROP via CoolProp"
    return "CoolProp cycle retry"


def _backend_label(backend_name: str) -> str:
    if backend_name == "refprop":
        return "REFPROP via CoolProp"
    return "CoolProp"


def _eos_source(backend_name: str) -> str:
    return "REFPROP" if backend_name == "refprop" else "CoolProp"


def _env_refprop_root() -> Path | None:
    value = _clean(os.environ.get("COOLPROP_REFPROP_ROOT"))
    return Path(value).expanduser() if value else None


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    text = _clean(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [_clean(item) for item in value if _clean(item)]
    text = _clean(value)
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [_clean(item) for item in parsed if _clean(item)]


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _first_clean(*values: Any) -> str:
    for value in values:
        cleaned = _clean(value)
        if cleaned:
            return cleaned
    return ""


def _join_notes(*values: Any) -> str:
    return "; ".join(_clean(value) for value in values if _clean(value))


def _clean(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def _relpath(path: Path) -> str:
    try:
        return str(path.relative_to(DATA_DIR.parents[0])).replace("\\", "/")
    except ValueError:
        return str(path).replace("\\", "/")


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in df.columns:
            df[column] = None
    return df[columns]
