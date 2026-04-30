"""Cycle operating point helpers for structured cycle observations."""

from __future__ import annotations

import hashlib
import json
from typing import Any

import pandas as pd

from r_physgen_db.constants import STANDARD_CYCLE, TRANSCRITICAL_CO2_CYCLE


CYCLE_PROPERTIES = {
    "cop_standard_cycle",
    "volumetric_cooling_mjm3",
    "pressure_ratio",
    "discharge_temperature_c",
    "cycle.cop",
    "cycle.volumetric_cooling_capacity",
    "cycle.pressure_ratio",
    "cycle.discharge_temperature",
}

CYCLE_CASE_COLUMNS = [
    "cycle_case_id",
    "cycle_model",
    "eos_source",
    "case_name",
    "operating_point_hash",
    "operating_point_json",
    "source_id",
    "source_name",
    "created_by_stage_id",
    "notes",
]

CYCLE_OPERATING_POINT_COLUMNS = [
    "operating_point_hash",
    "cycle_case_id",
    "evaporating_temperature_c",
    "condensing_temperature_c",
    "gas_cooler_outlet_temperature_c",
    "high_side_pressure_mpa",
    "superheat_k",
    "subcooling_k",
    "compressor_isentropic_efficiency",
    "operating_point_json",
]

OPERATING_POINT_FIELDS = [
    "evaporating_temperature_c",
    "condensing_temperature_c",
    "gas_cooler_outlet_temperature_c",
    "high_side_pressure_mpa",
    "superheat_k",
    "subcooling_k",
    "compressor_isentropic_efficiency",
]


def operating_point_hash(operating_point: dict[str, Any]) -> tuple[str, str]:
    signature = {field: _normalize_for_hash(operating_point.get(field)) for field in OPERATING_POINT_FIELDS}
    signature_json = json.dumps(signature, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(signature_json.encode("utf-8")).hexdigest()[:20]
    return f"op_{digest}", signature_json


def built_in_cycle_cases(*, source_id: str = "source_coolprop_session", source_name: str = "CoolProp") -> dict[str, dict[str, Any]]:
    cases: dict[str, dict[str, Any]] = {}
    standard_point = {
        "evaporating_temperature_c": STANDARD_CYCLE["evaporating_temp_c"],
        "condensing_temperature_c": STANDARD_CYCLE["condensing_temp_c"],
        "gas_cooler_outlet_temperature_c": None,
        "high_side_pressure_mpa": None,
        "superheat_k": STANDARD_CYCLE["superheat_k"],
        "subcooling_k": STANDARD_CYCLE["subcooling_k"],
        "compressor_isentropic_efficiency": STANDARD_CYCLE["compressor_isentropic_efficiency"],
    }
    cases["standard_subcritical_cycle"] = _cycle_case(
        cycle_case_id="standard_subcritical_cycle",
        cycle_model="subcritical_vapor_compression",
        eos_source="CoolProp",
        case_name="5 degC evaporating / 50 degC condensing",
        operating_point=standard_point,
        source_id=source_id,
        source_name=source_name,
        notes="P0 built-in CoolProp subcritical cycle",
    )

    transcritical_point = {
        "evaporating_temperature_c": TRANSCRITICAL_CO2_CYCLE["evaporating_temp_c"],
        "condensing_temperature_c": None,
        "gas_cooler_outlet_temperature_c": TRANSCRITICAL_CO2_CYCLE["gas_cooler_outlet_temp_c"],
        "high_side_pressure_mpa": TRANSCRITICAL_CO2_CYCLE["high_side_pressure_mpa"],
        "superheat_k": TRANSCRITICAL_CO2_CYCLE["superheat_k"],
        "subcooling_k": None,
        "compressor_isentropic_efficiency": TRANSCRITICAL_CO2_CYCLE["compressor_isentropic_efficiency"],
    }
    cases["transcritical_co2_cycle"] = _cycle_case(
        cycle_case_id="transcritical_co2_cycle",
        cycle_model="transcritical_co2",
        eos_source="CoolProp",
        case_name="-5 degC evaporating / 35 degC gas cooler / 9 MPa high side",
        operating_point=transcritical_point,
        source_id=source_id,
        source_name=source_name,
        notes="P0 built-in CoolProp transcritical CO2 cycle",
    )
    transcritical_generalized_point = {
        "evaporating_temperature_c": TRANSCRITICAL_CO2_CYCLE["evaporating_temp_c"],
        "condensing_temperature_c": None,
        "gas_cooler_outlet_temperature_c": TRANSCRITICAL_CO2_CYCLE["gas_cooler_outlet_temp_c"],
        "high_side_pressure_mpa": TRANSCRITICAL_CO2_CYCLE["high_side_pressure_mpa"],
        "superheat_k": TRANSCRITICAL_CO2_CYCLE["superheat_k"],
        "subcooling_k": None,
        "compressor_isentropic_efficiency": TRANSCRITICAL_CO2_CYCLE["compressor_isentropic_efficiency"],
    }
    cases["transcritical_generalized_cycle"] = _cycle_case(
        cycle_case_id="transcritical_generalized_cycle",
        cycle_model="transcritical_generalized",
        eos_source="REFPROP",
        case_name="-5 degC evaporating / 35 degC gas cooler / 9 MPa high side",
        operating_point=transcritical_generalized_point,
        source_id=source_id,
        source_name=source_name,
        notes="Generalized REFPROP transcritical cycle for fluids without a feasible standard subcritical condenser point",
    )
    return cases


def cycle_case_for_id(cycle_case_id: str, *, source_id: str = "source_coolprop_session", source_name: str = "CoolProp") -> dict[str, Any] | None:
    return built_in_cycle_cases(source_id=source_id, source_name=source_name).get(cycle_case_id)


def infer_cycle_context(row: dict[str, Any]) -> dict[str, Any]:
    if not _is_cycle_row(row):
        return {}

    cycle_case_id = _clean(row.get("cycle_case_id"))
    if not cycle_case_id:
        text = " ".join(
            [
                _clean(row.get("temperature")),
                _clean(row.get("method")),
                _clean(row.get("notes")),
            ]
        ).lower()
        if "transcritical" in text or "gas cooler" in text or "high side" in text:
            cycle_case_id = "transcritical_co2_cycle"
        else:
            cycle_case_id = "standard_subcritical_cycle"

    source_id = _clean(row.get("source_id")) or "source_coolprop_session"
    source_name = _clean(row.get("source_name")) or "CoolProp"
    case = cycle_case_for_id(cycle_case_id, source_id=source_id, source_name=source_name)
    if case is None:
        return {"cycle_case_id": cycle_case_id}
    return {
        "cycle_case_id": case["cycle_case_id"],
        "operating_point_hash": case["operating_point_hash"],
        "operating_point_json": case["operating_point_json"],
        "cycle_model": case["cycle_model"],
        "eos_source": case["eos_source"],
    }


def fill_cycle_observation_fields(observation: pd.DataFrame) -> pd.DataFrame:
    df = observation.copy()
    for column in ["cycle_case_id", "operating_point_hash", "cycle_model", "eos_source", "convergence_flag"]:
        if column not in df.columns:
            df[column] = None
    if df.empty:
        return df

    for idx, record in df.to_dict(orient="index").items():
        if not _is_cycle_row(record):
            continue
        unresolved = "cycle_unresolved" in _clean(record.get("qc_flags"))
        context = infer_cycle_context(record)
        if not unresolved:
            for column in ["cycle_case_id", "operating_point_hash", "cycle_model", "eos_source"]:
                if not _clean(df.at[idx, column]) and context.get(column):
                    df.at[idx, column] = context[column]
            if df.at[idx, "convergence_flag"] is None or pd.isna(df.at[idx, "convergence_flag"]):
                df.at[idx, "convergence_flag"] = 1
        else:
            if df.at[idx, "convergence_flag"] is None or pd.isna(df.at[idx, "convergence_flag"]):
                df.at[idx, "convergence_flag"] = 0
            if not _clean(df.at[idx, "eos_source"]):
                df.at[idx, "eos_source"] = "CoolProp"
    return df


def build_cycle_tables(property_observation: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if property_observation.empty:
        return (
            pd.DataFrame(columns=CYCLE_CASE_COLUMNS),
            pd.DataFrame(columns=CYCLE_OPERATING_POINT_COLUMNS),
            _cycle_summary(property_observation, 0, 0),
        )

    cycle_rows = property_observation.loc[property_observation.apply(lambda row: _is_cycle_row(row.to_dict()), axis=1)].copy()
    resolved = cycle_rows.loc[
        ~cycle_rows.get("qc_flags", pd.Series("", index=cycle_rows.index)).fillna("").astype(str).str.contains("cycle_unresolved", na=False)
    ].copy()

    cases: dict[str, dict[str, Any]] = {}
    operating_points: dict[str, dict[str, Any]] = {}
    for record in resolved.to_dict(orient="records"):
        context = infer_cycle_context(record)
        case_id = _clean(record.get("cycle_case_id")) or _clean(context.get("cycle_case_id"))
        op_hash = _clean(record.get("operating_point_hash")) or _clean(context.get("operating_point_hash"))
        if not case_id or not op_hash:
            continue
        source_id = _clean(record.get("source_id")) or "source_coolprop_session"
        source_name = _clean(record.get("source_name")) or "CoolProp"
        case = cycle_case_for_id(case_id, source_id=source_id, source_name=source_name)
        if case is None:
            operating_point_json = _clean(record.get("operating_point_json")) or _clean(context.get("operating_point_json"))
            case = {
                "cycle_case_id": case_id,
                "cycle_model": _clean(record.get("cycle_model")) or _clean(context.get("cycle_model")),
                "eos_source": _clean(record.get("eos_source")) or _clean(context.get("eos_source")),
                "case_name": case_id,
                "operating_point_hash": op_hash,
                "operating_point_json": operating_point_json,
                "source_id": source_id,
                "source_name": source_name,
                "created_by_stage_id": "05",
                "notes": "derived from cycle observation",
            }
        cases[case_id] = case
        operating_points[op_hash] = _operating_point_row(case)

    cycle_case = _ensure_columns(pd.DataFrame(cases.values()), CYCLE_CASE_COLUMNS)
    cycle_operating_point = _ensure_columns(pd.DataFrame(operating_points.values()), CYCLE_OPERATING_POINT_COLUMNS)
    return cycle_case, cycle_operating_point, _cycle_summary(cycle_rows, len(cycle_case), len(cycle_operating_point))


def _cycle_case(
    *,
    cycle_case_id: str,
    cycle_model: str,
    eos_source: str,
    case_name: str,
    operating_point: dict[str, Any],
    source_id: str,
    source_name: str,
    notes: str,
) -> dict[str, Any]:
    op_hash, op_json = operating_point_hash(operating_point)
    return {
        "cycle_case_id": cycle_case_id,
        "cycle_model": cycle_model,
        "eos_source": eos_source,
        "case_name": case_name,
        "operating_point_hash": op_hash,
        "operating_point_json": op_json,
        "source_id": source_id,
        "source_name": source_name,
        "created_by_stage_id": "05",
        "notes": notes,
    }


def _operating_point_row(case: dict[str, Any]) -> dict[str, Any]:
    try:
        point = json.loads(str(case.get("operating_point_json") or "{}"))
    except json.JSONDecodeError:
        point = {}
    row = {
        "operating_point_hash": case.get("operating_point_hash", ""),
        "cycle_case_id": case.get("cycle_case_id", ""),
        "operating_point_json": case.get("operating_point_json", ""),
    }
    row.update({field: point.get(field) for field in OPERATING_POINT_FIELDS})
    return row


def _cycle_summary(cycle_rows: pd.DataFrame, case_count: int, operating_point_count: int) -> dict[str, Any]:
    if cycle_rows.empty:
        return {
            "cycle_observation_count": 0,
            "resolved_cycle_observation_count": 0,
            "unresolved_cycle_observation_count": 0,
            "cycle_case_count": case_count,
            "cycle_operating_point_count": operating_point_count,
        }
    flags = cycle_rows.get("qc_flags", pd.Series("", index=cycle_rows.index)).fillna("").astype(str)
    unresolved_count = int(flags.str.contains("cycle_unresolved", na=False).sum())
    return {
        "cycle_observation_count": int(len(cycle_rows)),
        "resolved_cycle_observation_count": int(len(cycle_rows) - unresolved_count),
        "unresolved_cycle_observation_count": unresolved_count,
        "cycle_case_count": int(case_count),
        "cycle_operating_point_count": int(operating_point_count),
    }


def _is_cycle_row(row: dict[str, Any]) -> bool:
    prop = _clean(row.get("canonical_feature_key")) or _clean(row.get("property_name"))
    return prop in CYCLE_PROPERTIES or _clean(row.get("phase")).lower() == "cycle"


def _normalize_for_hash(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float):
        if pd.isna(value):
            return None
        return float(f"{value:.12g}")
    if isinstance(value, str):
        text = value.strip()
        return text if text else None
    return value


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
