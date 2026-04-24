"""Condition-set backfill helpers for production property observations."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

import pandas as pd

from r_physgen_db.constants import PARSER_VERSION
from r_physgen_db.cycle_conditions import CYCLE_PROPERTIES, fill_cycle_observation_fields, infer_cycle_context
from r_physgen_db.proxy_features import PROXY_FEATURE_KEYS
from r_physgen_db.quantum_pilot import QUANTUM_FEATURE_KEYS


HASH_FIELDS = [
    "condition_role",
    "temperature_value",
    "temperature_unit",
    "pressure_value",
    "pressure_unit",
    "phase",
    "vapor_quality_value",
    "vapor_quality_basis",
    "composition_basis",
    "mixture_composition_json",
    "cycle_case_id",
    "operating_point_hash",
    "reference_state",
]

CONDITION_SET_COLUMNS = [
    "condition_set_id",
    "condition_signature_json",
    "condition_role",
    "temperature_value",
    "temperature_unit",
    "pressure_value",
    "pressure_unit",
    "phase",
    "vapor_quality_value",
    "vapor_quality_basis",
    "composition_basis",
    "composition_value",
    "mixture_composition_json",
    "mixture_composition_hash",
    "cycle_case_id",
    "operating_point_hash",
    "reference_state",
    "source_condition_text",
    "normalization_status",
    "parser_version",
    "created_by_stage_id",
    "notes",
]

PRC_OBSERVATION_COLUMNS = [
    "value_text_normalized",
    "value_num_lower",
    "value_num_upper",
    "value_num_bound_type",
    "value_parse_status",
    "standard_value_num",
    "condition_set_id",
    "source_record_id",
    "ingestion_stage_id",
    "normalization_rule_id",
    "cycle_case_id",
    "operating_point_hash",
    "cycle_model",
    "eos_source",
    "convergence_flag",
]


def condition_id(condition: dict[str, Any]) -> tuple[str, str]:
    """Return the stable condition id and canonical signature JSON."""

    signature = {field: _normalize_for_hash(condition.get(field)) for field in HASH_FIELDS}
    if signature.get("mixture_composition_json"):
        try:
            parsed = json.loads(str(signature["mixture_composition_json"]))
            signature["mixture_composition_json"] = {
                str(key): float(f"{float(value):.12g}") for key, value in sorted(parsed.items())
            }
        except Exception:
            pass
    signature_json = json.dumps(signature, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(signature_json.encode("utf-8")).hexdigest()[:20]
    return f"cond_{digest}", signature_json


def backfill_condition_sets(
    property_observation: pd.DataFrame,
    *,
    created_by_stage_id: str = "05",
    parser_version: str = PARSER_VERSION,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Backfill nullable PR-C fields plus the condition-set dimension table."""

    observation = property_observation.copy()
    observation = _ensure_prc_columns(observation, created_by_stage_id=created_by_stage_id)
    observation = fill_cycle_observation_fields(observation)
    conditions: dict[str, dict[str, Any]] = {}
    condition_ids: list[str] = []
    statuses: list[str] = []

    for record in observation.to_dict(orient="records"):
        condition = canonicalize_condition(record, created_by_stage_id=created_by_stage_id, parser_version=parser_version)
        condition_set_id, signature_json = condition_id(condition)
        condition["condition_set_id"] = condition_set_id
        condition["condition_signature_json"] = signature_json
        conditions[condition_set_id] = condition
        condition_ids.append(condition_set_id)
        statuses.append(_clean_str(condition.get("normalization_status")))

    observation["condition_set_id"] = condition_ids
    condition_set = pd.DataFrame(conditions.values())
    condition_set = _ensure_columns(condition_set, CONDITION_SET_COLUMNS)

    total = int(len(observation))
    with_condition_set_id = int(observation["condition_set_id"].fillna("").astype(str).str.len().gt(0).sum()) if total else 0
    needs_manual_review = int(sum(status == "unresolved_text" for status in statuses))
    status_counts = pd.Series(statuses, dtype="object").value_counts().sort_index().to_dict()
    manual_review_condition_ids = (
        condition_set.loc[
            condition_set["normalization_status"].fillna("").astype(str).eq("unresolved_text"),
            "condition_set_id",
        ]
        .fillna("")
        .astype(str)
        .tolist()
    )
    progress = {
        "total": total,
        "with_condition_set_id": with_condition_set_id,
        "auto_backfilled": int(total - needs_manual_review),
        "needs_manual_review": needs_manual_review,
        "condition_set_count": int(len(condition_set)),
        "manual_review_condition_set_count": int(len(manual_review_condition_ids)),
        "manual_review_condition_set_ids_sample": manual_review_condition_ids[:10],
        "normalization_status_counts": {str(key): int(value) for key, value in status_counts.items()},
        "coverage_fraction": with_condition_set_id / total if total else 0.0,
        "auto_backfill_fraction": (total - needs_manual_review) / total if total else 0.0,
    }
    return observation, condition_set, progress


def canonicalize_condition(
    row: dict[str, Any],
    *,
    created_by_stage_id: str = "05",
    parser_version: str = PARSER_VERSION,
) -> dict[str, Any]:
    """Build the normalized condition record used for condition_set_id hashing."""

    prop = _clean_str(row.get("canonical_feature_key")) or _clean_str(row.get("property_name"))
    temp_text = _clean_str(row.get("temperature"))
    pressure_text = _clean_str(row.get("pressure"))
    phase_text = _normalize_phase(_clean_str(row.get("phase")))
    source_condition_text = " | ".join(text for text in [temp_text, pressure_text, phase_text] if text)

    condition: dict[str, Any] = {
        "condition_role": "unspecified",
        "temperature_value": None,
        "temperature_unit": "",
        "pressure_value": None,
        "pressure_unit": "",
        "phase": phase_text or "unspecified",
        "vapor_quality_value": None,
        "vapor_quality_basis": "",
        "composition_basis": "pure",
        "composition_value": None,
        "mixture_composition_json": "",
        "mixture_composition_hash": "",
        "cycle_case_id": _clean_str(row.get("cycle_case_id")),
        "operating_point_hash": _clean_str(row.get("operating_point_hash")),
        "reference_state": "",
        "source_condition_text": source_condition_text,
        "normalization_status": "inferred_default",
        "parser_version": parser_version,
        "created_by_stage_id": created_by_stage_id,
        "notes": "",
    }

    if prop in {"boiling_point_c", "boiling_point", "thermodynamic.normal_boiling_temperature"}:
        condition["condition_role"] = "normal_boiling_point"
        condition["pressure_value"] = 0.101325
        condition["pressure_unit"] = "MPa"
        condition["phase"] = "vapor_liquid_equilibrium"
        return condition

    if prop.startswith("critical_") or prop.startswith("thermodynamic.critical_"):
        condition["condition_role"] = "critical_point"
        condition["phase"] = "supercritical"
        return condition

    if prop in PROXY_FEATURE_KEYS:
        condition["condition_role"] = "standard_reference_state"
        condition["phase"] = "unspecified"
        condition["normalization_status"] = "not_applicable"
        condition["notes"] = "screening_proxy_condition_not_applicable"
        return condition

    if prop in QUANTUM_FEATURE_KEYS:
        condition["condition_role"] = "gas_phase_298k"
        condition["temperature_value"] = 298.15
        condition["temperature_unit"] = "K"
        condition["phase"] = "gas"
        condition["reference_state"] = "gas_phase_298k"
        return condition

    if prop in CYCLE_PROPERTIES:
        context = infer_cycle_context(row)
        condition["condition_role"] = "cycle_operating_point"
        condition["phase"] = "cycle"
        condition["normalization_status"] = "partially_normalized"
        condition["cycle_case_id"] = condition["cycle_case_id"] or _clean_str(context.get("cycle_case_id"))
        condition["operating_point_hash"] = condition["operating_point_hash"] or _clean_str(context.get("operating_point_hash"))
        if not condition["cycle_case_id"]:
            condition["cycle_case_id"] = "standard_subcritical_cycle"
        return condition

    if "298" in temp_text and ("gas" in phase_text or "vapor" in phase_text):
        condition["condition_role"] = "gas_phase_298k"
        condition["temperature_value"] = 298.15
        condition["temperature_unit"] = "K"
        condition["phase"] = "gas"
        return condition

    temp_value = _first_number(temp_text)
    if temp_value is not None:
        condition["temperature_value"] = temp_value
        condition["temperature_unit"] = _temperature_unit(temp_text)

    pressure_value = _first_number(pressure_text)
    if pressure_value is not None:
        condition["pressure_value"] = pressure_value
        condition["pressure_unit"] = _pressure_unit(pressure_text)

    if temp_value is not None or pressure_value is not None or phase_text:
        condition["condition_role"] = "standard_reference_state"
        condition["normalization_status"] = "partially_normalized"
    else:
        condition["normalization_status"] = "unresolved_text"
        condition["notes"] = "manual_review_required"
    return condition


def _ensure_prc_columns(df: pd.DataFrame, *, created_by_stage_id: str) -> pd.DataFrame:
    for column in PRC_OBSERVATION_COLUMNS:
        if column not in df.columns:
            df[column] = None

    value_text = df["value"].map(_clean_str) if "value" in df.columns else pd.Series([""] * len(df), index=df.index)
    df["value_text_normalized"] = df["value_text_normalized"].fillna(value_text)
    if "value_num" in df.columns:
        numeric_values = pd.to_numeric(df["value_num"], errors="coerce")
    else:
        numeric_values = pd.Series([float("nan")] * len(df), index=df.index)

    parsed_mask = numeric_values.notna()
    df.loc[parsed_mask & df["value_num_lower"].isna(), "value_num_lower"] = numeric_values[parsed_mask]
    df.loc[parsed_mask & df["value_num_upper"].isna(), "value_num_upper"] = numeric_values[parsed_mask]
    df.loc[parsed_mask & df["standard_value_num"].isna(), "standard_value_num"] = numeric_values[parsed_mask]
    df.loc[parsed_mask & df["value_num_bound_type"].isna(), "value_num_bound_type"] = "exact"
    df.loc[parsed_mask & df["value_parse_status"].isna(), "value_parse_status"] = "parsed_exact"
    df.loc[~parsed_mask & df["value_num_bound_type"].isna(), "value_num_bound_type"] = "text_only"
    df.loc[~parsed_mask & df["value_parse_status"].isna(), "value_parse_status"] = "text_only"

    if "bundle_record_id" in df.columns:
        df["source_record_id"] = df["source_record_id"].fillna(df["bundle_record_id"])
    if "observation_id" in df.columns:
        df["source_record_id"] = df["source_record_id"].fillna(df["observation_id"])
    df["ingestion_stage_id"] = df["ingestion_stage_id"].fillna(created_by_stage_id)
    return df


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in df.columns:
            df[column] = None
    return df[columns]


def _clean_str(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()


def _first_number(value: Any) -> float | None:
    text = _clean_str(value)
    if not text:
        return None
    match = re.search(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", text)
    if not match:
        return None
    return float(match.group(0))


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


def _normalize_phase(value: str) -> str:
    text = value.lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "vapor_liquid_equilibrium": "vapor_liquid_equilibrium",
        "vapour_liquid_equilibrium": "vapor_liquid_equilibrium",
        "vle": "vapor_liquid_equilibrium",
        "vapour": "vapor",
        "gas_phase": "gas",
        "liquid_phase": "liquid",
    }
    return aliases.get(text, text)


def _temperature_unit(text: str) -> str:
    lower = text.lower()
    if "degc" in lower or " c" in lower or lower.endswith("c") or "celsius" in lower:
        return "degC"
    return "K"


def _pressure_unit(text: str) -> str:
    lower = text.lower()
    if "mpa" in lower:
        return "MPa"
    if "kpa" in lower:
        return "kPa"
    if "bar" in lower:
        return "bar"
    if "pa" in lower:
        return "Pa"
    return ""
