#!/usr/bin/env python3
"""Backfill observation_condition_set for existing property_observation rows.

Draft utility for P0/PR-C. It is intentionally conservative:
- deterministic condition_set_id from canonical JSON;
- automatic rules only for low-risk condition roles;
- unresolved rows are flagged for manual review instead of guessed.

Usage:
  python scripts/backfill_condition_set.py \
      --property-observation data/lake/silver/property_observation.parquet \
      --out-observation data/lake/silver/property_observation.backfilled.parquet \
      --out-condition-set data/lake/silver/observation_condition_set.parquet \
      --report data/lake/gold/condition_migration_report.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd


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


def _clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _num(value: Any) -> float | None:
    text = _clean(value)
    if not text:
        return None
    m = re.search(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", text)
    if not m:
        return None
    return float(m.group(0))


def _canonicalize_condition(row: dict[str, Any]) -> dict[str, Any]:
    prop = _clean(row.get("canonical_feature_key")) or _clean(row.get("property_name"))
    temp_text = _clean(row.get("temperature"))
    pressure_text = _clean(row.get("pressure"))
    phase_text = _clean(row.get("phase")).lower()

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
        "mixture_composition_json": "",
        "cycle_case_id": _clean(row.get("cycle_case_id")),
        "operating_point_hash": _clean(row.get("operating_point_hash")),
        "reference_state": "",
        "source_condition_text": " | ".join(x for x in [temp_text, pressure_text, phase_text] if x),
        "normalization_status": "inferred_default",
        "parser_version": "p0_backfill_draft",
        "created_by_stage_id": "05",
        "notes": "",
    }

    if prop in {"boiling_point_c", "thermodynamic.normal_boiling_temperature", "boiling_point"}:
        condition["condition_role"] = "normal_boiling_point"
        condition["pressure_value"] = 0.101325
        condition["pressure_unit"] = "MPa"
        condition["phase"] = "vapor_liquid_equilibrium"
        return condition

    if prop.startswith("critical_") or prop.startswith("thermodynamic.critical_"):
        condition["condition_role"] = "critical_point"
        condition["phase"] = "critical_point"
        return condition

    if prop in {"cop_standard_cycle", "volumetric_cooling_mjm3", "cycle.cop", "cycle.volumetric_cooling_capacity"}:
        condition["condition_role"] = "cycle_operating_point"
        condition["phase"] = "cycle"
        condition["normalization_status"] = "partially_normalized"
        return condition

    if "298" in temp_text and ("gas" in phase_text or "vapor" in phase_text):
        condition["condition_role"] = "gas_phase_298k"
        condition["temperature_value"] = 298.15
        condition["temperature_unit"] = "K"
        condition["phase"] = "gas"
        return condition

    temp_value = _num(temp_text)
    if temp_value is not None:
        condition["temperature_value"] = temp_value
        condition["temperature_unit"] = "degC" if "c" in temp_text.lower() else "K"

    pressure_value = _num(pressure_text)
    if pressure_value is not None:
        condition["pressure_value"] = pressure_value
        lower = pressure_text.lower()
        if "mpa" in lower:
            condition["pressure_unit"] = "MPa"
        elif "kpa" in lower:
            condition["pressure_unit"] = "kPa"
        elif "bar" in lower:
            condition["pressure_unit"] = "bar"
        elif "pa" in lower:
            condition["pressure_unit"] = "Pa"

    if temp_value is not None or pressure_value is not None or phase_text:
        condition["condition_role"] = "standard_reference_state"
        condition["normalization_status"] = "partially_normalized"
    else:
        condition["normalization_status"] = "unresolved_text"
        condition["notes"] = "manual_review_required"

    return condition


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


def condition_id(condition: dict[str, Any]) -> tuple[str, str]:
    signature = {field: _normalize_for_hash(condition.get(field)) for field in HASH_FIELDS}
    if signature.get("mixture_composition_json"):
        try:
            parsed = json.loads(signature["mixture_composition_json"])
            signature["mixture_composition_json"] = {
                str(k): float(f"{float(v):.12g}") for k, v in sorted(parsed.items())
            }
        except Exception:
            pass
    signature_json = json.dumps(signature, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(signature_json.encode("utf-8")).hexdigest()[:20]
    return f"cond_{digest}", signature_json


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--property-observation", required=True)
    parser.add_argument("--out-observation", required=True)
    parser.add_argument("--out-condition-set", required=True)
    parser.add_argument("--report", required=True)
    args = parser.parse_args()

    obs = pd.read_parquet(args.property_observation).fillna("")
    conditions: dict[str, dict[str, Any]] = {}
    condition_ids: list[str] = []
    statuses: list[str] = []

    for record in obs.to_dict(orient="records"):
        cond = _canonicalize_condition(record)
        cid, signature_json = condition_id(cond)
        cond["condition_set_id"] = cid
        cond["condition_signature_json"] = signature_json
        conditions[cid] = cond
        condition_ids.append(cid)
        statuses.append(cond.get("normalization_status", ""))

    obs = obs.copy()
    obs["condition_set_id"] = condition_ids

    condition_df = pd.DataFrame(conditions.values())
    Path(args.out_observation).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_condition_set).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    obs.to_parquet(args.out_observation, index=False)
    condition_df.to_parquet(args.out_condition_set, index=False)

    total = int(len(obs))
    unresolved = int(sum(s == "unresolved_text" for s in statuses))
    report = {
        "total_rows": total,
        "with_condition_set_id": total,
        "condition_set_count": int(len(condition_df)),
        "unresolved_text": unresolved,
        "auto_or_partial_backfilled": total - unresolved,
        "unresolved_fraction": unresolved / total if total else 0.0,
    }
    Path(args.report).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
