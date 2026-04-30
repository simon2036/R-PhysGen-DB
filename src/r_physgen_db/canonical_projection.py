"""Native canonical projection helpers for non-governance recommendations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from r_physgen_db.constants import QUALITY_SCORES, SCHEMA_DIR
from r_physgen_db.proxy_features import PROXY_PROPERTIES, PROXY_SOURCE_ID
from r_physgen_db.sources.property_governance_bundle import (
    _canonical_recommended_columns,
    _canonical_recommended_strict_columns,
    select_canonical_recommended_strict,
)
from r_physgen_db.utils import load_yaml


@dataclass(slots=True)
class CanonicalProjectionBuild:
    canonical_recommended: pd.DataFrame
    canonical_recommended_strict: pd.DataFrame
    native_projection_rows: pd.DataFrame
    added_count: int
    summary: dict[str, Any]


def project_native_canonical_recommendations(
    *,
    property_recommended: pd.DataFrame,
    existing_canonical_recommended: pd.DataFrame | None = None,
    readiness_rules: pd.DataFrame | None = None,
    existing_canonical_recommended_strict: pd.DataFrame | None = None,
) -> CanonicalProjectionBuild:
    """Supplement governance canonical recommendations from native recommended rows.

    Existing governance rows win for an existing ``(mol_id, canonical_feature_key)`` pair.
    Native rows only fill gaps and keep their original selected source traceability.
    """

    existing = _ensure_columns(existing_canonical_recommended, _canonical_recommended_columns())
    existing_strict = _ensure_columns(existing_canonical_recommended_strict, _canonical_recommended_strict_columns())
    readiness = readiness_rules.copy() if readiness_rules is not None else pd.DataFrame()
    mapping = _legacy_property_mapping(readiness)

    native = _native_recommended_rows(property_recommended, mapping)
    if not existing.empty and not native.empty:
        existing_keys = set(zip(existing["mol_id"].astype(str), existing["canonical_feature_key"].astype(str), strict=True))
        native = native.loc[
            ~native.apply(lambda row: (str(row["mol_id"]), str(row["canonical_feature_key"])) in existing_keys, axis=1)
        ].copy()

    combined = _ensure_columns(
        pd.concat([existing, native], ignore_index=True) if not native.empty else existing,
        _canonical_recommended_columns(),
    )
    if not combined.empty:
        combined = combined.sort_values(["mol_id", "canonical_feature_key"], kind="stable").reset_index(drop=True)

    native_strict = (
        select_canonical_recommended_strict(canonical_recommended=native, readiness_rules=readiness)
        if not native.empty
        else pd.DataFrame(columns=_canonical_recommended_strict_columns())
    )
    strict = _ensure_columns(
        pd.concat([existing_strict, native_strict], ignore_index=True) if not native_strict.empty else existing_strict,
        _canonical_recommended_strict_columns(),
    )
    if not strict.empty:
        strict = (
            strict.drop_duplicates(subset=["mol_id", "canonical_feature_key"], keep="first")
            .sort_values(["mol_id", "canonical_feature_key"], kind="stable")
            .reset_index(drop=True)
        )

    summary = {
        "native_projection_candidate_count": int(len(_native_recommended_rows(property_recommended, mapping))),
        "native_projection_added_count": int(len(native)),
        "native_projection_strict_added_count": int(len(native_strict)),
        "canonical_recommended_count": int(len(combined)),
        "canonical_recommended_strict_count": int(len(strict)),
    }
    return CanonicalProjectionBuild(
        canonical_recommended=combined,
        canonical_recommended_strict=strict,
        native_projection_rows=native.reset_index(drop=True),
        added_count=int(len(native)),
        summary=summary,
    )


def _native_recommended_rows(property_recommended: pd.DataFrame, mapping: dict[str, dict[str, str]]) -> pd.DataFrame:
    if property_recommended is None or property_recommended.empty:
        return pd.DataFrame(columns=_canonical_recommended_columns())
    rows: list[dict[str, Any]] = []
    for record in property_recommended.fillna("").to_dict(orient="records"):
        property_name = str(record.get("property_name", "")).strip()
        item = mapping.get(property_name)
        if item is None:
            continue
        mol_id = str(record.get("mol_id", "")).strip()
        source_id = str(record.get("selected_source_id", "")).strip()
        if not mol_id or not source_id:
            continue
        value = str(record.get("value", "")).strip()
        value_num = _canonical_value_num(item["canonical_feature_key"], record)
        if not value and pd.isna(value_num):
            continue
        quality_level = str(record.get("selected_quality_level", "")).strip() or "derived_harmonized"
        quality_score = _quality_score_100(quality_level)
        source_count = _int_value(record.get("source_count"), 1)
        source_priority = _int_value(record.get("source_priority"), 0)
        source_priority_rank = _source_priority_rank(source_priority)
        is_proxy = bool(source_id == PROXY_SOURCE_ID or property_name in PROXY_PROPERTIES)
        rows.append(
            {
                "mol_id": mol_id,
                "canonical_feature_key": item["canonical_feature_key"],
                "canonical_property_id": item["canonical_property_id"],
                "canonical_property_group": item["canonical_property_group"],
                "canonical_property_name": item["canonical_property_name"],
                "value": value if value else ("" if pd.isna(value_num) else f"{float(value_num):.12g}"),
                "value_num": None if pd.isna(value_num) else float(value_num),
                "unit": str(record.get("unit", "")).strip() or item["unit"],
                "selected_source_id": source_id,
                "selected_source_name": str(record.get("selected_source_name", "")).strip(),
                "selected_quality_level": quality_level,
                "source_priority_rank": source_priority_rank,
                "data_quality_score_100": quality_score,
                "is_proxy_or_screening": is_proxy,
                "ml_use_status": "candidate_categorical_feature" if pd.isna(value_num) else "recommended_numeric_candidate",
                "proxy_only_flag": is_proxy,
                "nonproxy_candidate_count": 0 if is_proxy else 1,
                "top_rank_source_count": max(source_count, 1),
                "source_divergence_flag": False,
                "source_divergence_detail": "",
                "source_count": max(source_count, 1),
                "conflict_flag": bool(record.get("conflict_flag", False)),
                "conflict_detail": str(record.get("conflict_detail", "")).strip(),
            }
        )
    return _ensure_columns(pd.DataFrame(rows), _canonical_recommended_columns())


def _legacy_property_mapping(readiness_rules: pd.DataFrame) -> dict[str, dict[str, str]]:
    registry = load_yaml(SCHEMA_DIR / "canonical_feature_registry.yaml").get("registry", [])
    readiness_lookup: dict[str, dict[str, str]] = {}
    if readiness_rules is not None and not readiness_rules.empty:
        for row in readiness_rules.fillna("").to_dict(orient="records"):
            key = str(row.get("canonical_feature_key", "")).strip()
            if key:
                readiness_lookup[key] = {
                    "canonical_property_id": str(row.get("canonical_property_id", "")).strip(),
                    "preferred_standard_unit": str(row.get("preferred_standard_unit", "")).strip(),
                }

    mapping: dict[str, dict[str, str]] = {}
    for item in registry:
        key = str(item.get("canonical_feature_key", "")).strip()
        legacy = str(item.get("legacy_property_name", "")).strip()
        if not key or not legacy:
            continue
        readiness_item = readiness_lookup.get(key, {})
        canonical_property_id = readiness_item.get("canonical_property_id") or _fallback_property_id(key)
        mapping[legacy] = {
            "canonical_feature_key": key,
            "canonical_property_id": canonical_property_id,
            "canonical_property_group": key.split(".", 1)[0],
            "canonical_property_name": key.split(".", 1)[1] if "." in key else key,
            "unit": str(item.get("preferred_standard_unit", "")).strip(),
        }
        aliases_json = str(item.get("aliases_json", "")).strip()
        if aliases_json:
            try:
                import json

                for alias in json.loads(aliases_json):
                    alias_text = str(alias).strip()
                    if alias_text:
                        mapping.setdefault(alias_text, mapping[legacy])
            except Exception:
                pass
    return mapping


def _canonical_value_num(canonical_feature_key: str, record: dict[str, Any]) -> float | pd._libs.missing.NAType:
    numeric = pd.to_numeric(pd.Series([record.get("value_num")]), errors="coerce").iloc[0]
    if pd.notna(numeric):
        return float(numeric)
    if canonical_feature_key == "safety.safety_group":
        return _safety_group_numeric(str(record.get("value", "")).strip())
    return numeric


def _safety_group_numeric(value: str) -> float | pd._libs.missing.NAType:
    mapping = {
        "A1": 1.0,
        "B1": 1.0,
        "A2": 2.0,
        "A2L": 2.0,
        "B2": 2.0,
        "B2L": 2.0,
        "A3": 3.0,
        "B3": 3.0,
    }
    return mapping.get(value.upper(), pd.NA)


def _quality_score_100(quality_level: str) -> float:
    return float(QUALITY_SCORES.get(quality_level, QUALITY_SCORES.get("derived_harmonized", 0.85)) * 100.0)


def _source_priority_rank(source_priority: int) -> int:
    if source_priority <= 0:
        return 9999
    return max(1, 1000 - int(source_priority))


def _fallback_property_id(canonical_feature_key: str) -> str:
    return "NATIVE_" + canonical_feature_key.upper().replace(".", "_")


def _int_value(value: Any, default: int) -> int:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return default
    return int(numeric)


def _ensure_columns(df: pd.DataFrame | None, columns: list[str]) -> pd.DataFrame:
    if df is None:
        df = pd.DataFrame()
    out = df.copy()
    for column in columns:
        if column not in out.columns:
            out[column] = None
    return out[columns]
