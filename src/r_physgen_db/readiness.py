"""Research-task readiness evaluation for PR-C validation."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from r_physgen_db.constants import DATA_DIR, SCHEMA_DIR
from r_physgen_db.proxy_features import PROXY_DATA_QUALITY_SCORE, PROXY_PROPERTIES
from r_physgen_db.utils import load_yaml


READINESS_COLUMNS = [
    "readiness_rule_id",
    "task_name",
    "task_scope",
    "status",
    "molecule_count",
    "minimum_molecule_count",
    "must_have_passed",
    "should_have_average_coverage",
    "minimum_should_have_coverage",
    "failed_feature_keys_json",
    "hard_failures_json",
    "warnings_json",
    "must_have_coverage_json",
    "should_have_coverage_json",
    "source_layer",
    "evaluated_at",
]


def validate_readiness_rule_references(*, schema_dir: Path = SCHEMA_DIR) -> dict[str, Any]:
    registry = load_canonical_feature_registry(schema_dir=schema_dir)
    rules = load_readiness_rules(schema_dir=schema_dir)
    registry_keys = set(registry["canonical_feature_key"].astype(str).tolist())
    missing: list[dict[str, str]] = []
    invalid_filters: list[dict[str, str]] = []
    for rule in rules:
        for item in rule.get("must_have", []) + rule.get("should_have", []):
            key = str(item.get("canonical_feature_key", ""))
            if key not in registry_keys:
                missing.append({"readiness_rule_id": str(rule.get("readiness_rule_id", "")), "canonical_feature_key": key})
        model_filter = _rule_text(rule.get("model_inclusion_filter", "any"), default="any")
        if model_filter not in {"any", "yes", "no"}:
            invalid_filters.append({"readiness_rule_id": str(rule.get("readiness_rule_id", "")), "filter": "model_inclusion_filter"})
        entity_filter = _rule_text(rule.get("entity_scope_filter", "any"), default="any")
        if entity_filter not in {"any", "refrigerant", "candidate", "refrigerant_or_candidate"}:
            invalid_filters.append({"readiness_rule_id": str(rule.get("readiness_rule_id", "")), "filter": "entity_scope_filter"})
        invalid_tiers = sorted(set(_rule_list(rule.get("coverage_tier_filter"))) - {"A", "B", "C", "D"})
        if invalid_tiers:
            invalid_filters.append(
                {
                    "readiness_rule_id": str(rule.get("readiness_rule_id", "")),
                    "filter": f"coverage_tier_filter={','.join(invalid_tiers)}",
                }
            )
    return {
        "valid": not missing and not invalid_filters,
        "missing_references": missing,
        "invalid_filters": invalid_filters,
        "rule_count": len(rules),
        "registry_key_count": len(registry_keys),
    }


def evaluate_research_task_readiness_from_paths(
    *,
    data_dir: Path = DATA_DIR,
    schema_dir: Path = SCHEMA_DIR,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    frames = {
        "molecule_core": _read_parquet(data_dir / "silver" / "molecule_core.parquet"),
        "property_recommended": _read_parquet(data_dir / "gold" / "property_recommended.parquet"),
        "property_recommended_canonical": _read_parquet(data_dir / "gold" / "property_recommended_canonical.parquet"),
        "property_recommended_canonical_strict": _read_parquet(data_dir / "gold" / "property_recommended_canonical_strict.parquet"),
        "model_ready": _read_parquet(data_dir / "gold" / "model_ready.parquet"),
        "seed_catalog": _read_csv(data_dir / "raw" / "manual" / "seed_catalog.csv"),
    }
    return evaluate_research_task_readiness(frames=frames, schema_dir=schema_dir)


def evaluate_research_task_readiness(
    *,
    frames: dict[str, pd.DataFrame],
    schema_dir: Path = SCHEMA_DIR,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    registry = load_canonical_feature_registry(schema_dir=schema_dir)
    rules = load_readiness_rules(schema_dir=schema_dir)
    feature_aliases = _feature_aliases(registry)
    evaluated_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []

    for rule in rules:
        universe, universe_filter_failures = _eligible_molecule_ids(rule, frames)
        long_values = _values_for_rule(rule, frames, feature_aliases, universe)
        row = _evaluate_rule(rule, universe, long_values, universe_filter_failures, evaluated_at=evaluated_at)
        rows.append(row)

    report_df = _ensure_columns(pd.DataFrame(rows), READINESS_COLUMNS)
    summary = {
        "rule_count": int(len(report_df)),
        "status_counts": report_df["status"].value_counts(dropna=False).to_dict() if not report_df.empty else {},
        "failed_rule_ids": report_df.loc[report_df["status"] == "failed", "readiness_rule_id"].tolist() if not report_df.empty else [],
        "degraded_rule_ids": report_df.loc[report_df["status"] == "degraded", "readiness_rule_id"].tolist() if not report_df.empty else [],
    }
    return report_df, summary


def load_canonical_feature_registry(*, schema_dir: Path = SCHEMA_DIR) -> pd.DataFrame:
    payload = load_yaml(schema_dir / "canonical_feature_registry.yaml")
    return pd.DataFrame(payload.get("registry", [])).fillna("")


def load_readiness_rules(*, schema_dir: Path = SCHEMA_DIR) -> list[dict[str, Any]]:
    payload = load_yaml(schema_dir / "research_task_readiness_rules.yaml")
    return list(payload.get("rules", []))


def _evaluate_rule(
    rule: dict[str, Any],
    universe: set[str],
    long_values: pd.DataFrame,
    universe_filter_failures: list[str] | None = None,
    *,
    evaluated_at: str,
) -> dict[str, Any]:
    molecule_count = len(universe)
    hard_failures: list[str] = list(universe_filter_failures or [])
    warnings: list[str] = []
    failed_feature_keys: list[str] = []
    must_coverages: list[dict[str, Any]] = []
    should_coverages: list[dict[str, Any]] = []

    minimum_molecule_count = int(rule.get("minimum_molecule_count") or 0)
    if molecule_count < minimum_molecule_count:
        hard_failures.append(f"molecule_count {molecule_count} below minimum {minimum_molecule_count}")

    for item in rule.get("must_have", []):
        coverage = _feature_coverage(item, universe, long_values, rule=rule, fallback_target_key="required_coverage")
        must_coverages.append(coverage)
        if coverage["coverage"] + 1e-12 < coverage["target_coverage"]:
            failed_feature_keys.append(coverage["canonical_feature_key"])
            hard_failures.append(
                f"{coverage['canonical_feature_key']} coverage {coverage['coverage']:.3f} below {coverage['target_coverage']:.3f}"
            )

    for item in rule.get("should_have", []):
        coverage = _feature_coverage(item, universe, long_values, rule=rule, fallback_target_key="target_coverage")
        should_coverages.append(coverage)
        if coverage["coverage"] + 1e-12 < coverage["target_coverage"]:
            warnings.append(
                f"{coverage['canonical_feature_key']} coverage {coverage['coverage']:.3f} below {coverage['target_coverage']:.3f}"
            )

    should_average = (
        sum(item["coverage"] for item in should_coverages) / len(should_coverages)
        if should_coverages
        else 1.0
    )
    minimum_should = float(rule.get("minimum_should_have_coverage") or 0.0)
    status = "passed"
    if hard_failures:
        status = "failed"
    elif should_average + 1e-12 < minimum_should:
        status = "degraded"

    return {
        "readiness_rule_id": str(rule.get("readiness_rule_id", "")),
        "task_name": str(rule.get("task_name", "")),
        "task_scope": str(rule.get("task_scope", "")),
        "status": status,
        "molecule_count": int(molecule_count),
        "minimum_molecule_count": minimum_molecule_count,
        "must_have_passed": not hard_failures,
        "should_have_average_coverage": float(should_average),
        "minimum_should_have_coverage": minimum_should,
        "failed_feature_keys_json": json.dumps(sorted(set(failed_feature_keys)), ensure_ascii=False),
        "hard_failures_json": json.dumps(hard_failures, ensure_ascii=False),
        "warnings_json": json.dumps(warnings, ensure_ascii=False),
        "must_have_coverage_json": json.dumps(must_coverages, ensure_ascii=False),
        "should_have_coverage_json": json.dumps(should_coverages, ensure_ascii=False),
        "source_layer": str(rule.get("source_layer", "")),
        "evaluated_at": evaluated_at,
    }


def _feature_coverage(
    item: dict[str, Any],
    universe: set[str],
    long_values: pd.DataFrame,
    *,
    rule: dict[str, Any],
    fallback_target_key: str,
) -> dict[str, Any]:
    key = str(item.get("canonical_feature_key", ""))
    target = float(item.get(fallback_target_key) or item.get("required_coverage") or item.get("target_coverage") or 0.0)
    requirement = str(item.get("value_requirement") or ("numeric" if bool(int(rule.get("require_numeric_values", 0))) else "non_null"))
    values = long_values.loc[long_values["canonical_feature_key"] == key].copy() if not long_values.empty else pd.DataFrame()

    if not values.empty and not bool(int(rule.get("allow_proxy_rows", 1))):
        values = values.loc[~values["is_proxy_or_screening"].fillna(False).astype(bool)]

    minimum_quality = rule.get("minimum_quality_score")
    if minimum_quality not in {None, ""} and not values.empty and "data_quality_score_100" in values.columns:
        quality = pd.to_numeric(values["data_quality_score_100"], errors="coerce")
        values = values.loc[quality.isna() | (quality >= float(minimum_quality))]

    if requirement == "numeric":
        numeric = pd.to_numeric(values["value_num"], errors="coerce") if "value_num" in values.columns else pd.Series([], dtype="float64")
        values = values.loc[numeric.notna()]
    else:
        value_text = values["value"].fillna("").astype(str) if "value" in values.columns else pd.Series([], dtype="object")
        value_num = pd.to_numeric(values["value_num"], errors="coerce") if "value_num" in values.columns else pd.Series([], dtype="float64")
        values = values.loc[value_text.str.len().gt(0) | value_num.notna()]

    if bool(int(rule.get("require_source_traceability", 0))) and not values.empty:
        source_ids = values["selected_source_id"].fillna("").astype(str) if "selected_source_id" in values.columns else pd.Series([], dtype="object")
        values = values.loc[source_ids.str.len().gt(0)]

    available = set(values["mol_id"].astype(str).tolist()) & universe if not values.empty else set()
    coverage = len(available) / len(universe) if universe else 0.0
    return {
        "canonical_feature_key": key,
        "available_count": int(len(available)),
        "molecule_count": int(len(universe)),
        "coverage": float(coverage),
        "target_coverage": target,
        "value_requirement": requirement,
    }


def _eligible_molecule_ids(rule: dict[str, Any], frames: dict[str, pd.DataFrame]) -> tuple[set[str], list[str]]:
    molecule_core = frames.get("molecule_core", pd.DataFrame()).copy()
    source_layer = str(rule.get("source_layer", ""))
    if source_layer == "model_ready_plus_property_recommended":
        base = frames.get("model_ready", pd.DataFrame()).copy()
    else:
        base = molecule_core

    if base.empty or "mol_id" not in base.columns:
        return set(), ["eligible molecule universe is empty or lacks mol_id"]

    base = _enrich_with_seed_catalog(base, frames)
    failures: list[str] = []

    entity_scope_filter = _rule_text(rule.get("entity_scope_filter", "any"), default="any")
    if entity_scope_filter != "any":
        if "entity_scope" not in base.columns:
            failures.append(f"entity_scope_filter={entity_scope_filter} requires entity_scope column")
        elif entity_scope_filter == "refrigerant_or_candidate":
            base = base.loc[base["entity_scope"].fillna("").astype(str).isin({"refrigerant", "candidate"})]
        elif entity_scope_filter in {"refrigerant", "candidate"}:
            base = base.loc[base["entity_scope"].fillna("").astype(str) == entity_scope_filter]
        else:
            failures.append(f"unsupported entity_scope_filter={entity_scope_filter}")

    model_inclusion_filter = _rule_text(rule.get("model_inclusion_filter", "any"), default="any")
    if model_inclusion_filter != "any":
        if "model_inclusion" not in base.columns:
            failures.append(f"model_inclusion_filter={model_inclusion_filter} requires model_inclusion column")
        elif model_inclusion_filter in {"yes", "no"}:
            base = base.loc[base["model_inclusion"].fillna("").astype(str) == model_inclusion_filter]
        else:
            failures.append(f"unsupported model_inclusion_filter={model_inclusion_filter}")

    coverage_tier_filter = _rule_list(rule.get("coverage_tier_filter"))
    if coverage_tier_filter:
        if "coverage_tier" not in base.columns:
            failures.append(f"coverage_tier_filter={','.join(coverage_tier_filter)} requires coverage_tier column")
        else:
            base = base.loc[base["coverage_tier"].fillna("").astype(str).isin(set(coverage_tier_filter))]

    return set(base["mol_id"].astype(str).tolist()), failures


def _enrich_with_seed_catalog(base: pd.DataFrame, frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    out = base.copy()
    seed_catalog = frames.get("seed_catalog", pd.DataFrame()).copy()
    molecule_core = frames.get("molecule_core", pd.DataFrame()).copy()
    filter_columns = ["coverage_tier", "entity_scope", "model_inclusion"]

    if "seed_id" not in out.columns and {"mol_id", "seed_id"}.issubset(molecule_core.columns):
        seed_by_mol = molecule_core[["mol_id", "seed_id"] + [column for column in filter_columns if column in molecule_core.columns]]
        seed_by_mol = seed_by_mol.drop_duplicates(subset=["mol_id"], keep="first")
        out = out.merge(seed_by_mol, on="mol_id", how="left", suffixes=("", "_molecule_core"))
        for column in filter_columns:
            out = _fill_from_suffix(out, column, "_molecule_core")

    if "seed_id" in out.columns and not seed_catalog.empty and "seed_id" in seed_catalog.columns:
        seed_columns = ["seed_id"] + [column for column in filter_columns if column in seed_catalog.columns]
        seed_lookup = seed_catalog[seed_columns].drop_duplicates(subset=["seed_id"], keep="first")
        out = out.merge(seed_lookup, on="seed_id", how="left", suffixes=("", "_seed"))
        for column in filter_columns:
            out = _fill_from_suffix(out, column, "_seed")

    return out


def _fill_from_suffix(df: pd.DataFrame, column: str, suffix: str) -> pd.DataFrame:
    suffix_column = f"{column}{suffix}"
    if suffix_column not in df.columns:
        return df
    if column in df.columns:
        current = df[column].fillna("").astype(str)
        df[column] = df[column].where(current.str.len().gt(0), df[suffix_column])
    else:
        df[column] = df[suffix_column]
    return df.drop(columns=[suffix_column])


def _rule_text(value: Any, *, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, bool):
        return "yes" if value else "no"
    text = str(value).strip()
    return text if text else default


def _rule_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    try:
        if pd.isna(value):
            return []
    except TypeError:
        pass
    if isinstance(value, str) and not value.strip():
        return []
    return [item.strip() for item in str(value).replace(";", ",").split(",") if item.strip()]


def _values_for_rule(
    rule: dict[str, Any],
    frames: dict[str, pd.DataFrame],
    feature_aliases: dict[str, set[str]],
    universe: set[str],
) -> pd.DataFrame:
    source_layer = str(rule.get("source_layer", ""))
    frames_long: list[pd.DataFrame] = []
    if source_layer == "property_recommended_canonical_strict":
        frames_long.append(_canonical_recommended_long(frames.get("property_recommended_canonical_strict", pd.DataFrame())))
    elif source_layer == "property_recommended_canonical_or_legacy_recommended":
        frames_long.append(_canonical_recommended_long(frames.get("property_recommended_canonical", pd.DataFrame())))
        frames_long.append(_legacy_recommended_long(frames.get("property_recommended", pd.DataFrame()), feature_aliases))
        frames_long.append(_molecule_core_long(frames.get("molecule_core", pd.DataFrame())))
    elif source_layer == "model_ready_plus_property_recommended":
        frames_long.append(_wide_feature_long(frames.get("model_ready", pd.DataFrame()), feature_aliases, "source_model_ready"))
        frames_long.append(_legacy_recommended_long(frames.get("property_recommended", pd.DataFrame()), feature_aliases))
    elif source_layer == "molecule_core_plus_property_recommended":
        frames_long.append(_molecule_core_long(frames.get("molecule_core", pd.DataFrame())))
        frames_long.append(_legacy_recommended_long(frames.get("property_recommended", pd.DataFrame()), feature_aliases))
    else:
        frames_long.append(_legacy_recommended_long(frames.get("property_recommended", pd.DataFrame()), feature_aliases))

    non_empty = [frame for frame in frames_long if not frame.empty]
    if not non_empty:
        return _empty_values()
    long_values = pd.concat(non_empty, ignore_index=True)
    long_values = long_values.loc[long_values["mol_id"].astype(str).isin(universe)]
    return _ensure_columns(long_values, _empty_values().columns.tolist())


def _feature_aliases(registry: pd.DataFrame) -> dict[str, set[str]]:
    aliases: dict[str, set[str]] = {}
    for row in registry.to_dict(orient="records"):
        key = str(row.get("canonical_feature_key", ""))
        values = {key}
        legacy = str(row.get("legacy_property_name", "")).strip()
        if legacy:
            values.add(legacy)
        alias_json = str(row.get("aliases_json", "")).strip()
        if alias_json:
            try:
                values.update(str(item) for item in json.loads(alias_json))
            except json.JSONDecodeError:
                pass
        aliases[key] = values
    return aliases


def _canonical_recommended_long(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return _empty_values()
    out = pd.DataFrame(
        {
            "mol_id": df["mol_id"].astype(str),
            "canonical_feature_key": df["canonical_feature_key"].astype(str),
            "value": df["value"].fillna("").astype(str) if "value" in df.columns else "",
            "value_num": pd.to_numeric(df["value_num"], errors="coerce") if "value_num" in df.columns else None,
            "selected_source_id": df["selected_source_id"].fillna("").astype(str) if "selected_source_id" in df.columns else "",
            "data_quality_score_100": pd.to_numeric(df["data_quality_score_100"], errors="coerce") if "data_quality_score_100" in df.columns else None,
            "is_proxy_or_screening": df["is_proxy_or_screening"].fillna(False).astype(bool) if "is_proxy_or_screening" in df.columns else False,
        }
    )
    return _ensure_columns(out, _empty_values().columns.tolist())


def _legacy_recommended_long(df: pd.DataFrame, feature_aliases: dict[str, set[str]]) -> pd.DataFrame:
    if df.empty:
        return _empty_values()
    property_to_key: dict[str, str] = {}
    for key, aliases in feature_aliases.items():
        for alias in aliases:
            property_to_key[alias] = key
    mapped = df.copy()
    mapped["canonical_feature_key"] = mapped["property_name"].map(property_to_key).fillna("")
    mapped = mapped.loc[mapped["canonical_feature_key"].astype(str).str.len().gt(0)]
    if mapped.empty:
        return _empty_values()
    is_proxy = mapped["property_name"].fillna("").astype(str).isin(PROXY_PROPERTIES)
    out = pd.DataFrame(
        {
            "mol_id": mapped["mol_id"].astype(str),
            "canonical_feature_key": mapped["canonical_feature_key"].astype(str),
            "value": mapped["value"].fillna("").astype(str) if "value" in mapped.columns else "",
            "value_num": pd.to_numeric(mapped["value_num"], errors="coerce") if "value_num" in mapped.columns else None,
            "selected_source_id": mapped["selected_source_id"].fillna("").astype(str) if "selected_source_id" in mapped.columns else "",
            "data_quality_score_100": is_proxy.map({True: PROXY_DATA_QUALITY_SCORE, False: None}),
            "is_proxy_or_screening": is_proxy,
        }
    )
    return _ensure_columns(out, _empty_values().columns.tolist())


def _molecule_core_long(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return _empty_values()
    rows: list[dict[str, Any]] = []
    for record in df.to_dict(orient="records"):
        mol_id = str(record.get("mol_id", ""))
        if not mol_id:
            continue
        rows.append(_value_row(mol_id, "identity.mol_id", mol_id, None, "source_molecule_core"))
        molecular_weight = record.get("molecular_weight")
        rows.append(_value_row(mol_id, "molecular_descriptor.molecular_weight", molecular_weight, molecular_weight, "source_molecule_core"))
    return _ensure_columns(pd.DataFrame(rows), _empty_values().columns.tolist())


def _wide_feature_long(df: pd.DataFrame, feature_aliases: dict[str, set[str]], source_id: str) -> pd.DataFrame:
    if df.empty or "mol_id" not in df.columns:
        return _empty_values()
    rows: list[dict[str, Any]] = []
    for key, aliases in feature_aliases.items():
        matching = [alias for alias in aliases if alias in df.columns]
        if not matching:
            continue
        column = matching[0]
        if column == "mol_id":
            for mol_id in df["mol_id"].astype(str).tolist():
                rows.append(_value_row(mol_id, key, mol_id, None, source_id))
            continue
        values = df[["mol_id", column]].copy()
        for record in values.to_dict(orient="records"):
            value = record.get(column)
            rows.append(_value_row(str(record.get("mol_id", "")), key, value, value, source_id))
    return _ensure_columns(pd.DataFrame(rows), _empty_values().columns.tolist())


def _value_row(mol_id: str, key: str, value: Any, value_num: Any, source_id: str) -> dict[str, Any]:
    return {
        "mol_id": mol_id,
        "canonical_feature_key": key,
        "value": "" if pd.isna(value) else str(value),
        "value_num": pd.to_numeric(pd.Series([value_num]), errors="coerce").iloc[0],
        "selected_source_id": source_id,
        "data_quality_score_100": None,
        "is_proxy_or_screening": False,
    }


def _empty_values() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "mol_id",
            "canonical_feature_key",
            "value",
            "value_num",
            "selected_source_id",
            "data_quality_score_100",
            "is_proxy_or_screening",
        ]
    )


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path).fillna("")


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in df.columns:
            df[column] = None
    return df[columns]
