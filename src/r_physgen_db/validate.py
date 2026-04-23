"""Dataset validation helpers."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

from r_physgen_db.constants import DATA_DIR, PROJECT_ROOT, SCHEMA_DIR
from r_physgen_db.sources.property_governance_bundle import default_bundle_path, load_property_governance_bundle
from r_physgen_db.utils import load_yaml, write_json


def validate_dataset() -> dict[str, Any]:
    results: dict[str, Any] = {
        "schema_checks": [],
        "integration_checks": [],
        "inventory_checks": [],
        "quality_gate_checks": [],
        "errors": [],
    }

    for schema_path in [
        SCHEMA_DIR / "source_manifest.yaml",
        SCHEMA_DIR / "pending_sources.yaml",
        SCHEMA_DIR / "molecule_core.yaml",
        SCHEMA_DIR / "molecule_alias.yaml",
        SCHEMA_DIR / "property_observation.yaml",
        SCHEMA_DIR / "property_observation_canonical.yaml",
        SCHEMA_DIR / "regulatory_status.yaml",
        SCHEMA_DIR / "property_recommended.yaml",
        SCHEMA_DIR / "property_recommended_canonical.yaml",
        SCHEMA_DIR / "property_recommended_canonical_strict.yaml",
        SCHEMA_DIR / "property_recommended_canonical_review_queue.yaml",
        SCHEMA_DIR / "property_dictionary.yaml",
        SCHEMA_DIR / "property_canonical_map.yaml",
        SCHEMA_DIR / "unit_conversion_rules.yaml",
        SCHEMA_DIR / "property_source_priority_rules.yaml",
        SCHEMA_DIR / "property_modeling_readiness_rules.yaml",
        SCHEMA_DIR / "property_governance_issues.yaml",
        SCHEMA_DIR / "structure_features.yaml",
        SCHEMA_DIR / "model_dataset_index.yaml",
    ]:
        schema = load_yaml(schema_path)
        parquet_path = PROJECT_ROOT / schema["path"]
        _check(results, parquet_path.exists(), f"{schema['table_name']}: table exists", f"Missing table: {parquet_path}")
        if parquet_path.exists():
            df = pd.read_parquet(parquet_path)
            for column in schema["columns"]:
                if column["required"]:
                    _check(
                        results,
                        column["name"] in df.columns,
                        f"{schema['table_name']}: has required column {column['name']}",
                        f"{schema['table_name']}: missing column {column['name']}",
                    )
            results["schema_checks"].append({"table": schema["table_name"], "row_count": int(len(df))})

    gold_schema = load_yaml(SCHEMA_DIR / "gold_tables.yaml")
    for table_def in gold_schema["tables"]:
        parquet_path = PROJECT_ROOT / table_def["path"]
        _check(results, parquet_path.exists(), f"{table_def['table_name']}: table exists", f"Missing gold table: {parquet_path}")
        if parquet_path.exists():
            df = pd.read_parquet(parquet_path)
            for column in table_def["required_columns"]:
                _check(
                    results,
                    column in df.columns,
                    f"{table_def['table_name']}: has required column {column}",
                    f"{table_def['table_name']}: missing column {column}",
                )

    alias_df = pd.read_parquet(DATA_DIR / "silver" / "molecule_alias.parquet")
    master_df = pd.read_parquet(DATA_DIR / "gold" / "molecule_master.parquet")
    model_index_df = pd.read_parquet(DATA_DIR / "gold" / "model_dataset_index.parquet")
    model_ready_df = pd.read_parquet(DATA_DIR / "gold" / "model_ready.parquet")
    recommended_df = pd.read_parquet(DATA_DIR / "gold" / "property_recommended.parquet")
    canonical_observation_df = pd.read_parquet(DATA_DIR / "silver" / "property_observation_canonical.parquet")
    canonical_recommended_df = pd.read_parquet(DATA_DIR / "gold" / "property_recommended_canonical.parquet")
    canonical_recommended_strict_df = pd.read_parquet(DATA_DIR / "gold" / "property_recommended_canonical_strict.parquet")
    canonical_review_queue_df = pd.read_parquet(DATA_DIR / "gold" / "property_recommended_canonical_review_queue.parquet")
    readiness_df = pd.read_parquet(DATA_DIR / "gold" / "property_modeling_readiness_rules.parquet")
    source_manifest_df = pd.read_parquet(DATA_DIR / "bronze" / "source_manifest.parquet")
    molecule_core_df = pd.read_parquet(DATA_DIR / "silver" / "molecule_core.parquet")
    seed_catalog_df = pd.read_csv(DATA_DIR / "raw" / "manual" / "seed_catalog.csv").fillna("")
    quality_report = _build_inventory_convergence(seed_catalog_df, molecule_core_df, recommended_df)
    bundle_audit = _load_property_governance_audit()

    for r_number in ["R-32", "R-134a", "R-1234yf", "R-744", "R-717"]:
        matching = alias_df.loc[(alias_df["alias_type"] == "r_number") & (alias_df["alias_value"] == r_number), "mol_id"].unique()
        _check(
            results,
            len(matching) == 1,
            f"{r_number}: alias resolves to one mol_id",
            f"Expected one mol_id for {r_number}, got {len(matching)}",
        )
        if len(matching) == 1:
            _check(
                results,
                bool((master_df["mol_id"] == matching[0]).any()),
                f"{r_number}: present in molecule_master",
                f"{r_number} missing from molecule_master",
            )

    ez_rows = alias_df.loc[(alias_df["alias_type"] == "r_number") & (alias_df["alias_value"].isin(["R-1234ze(E)", "R-1234ze(Z)"]))]
    _check(
        results,
        len(ez_rows["mol_id"].unique()) == 2,
        "R-1234ze(E)/R-1234ze(Z): separated",
        "R-1234ze(E) and R-1234ze(Z) must remain separate",
    )

    manifest_source_ids = set(source_manifest_df["source_id"].tolist())
    dangling = sorted(set(recommended_df["selected_source_id"].dropna().tolist()) - manifest_source_ids)
    _check(results, not dangling, "Recommended values: source tracing intact", f"Dangling recommended source ids: {dangling}")

    leakage = model_index_df.groupby("scaffold_key")["split"].nunique()
    leaking_scaffolds = leakage.loc[leakage > 1]
    _check(results, leaking_scaffolds.empty, "Scaffold split: no leakage", f"Scaffold leakage detected: {leaking_scaffolds.to_dict()}")

    _check(
        results,
        len(molecule_core_df) >= 120,
        "Wave 2C entity pool: at least 120 molecules",
        f"Expected at least 120 resolved molecules, got {len(molecule_core_df)}",
    )

    resolved_refrigerants = _resolved_refrigerant_inventory(seed_catalog_df, molecule_core_df)
    unresolved_refrigerants = _unresolved_refrigerants(seed_catalog_df, molecule_core_df)
    results["inventory_checks"].append(
        {
            "refrigerant_count": int((seed_catalog_df["entity_scope"].astype(str) == "refrigerant").sum()) if "entity_scope" in seed_catalog_df.columns else 0,
            "candidate_count": int((seed_catalog_df["entity_scope"].astype(str) == "candidate").sum()) if "entity_scope" in seed_catalog_df.columns else 0,
            "resolved_refrigerant_count": resolved_refrigerants,
            "unresolved_refrigerant_count": len(unresolved_refrigerants),
        }
    )
    _check(
        results,
        not unresolved_refrigerants,
        "Curated refrigerant inventory: every refrigerant seed resolves to a molecule",
        f"Unresolved refrigerant seeds: {unresolved_refrigerants}",
    )

    _check(
        results,
        set(model_ready_df["mol_id"].tolist()) <= set(model_index_df["mol_id"].tolist()),
        "model_ready: aligned with model_dataset_index eligibility",
        "model_ready contains mol_ids outside model_dataset_index",
    )

    strict_validation = _validate_canonical_strict_alignment(canonical_recommended_strict_df, readiness_df)
    _check(
        results,
        strict_validation["all_rules_satisfied"],
        "property_recommended_canonical_strict: strict filtering rules satisfied",
        f"property_recommended_canonical_strict rule violations: {strict_validation['violations']}",
    )

    review_queue_validation = _validate_canonical_review_queue(canonical_review_queue_df)
    _check(
        results,
        review_queue_validation["all_rules_satisfied"],
        "property_recommended_canonical_review_queue: review triggers are well formed",
        f"property_recommended_canonical_review_queue rule violations: {review_queue_validation['violations']}",
    )

    mirror_audit = _validate_property_governance_extension_mirror()
    if mirror_audit is not None:
        _check(
            results,
            mirror_audit["status"] == "ok",
            "Property governance extension mirror: row manifest aligned",
            f"Property governance extension mirror mismatch: {mirror_audit}",
        )

    coverage = _coverage_by_tier(seed_catalog_df, molecule_core_df, recommended_df)
    _check_threshold(results, coverage, "A", "boiling_point_c", 0.95)
    _check_threshold(results, coverage, "A", "critical_temp_c", 0.95)
    _check_threshold(results, coverage, "A", "critical_pressure_mpa", 0.95)
    _check_threshold(results, coverage, "A", "odp", 0.90)
    _check_threshold(results, coverage, "A", "gwp_100yr", 0.90)
    _check_threshold(results, coverage, "A", "ashrae_safety", 0.90)

    _check_threshold(results, coverage, "B", "boiling_point_c", 0.80)
    _check_threshold(results, coverage, "B", "critical_temp_c", 0.80)
    _check_threshold(results, coverage, "B", "critical_pressure_mpa", 0.80)

    report_path = DATA_DIR / "gold" / "validation_report.json"
    results["coverage_by_tier"] = coverage
    results["refrigerant_count"] = quality_report["refrigerant_count"]
    results["candidate_count"] = quality_report["candidate_count"]
    results["unresolved_refrigerants"] = unresolved_refrigerants
    results["inventory_property_gaps"] = quality_report["inventory_property_gaps"]
    results["inventory_convergence"] = quality_report
    results["canonical_metrics"] = _canonical_metrics(
        canonical_observation_df,
        canonical_recommended_df,
        canonical_recommended_strict_df,
        canonical_review_queue_df,
        bundle_audit,
    )
    results["property_governance_bundle"] = {
        "audit": bundle_audit,
        "extension_mirror_validation": mirror_audit,
        "strict_validation": strict_validation,
        "review_queue_validation": review_queue_validation,
    }
    write_json(report_path, results)
    return results


def _build_inventory_convergence(
    seed_catalog_df: pd.DataFrame,
    molecule_core_df: pd.DataFrame,
    recommended_df: pd.DataFrame,
) -> dict[str, Any]:
    return {
        "refrigerant_count": int((seed_catalog_df["entity_scope"].astype(str) == "refrigerant").sum()) if "entity_scope" in seed_catalog_df.columns else 0,
        "candidate_count": int((seed_catalog_df["entity_scope"].astype(str) == "candidate").sum()) if "entity_scope" in seed_catalog_df.columns else 0,
        "inventory_property_gaps": _inventory_property_gaps(seed_catalog_df, molecule_core_df, recommended_df),
    }


def _resolved_refrigerant_inventory(seed_catalog_df: pd.DataFrame, molecule_core_df: pd.DataFrame) -> int:
    if "entity_scope" not in seed_catalog_df.columns:
        return 0
    resolved = pd.merge(
        seed_catalog_df.loc[seed_catalog_df["entity_scope"].astype(str) == "refrigerant", ["seed_id"]],
        molecule_core_df[["seed_id"]],
        on="seed_id",
        how="inner",
    )
    return int(len(resolved))


def _unresolved_refrigerants(seed_catalog_df: pd.DataFrame, molecule_core_df: pd.DataFrame) -> list[str]:
    if "entity_scope" not in seed_catalog_df.columns:
        return []
    resolved_seed_ids = set(molecule_core_df["seed_id"].tolist())
    refrigerant_seed_ids = seed_catalog_df.loc[seed_catalog_df["entity_scope"].astype(str) == "refrigerant", "seed_id"].tolist()
    return sorted(seed_id for seed_id in refrigerant_seed_ids if seed_id not in resolved_seed_ids)


def _inventory_property_gaps(
    seed_catalog_df: pd.DataFrame,
    molecule_core_df: pd.DataFrame,
    recommended_df: pd.DataFrame,
) -> dict[str, Any]:
    tracked_properties = ["gwp_100yr", "odp", "ashrae_safety", "toxicity_class"]
    resolved = pd.merge(
        seed_catalog_df[["seed_id", "entity_scope", "coverage_tier"]],
        molecule_core_df[["seed_id", "mol_id"]] if not molecule_core_df.empty else pd.DataFrame(columns=["seed_id", "mol_id"]),
        on="seed_id",
        how="left",
    )
    available = (
        recommended_df.groupby("mol_id")["property_name"].apply(set).to_dict()
        if not recommended_df.empty
        else {}
    )

    gaps: dict[str, Any] = {}
    for (entity_scope, coverage_tier), group in resolved.groupby(["entity_scope", "coverage_tier"], dropna=False):
        scope_key = str(entity_scope or "unknown")
        tier_key = str(coverage_tier or "unknown")
        scope_bucket = gaps.setdefault(scope_key, {})
        total = int(len(group))
        tier_bucket: dict[str, Any] = {}
        for property_name in tracked_properties:
            present = 0
            for mol_id in group["mol_id"].tolist():
                if mol_id and property_name in available.get(mol_id, set()):
                    present += 1
            tier_bucket[property_name] = {
                "present_count": present,
                "missing_count": total - present,
                "total_count": total,
            }
        scope_bucket[tier_key] = tier_bucket
    return gaps


def _coverage_by_tier(seed_catalog_df: pd.DataFrame, molecule_core_df: pd.DataFrame, recommended_df: pd.DataFrame) -> dict[str, Any]:
    merged = pd.merge(molecule_core_df[["mol_id", "seed_id"]], seed_catalog_df[["seed_id", "coverage_tier"]], on="seed_id", how="left")
    coverage: dict[str, Any] = {}
    for tier in sorted(merged["coverage_tier"].dropna().unique()):
        mol_ids = merged.loc[merged["coverage_tier"] == tier, "mol_id"].unique().tolist()
        if not mol_ids:
            continue
        subset = recommended_df.loc[recommended_df["mol_id"].isin(mol_ids)]
        counts = subset.groupby("property_name")["mol_id"].nunique().to_dict() if not subset.empty else {}
        coverage[tier] = {prop: counts.get(prop, 0) / len(mol_ids) for prop in sorted(set(counts) | {"boiling_point_c", "critical_temp_c", "critical_pressure_mpa", "odp", "gwp_100yr", "ashrae_safety"})}
        coverage[tier]["molecule_count"] = len(mol_ids)
    return coverage


def _load_property_governance_audit() -> dict[str, Any]:
    path = DATA_DIR / "bronze" / "property_governance_20260422_audit.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _validate_property_governance_extension_mirror() -> dict[str, Any] | None:
    bundle_path = default_bundle_path(PROJECT_ROOT)
    extension_manifest_path = PROJECT_ROOT / "data" / "extensions" / "property_governance_20260422" / "extension_manifest.parquet"
    if not bundle_path.exists() or not extension_manifest_path.exists():
        return None

    bundle = load_property_governance_bundle(bundle_path)
    extension_manifest_df = pd.read_parquet(extension_manifest_path).fillna("")
    expected = {str(row["table_name"]): int(row["row_count"]) for row in bundle.row_manifest.to_dict(orient="records")}
    actual = {str(row["table_name"]): int(row["row_count"]) for row in extension_manifest_df.to_dict(orient="records")}
    missing_tables = sorted(set(expected) - set(actual))
    extra_tables = sorted(set(actual) - set(expected))
    row_mismatch_tables = [
        {"table_name": table_name, "expected": expected[table_name], "actual": actual[table_name]}
        for table_name in sorted(set(expected) & set(actual))
        if expected[table_name] != actual[table_name]
    ]
    return {
        "status": "ok" if not missing_tables and not extra_tables and not row_mismatch_tables else "error",
        "expected_table_count": int(len(expected)),
        "actual_table_count": int(len(actual)),
        "missing_tables": missing_tables,
        "extra_tables": extra_tables,
        "row_mismatch_tables": row_mismatch_tables,
    }


def _validate_canonical_strict_alignment(strict_df: pd.DataFrame, readiness_df: pd.DataFrame) -> dict[str, Any]:
    if strict_df.empty:
        return {"all_rules_satisfied": True, "violations": []}

    readiness = readiness_df.copy().fillna("")
    defaults: dict[str, Any] = {
        "use_as_ml_feature": 0,
        "use_as_ml_target": 0,
        "minimum_quality_score": None,
        "exclude_if_proxy_or_screening": 0,
    }
    for column, default in defaults.items():
        if column not in readiness.columns:
            readiness[column] = default
    merged = strict_df.merge(
        readiness[
            [
                "canonical_feature_key",
                "canonical_property_id",
                "use_as_ml_feature",
                "use_as_ml_target",
                "minimum_quality_score",
                "exclude_if_proxy_or_screening",
            ]
        ].drop_duplicates(subset=["canonical_feature_key", "canonical_property_id"], keep="first"),
        on=["canonical_feature_key", "canonical_property_id"],
        how="left",
        suffixes=("", "_rule"),
    )
    merged["use_as_ml_feature_rule"] = pd.to_numeric(merged["use_as_ml_feature_rule"], errors="coerce").fillna(0).astype(int)
    merged["use_as_ml_target_rule"] = pd.to_numeric(merged["use_as_ml_target_rule"], errors="coerce").fillna(0).astype(int)
    merged["exclude_if_proxy_or_screening_rule"] = (
        pd.to_numeric(merged["exclude_if_proxy_or_screening_rule"], errors="coerce").fillna(0).astype(int)
    )
    merged["minimum_quality_score_rule"] = pd.to_numeric(merged["minimum_quality_score_rule"], errors="coerce")

    violations: list[str] = []
    if not merged["strict_accept"].fillna(False).astype(bool).all():
        violations.append("strict_accept contains false rows")
    if not merged["value_num"].notna().all():
        violations.append("value_num contains null rows")
    use_mask = merged["use_as_ml_feature_rule"].eq(1) | merged["use_as_ml_target_rule"].eq(1)
    if not use_mask.all():
        violations.append("rows without ML feature/target eligibility")
    quality_mask = merged["minimum_quality_score_rule"].notna() & (
        pd.to_numeric(merged["data_quality_score_100"], errors="coerce") >= merged["minimum_quality_score_rule"]
    )
    if not quality_mask.all():
        violations.append("rows below minimum_quality_score")
    strict_basis = merged.get("strict_accept_basis", pd.Series("", index=merged.index)).fillna("").astype(str)
    if not strict_basis.isin(["standard", "proxy_only_policy"]).all():
        violations.append("strict_accept_basis contains unexpected values")
    proxy_override_mask = strict_basis.eq("proxy_only_policy") & (
        merged.get("proxy_policy_id", pd.Series("", index=merged.index)).fillna("").astype(str).ne("")
    )
    proxy_mask = (
        ~merged["exclude_if_proxy_or_screening_rule"].eq(1)
        | ~merged["is_proxy_or_screening"].fillna(False).astype(bool)
        | proxy_override_mask
    )
    if not proxy_mask.all():
        violations.append("proxy rows survived exclude_if_proxy_or_screening rule")

    return {"all_rules_satisfied": not violations, "violations": violations}


def _validate_canonical_review_queue(review_queue_df: pd.DataFrame) -> dict[str, Any]:
    if review_queue_df.empty:
        return {"all_rules_satisfied": True, "violations": []}

    violations: list[str] = []
    if review_queue_df["review_reason"].astype(str).str.strip().eq("").any():
        violations.append("review_reason contains blank rows")
    if review_queue_df["review_priority"].astype(str).str.strip().eq("").any():
        violations.append("review_priority contains blank rows")
    trigger_mask = (
        review_queue_df["conflict_flag"].fillna(False).astype(bool)
        | review_queue_df["source_divergence_flag"].fillna(False).astype(bool)
        | ~review_queue_df["strict_accept"].fillna(False).astype(bool)
    )
    if not trigger_mask.all():
        violations.append("rows without conflict/divergence/strict rejection trigger")
    return {"all_rules_satisfied": not violations, "violations": violations}


def _canonical_metrics(
    canonical_observation_df: pd.DataFrame,
    canonical_recommended_df: pd.DataFrame,
    canonical_recommended_strict_df: pd.DataFrame,
    canonical_review_queue_df: pd.DataFrame,
    bundle_audit: dict[str, Any],
) -> dict[str, Any]:
    crosswalk = bundle_audit.get("crosswalk", {}) if isinstance(bundle_audit, dict) else {}
    review_reason_counts = (
        canonical_review_queue_df["review_reason"].astype(str).value_counts().to_dict()
        if not canonical_review_queue_df.empty
        else {}
    )
    return {
        "canonical_observation_count": int(len(canonical_observation_df)),
        "canonical_recommended_count": int(len(canonical_recommended_df)),
        "canonical_recommended_strict_count": int(len(canonical_recommended_strict_df)),
        "canonical_review_queue_count": int(len(canonical_review_queue_df)),
        "canonical_proxy_selected_count": int(canonical_recommended_df["is_proxy_or_screening"].fillna(False).astype(bool).sum())
        if not canonical_recommended_df.empty
        else 0,
        "canonical_proxy_only_count": int(canonical_recommended_df["proxy_only_flag"].fillna(False).astype(bool).sum())
        if not canonical_recommended_df.empty
        else 0,
        "canonical_conflict_count": int(canonical_recommended_df["conflict_flag"].fillna(False).astype(bool).sum())
        if not canonical_recommended_df.empty
        else 0,
        "canonical_source_divergence_count": int(canonical_recommended_df["source_divergence_flag"].fillna(False).astype(bool).sum())
        if not canonical_recommended_df.empty
        else 0,
        "canonical_conflict_open_count": int(bundle_audit.get("canonical_conflict_open_count", review_reason_counts.get("top_rank_conflict", 0)) or 0),
        "canonical_source_divergence_open_count": int(
            bundle_audit.get("canonical_source_divergence_open_count", review_reason_counts.get("source_divergence", 0)) or 0
        ),
        "canonical_review_decision_count": int(bundle_audit.get("canonical_review_decision_count", 0) or 0),
        "canonical_proxy_policy_count": int(bundle_audit.get("canonical_proxy_policy_count", 0) or 0),
        "canonical_strict_proxy_accept_count": int(bundle_audit.get("canonical_strict_proxy_accept_count", 0) or 0),
        "canonical_review_reason_counts": review_reason_counts,
        "canonical_review_decision_reason_counts": bundle_audit.get("canonical_review_decision_reason_counts", {}),
        "canonical_review_decision_action_counts": bundle_audit.get("canonical_review_decision_action_counts", {}),
        "canonical_proxy_policy_feature_counts": bundle_audit.get("canonical_proxy_policy_feature_counts", {}),
        "bundle_unresolved_count": int(crosswalk.get("unresolved", 0) or 0),
        "bundle_external_resolution_count": int(crosswalk.get("external_resolution_count", 0) or 0),
        "row_count_audit_status": bundle_audit.get("row_count_audit", {}).get("status", ""),
    }


def _check_threshold(results: dict[str, Any], coverage: dict[str, Any], tier: str, property_name: str, minimum: float) -> None:
    actual = float(coverage.get(tier, {}).get(property_name, 0.0))
    _check(
        results,
        actual >= minimum,
        f"Tier {tier}: {property_name} coverage {actual:.3f} >= {minimum:.3f}",
        f"Tier {tier}: {property_name} coverage {actual:.3f} below {minimum:.3f}",
    )


def _check(results: dict[str, Any], condition: bool, success_message: str, error_message: str) -> None:
    if not condition:
        results["errors"].append(error_message)
    else:
        results["integration_checks"].append(success_message)
