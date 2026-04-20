"""Dataset validation helpers."""

from __future__ import annotations

from typing import Any

import pandas as pd

from r_physgen_db.constants import DATA_DIR, PROJECT_ROOT, SCHEMA_DIR
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
        SCHEMA_DIR / "regulatory_status.yaml",
        SCHEMA_DIR / "property_recommended.yaml",
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
    source_manifest_df = pd.read_parquet(DATA_DIR / "bronze" / "source_manifest.parquet")
    molecule_core_df = pd.read_parquet(DATA_DIR / "silver" / "molecule_core.parquet")
    seed_catalog_df = pd.read_csv(DATA_DIR / "raw" / "manual" / "seed_catalog.csv").fillna("")
    quality_report = _build_inventory_convergence(seed_catalog_df, molecule_core_df, recommended_df)

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
