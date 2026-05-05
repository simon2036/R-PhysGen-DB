"""Dataset validation helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from r_physgen_db.active_learning import (
    ACQUISITION_STRATEGIES,
    ACTIVE_LEARNING_SOURCE_ID,
    DECISION_ACTIONS,
    DECISION_STATUSES,
    HARD_CONSTRAINT_STATUSES,
    QUEUE_STATUSES,
    RECOMMENDED_NEXT_ACTIONS,
    active_learning_summary,
)
from r_physgen_db.constants import DATA_DIR, PROJECT_ROOT, SCHEMA_DIR
from r_physgen_db.dataset_migrations import validate_dataset_migrations
from r_physgen_db.mixtures import (
    MIXTURE_COMPOSITION_BASIS,
    MIXTURE_FORBIDDEN_WIDE_COLUMNS,
    fraction_sum_audit,
    mixture_summary,
)
from r_physgen_db.proxy_features import (
    PROXY_CANONICAL_FEATURE_KEYS,
    PROXY_DATA_QUALITY_SCORE,
    PROXY_ML_USE_STATUS,
    PROXY_PROPERTIES,
    PROXY_SOURCE_ID,
    SYNTHETIC_ACCESSIBILITY_PROPERTY,
    TFA_RISK_PROPERTY,
    TFA_RISK_SCORE,
    TFA_RISK_VOCABULARY,
    proxy_feature_summary,
)
from r_physgen_db.quantum_pilot import (
    ALL_QUANTUM_CANONICAL_FEATURE_KEYS,
    ALL_QUANTUM_FEATURES,
    ALL_QUANTUM_PROPERTY_NAMES,
    QUANTUM_CANONICAL_FEATURE_KEYS,
    QUANTUM_FEATURES,
    QUANTUM_FORBIDDEN_WIDE_COLUMNS,
    QUANTUM_PROPERTY_NAMES,
    QUANTUM_QUALITY_LEVELS,
    QUANTUM_SOURCE_ID,
    quantum_pilot_summary,
)
from r_physgen_db.readiness import evaluate_research_task_readiness_from_paths, validate_readiness_rule_references
from r_physgen_db.sources.property_governance_bundle import default_bundle_path, load_property_governance_bundle
from r_physgen_db.utils import load_yaml, write_json


def validate_dataset() -> dict[str, Any]:
    results: dict[str, Any] = {
        "schema_checks": [],
        "integration_checks": [],
        "inventory_checks": [],
        "quality_gate_checks": [],
        "migration_checks": {},
        "errors": [],
    }

    for schema_path in [
        SCHEMA_DIR / "source_manifest.yaml",
        SCHEMA_DIR / "pending_sources.yaml",
        SCHEMA_DIR / "molecule_core.yaml",
        SCHEMA_DIR / "molecule_alias.yaml",
        SCHEMA_DIR / "property_observation.yaml",
        SCHEMA_DIR / "observation_condition_set.yaml",
        SCHEMA_DIR / "cycle_case.yaml",
        SCHEMA_DIR / "cycle_operating_point.yaml",
        SCHEMA_DIR / "quantum_job.yaml",
        SCHEMA_DIR / "quantum_artifact.yaml",
        SCHEMA_DIR / "mixture_core.yaml",
        SCHEMA_DIR / "mixture_composition.yaml",
        SCHEMA_DIR / "active_learning_queue.yaml",
        SCHEMA_DIR / "active_learning_decision_log.yaml",
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
    property_matrix_df = pd.read_parquet(DATA_DIR / "gold" / "property_matrix.parquet")
    recommended_df = pd.read_parquet(DATA_DIR / "gold" / "property_recommended.parquet")
    property_observation_df = pd.read_parquet(DATA_DIR / "silver" / "property_observation.parquet")
    observation_condition_set_df = pd.read_parquet(DATA_DIR / "silver" / "observation_condition_set.parquet")
    cycle_case_df = pd.read_parquet(DATA_DIR / "silver" / "cycle_case.parquet")
    cycle_operating_point_df = pd.read_parquet(DATA_DIR / "silver" / "cycle_operating_point.parquet")
    quantum_job_df = pd.read_parquet(DATA_DIR / "silver" / "quantum_job.parquet")
    quantum_artifact_df = pd.read_parquet(DATA_DIR / "silver" / "quantum_artifact.parquet")
    mixture_core_df = pd.read_parquet(DATA_DIR / "silver" / "mixture_core.parquet")
    mixture_composition_df = pd.read_parquet(DATA_DIR / "silver" / "mixture_composition.parquet")
    active_learning_queue_df = pd.read_parquet(DATA_DIR / "gold" / "active_learning_queue.parquet")
    active_learning_decision_log_df = pd.read_parquet(DATA_DIR / "gold" / "active_learning_decision_log.parquet")
    canonical_observation_df = pd.read_parquet(DATA_DIR / "silver" / "property_observation_canonical.parquet")
    canonical_recommended_df = pd.read_parquet(DATA_DIR / "gold" / "property_recommended_canonical.parquet")
    canonical_recommended_strict_df = pd.read_parquet(DATA_DIR / "gold" / "property_recommended_canonical_strict.parquet")
    canonical_review_queue_df = pd.read_parquet(DATA_DIR / "gold" / "property_recommended_canonical_review_queue.parquet")
    readiness_df = pd.read_parquet(DATA_DIR / "gold" / "property_modeling_readiness_rules.parquet")
    source_manifest_df = pd.read_parquet(DATA_DIR / "bronze" / "source_manifest.parquet")
    molecule_core_df = pd.read_parquet(DATA_DIR / "silver" / "molecule_core.parquet")
    seed_catalog_df = pd.read_csv(DATA_DIR / "raw" / "manual" / "seed_catalog.csv").fillna("")
    quality_report = _build_inventory_convergence(seed_catalog_df, molecule_core_df, recommended_df)
    quality_report_payload = _load_quality_report()
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

    _validate_condition_sets(results, property_observation_df, observation_condition_set_df)
    _validate_cycle_operating_points(results, property_observation_df, observation_condition_set_df, cycle_case_df, cycle_operating_point_df)
    _validate_mixtures(
        results,
        mixture_core_df,
        mixture_composition_df,
        molecule_core_df,
        property_matrix_df,
        model_ready_df,
        model_index_df,
    )
    _validate_active_learning(
        results,
        active_learning_queue_df,
        active_learning_decision_log_df,
        molecule_core_df,
        source_manifest_df,
    )
    _validate_dataset_version(results, quality_report_payload)
    migration_report = validate_dataset_migrations(PROJECT_ROOT)
    results["migration_checks"] = migration_report
    if migration_report["errors"]:
        results["errors"].extend(f"Dataset migration registry: {error}" for error in migration_report["errors"])
    else:
        results["integration_checks"].append("Dataset migration registry: current VERSION covered")
    _validate_quantum_pilot(
        results,
        property_observation_df,
        recommended_df,
        source_manifest_df,
        observation_condition_set_df,
        quantum_job_df,
        quantum_artifact_df,
        property_matrix_df,
        model_ready_df,
        model_index_df,
    )
    _validate_proxy_features(
        results,
        property_observation_df,
        recommended_df,
        source_manifest_df,
        property_matrix_df,
        model_ready_df,
        model_index_df,
    )

    readiness_reference_validation = validate_readiness_rule_references(schema_dir=SCHEMA_DIR)
    _check(
        results,
        readiness_reference_validation["valid"],
        "Research task readiness rules: canonical feature references resolve",
        (
            "Research task readiness rules invalid: "
            f"missing={readiness_reference_validation['missing_references']}; "
            f"invalid_filters={readiness_reference_validation.get('invalid_filters', [])}"
        ),
    )
    readiness_report_df, readiness_summary = evaluate_research_task_readiness_from_paths(data_dir=DATA_DIR, schema_dir=SCHEMA_DIR)
    readiness_report_df.to_parquet(DATA_DIR / "gold" / "research_task_readiness_report.parquet", index=False)

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
    results["condition_migration_progress"] = _condition_migration_progress(property_observation_df, observation_condition_set_df)
    results["cycle_operating_point_summary"] = _cycle_operating_point_summary(property_observation_df, cycle_case_df, cycle_operating_point_df)
    results["mixture_summary"] = mixture_summary(mixture_composition_df, mixture_core_df, molecule_core_df)
    quality_active_summary = quality_report_payload.get("active_learning_summary", {}) if isinstance(quality_report_payload, dict) else {}
    active_learning_input_path = (
        Path(quality_active_summary.get("input_path"))
        if quality_active_summary.get("input_path")
        else DATA_DIR / "raw" / "manual" / "active_learning_queue.csv"
    )
    results["active_learning_summary"] = active_learning_summary(
        active_learning_queue_df,
        active_learning_decision_log_df,
        input_exists=not active_learning_queue_df.empty or not active_learning_decision_log_df.empty,
        input_path=active_learning_input_path,
        input_row_count=int(len(active_learning_queue_df)),
        decision_log_path=DATA_DIR / "raw" / "manual" / "active_learning_decision_log.csv",
        decision_input_row_count=int(len(active_learning_decision_log_df)),
        queue_input_exists=not active_learning_queue_df.empty,
        decision_input_exists=not active_learning_decision_log_df.empty,
    )
    results["proxy_feature_summary"] = proxy_feature_summary(property_observation_df)
    results["quantum_pilot_summary"] = quantum_pilot_summary(
        input_exists=not quantum_job_df.empty or not quantum_artifact_df.empty or not _quantum_observation_rows(property_observation_df).empty,
        property_rows=_quantum_observation_rows(property_observation_df).to_dict(orient="records"),
        quantum_job=quantum_job_df,
        quantum_artifact=quantum_artifact_df,
    )
    quality_quantum_summary = quality_report_payload.get("quantum_pilot_summary", {}) if isinstance(quality_report_payload, dict) else {}
    results["quantum_pilot_summary"] = _merge_quality_quantum_summary(
        results["quantum_pilot_summary"],
        quality_quantum_summary,
    )
    results["research_task_readiness"] = {
        "summary": readiness_summary,
        "rule_reference_validation": readiness_reference_validation,
        "rules": readiness_report_df.to_dict(orient="records"),
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


def _merge_quality_quantum_summary(
    validation_summary: dict[str, Any],
    quality_quantum_summary: dict[str, Any] | object,
) -> dict[str, Any]:
    """Carry request-manifest and configured-input facts into validation output.

    Validation derives observation/job/artifact counts from persisted Parquet
    tables.  Request manifests are raw/generated CSV artifacts, so their
    executor status and source-selection metadata live in the quality report
    produced during the build.  Preserve those fields instead of replacing the
    validation quantum summary with a lossy table-only view.
    """

    merged = dict(validation_summary)
    if not isinstance(quality_quantum_summary, dict):
        return merged

    for key in ("input_status", "input_path", "input_row_count"):
        value = quality_quantum_summary.get(key)
        if value not in {None, ""}:
            merged[key] = value

    if "request_manifest" in quality_quantum_summary:
        merged["request_manifest"] = quality_quantum_summary["request_manifest"]
    return merged


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


def _load_quality_report() -> dict[str, Any]:
    path = DATA_DIR / "gold" / "quality_report.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _validate_property_governance_extension_mirror() -> dict[str, Any] | None:
    bundle_path = default_bundle_path(PROJECT_ROOT)
    extension_manifest_path = DATA_DIR / "extensions" / "property_governance_20260422" / "extension_manifest.parquet"
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


def _validate_dataset_version(results: dict[str, Any], quality_report_payload: dict[str, Any]) -> None:
    version_path = DATA_DIR / "gold" / "VERSION"
    _check(results, version_path.exists(), "dataset VERSION: file exists", f"Missing dataset version file: {version_path}")
    if not version_path.exists():
        return
    version = version_path.read_text(encoding="utf-8").strip()
    report_version = str(quality_report_payload.get("dataset_version", "")).strip()
    _check(results, bool(version), "dataset VERSION: non-empty", "Dataset VERSION file is empty")
    if report_version:
        _check(
            results,
            version == report_version,
            "dataset VERSION: matches quality_report dataset_version",
            f"Dataset VERSION {version!r} does not match quality_report dataset_version {report_version!r}",
        )


def _validate_mixtures(
    results: dict[str, Any],
    mixture_core_df: pd.DataFrame,
    mixture_composition_df: pd.DataFrame,
    molecule_core_df: pd.DataFrame,
    property_matrix_df: pd.DataFrame,
    model_ready_df: pd.DataFrame,
    model_index_df: pd.DataFrame,
) -> None:
    mixture_ids = mixture_core_df.get("mixture_id", pd.Series(dtype="object")).fillna("").astype(str)
    duplicate_mixtures = sorted(mixture_ids.loc[mixture_ids.duplicated() & mixture_ids.str.len().gt(0)].unique().tolist())
    _check(results, not duplicate_mixtures, "mixture_core: mixture_id unique", f"Duplicate mixture_id values: {duplicate_mixtures[:10]}")

    composition_ids = mixture_composition_df.get("mixture_id", pd.Series(dtype="object")).fillna("").astype(str)
    known_mixtures = set(mixture_ids.tolist())
    known_mixtures.discard("")
    observed_mixtures = set(composition_ids.tolist())
    observed_mixtures.discard("")
    dangling_mixtures = sorted(observed_mixtures - known_mixtures)
    _check(results, not dangling_mixtures, "mixture_composition: mixture_id references resolve", f"Dangling mixture_id values: {dangling_mixtures[:10]}")

    component_ids = mixture_composition_df.get("component_mol_id", pd.Series(dtype="object")).fillna("").astype(str)
    known_mol_ids = set(molecule_core_df.get("mol_id", pd.Series(dtype="object")).fillna("").astype(str).tolist())
    known_mol_ids.discard("")
    observed_components = set(component_ids.tolist())
    observed_components.discard("")
    dangling_components = sorted(observed_components - known_mol_ids)
    _check(
        results,
        not dangling_components,
        "mixture_composition: component_mol_id references molecule_core",
        f"Dangling mixture component_mol_id values: {dangling_components[:10]}",
    )

    basis = mixture_composition_df.get("composition_basis", pd.Series(dtype="object")).fillna("").astype(str)
    invalid_basis = sorted(set(basis.tolist()) - MIXTURE_COMPOSITION_BASIS)
    _check(results, not invalid_basis, "mixture_composition: composition_basis vocabulary valid", f"Invalid mixture composition_basis values: {invalid_basis}")

    fractions = pd.to_numeric(mixture_composition_df.get("fraction_value", pd.Series(dtype="float64")), errors="coerce")
    populated_fraction_mask = fractions.notna()
    _check(
        results,
        bool(fractions.loc[populated_fraction_mask].between(0.0, 1.0).all()),
        "mixture_composition: populated fraction values are in [0, 1]",
        "Mixture fraction_value must be in [0, 1] when populated",
    )
    audit = fraction_sum_audit(mixture_composition_df)
    _check(
        results,
        not audit["error_groups"],
        "mixture_composition: complete fraction groups sum to 1",
        f"Mixture fraction groups do not sum to 1: {audit['error_groups'][:10]}",
    )
    _check(
        results,
        True,
        f"mixture_composition: unresolved fraction groups recorded ({len(audit['unresolved_groups'])})",
        "",
    )

    wide_violations = {
        "property_matrix": sorted(MIXTURE_FORBIDDEN_WIDE_COLUMNS & set(property_matrix_df.columns)),
        "model_ready": sorted(MIXTURE_FORBIDDEN_WIDE_COLUMNS & set(model_ready_df.columns)),
        "model_dataset_index": sorted(MIXTURE_FORBIDDEN_WIDE_COLUMNS & set(model_index_df.columns)),
    }
    _check(
        results,
        not any(wide_violations.values()),
        "mixture tables: wide ML outputs remain unchanged",
        f"Mixture columns leaked into wide ML outputs: {wide_violations}",
    )


def _validate_active_learning(
    results: dict[str, Any],
    queue_df: pd.DataFrame,
    decision_log_df: pd.DataFrame,
    molecule_core_df: pd.DataFrame,
    source_manifest_df: pd.DataFrame,
) -> None:
    if queue_df.empty and decision_log_df.empty:
        _check(results, True, "active learning: no manual queue configured", "")
        return

    manifest_sources = set(source_manifest_df.get("source_id", pd.Series(dtype="object")).fillna("").astype(str).tolist())
    _check(
        results,
        ACTIVE_LEARNING_SOURCE_ID in manifest_sources,
        "active learning: source manifest row exists when configured",
        f"Missing source manifest row for {ACTIVE_LEARNING_SOURCE_ID}",
    )

    entry_ids = queue_df.get("queue_entry_id", pd.Series(dtype="object")).fillna("").astype(str)
    duplicate_entries = sorted(entry_ids.loc[entry_ids.duplicated() & entry_ids.str.len().gt(0)].unique().tolist())
    _check(results, not duplicate_entries, "active_learning_queue: queue_entry_id unique", f"Duplicate queue_entry_id values: {duplicate_entries[:10]}")
    _check(results, bool(entry_ids.str.len().gt(0).all()), "active_learning_queue: queue_entry_id non-empty", "active_learning_queue contains blank queue_entry_id")

    mol_ids = queue_df.get("mol_id", pd.Series(dtype="object")).fillna("").astype(str)
    known_mol_ids = set(molecule_core_df.get("mol_id", pd.Series(dtype="object")).fillna("").astype(str).tolist())
    known_mol_ids.discard("")
    observed_mol_ids = set(mol_ids.tolist())
    observed_mol_ids.discard("")
    _check(results, not sorted(observed_mol_ids - known_mol_ids), "active_learning_queue: mol_id references resolve", f"Active learning queue references missing mol_ids: {sorted(observed_mol_ids - known_mol_ids)[:10]}")

    for column in ["priority_score", "uncertainty_score", "novelty_score", "feasibility_score"]:
        scores = pd.to_numeric(queue_df.get(column, pd.Series(dtype="float64")), errors="coerce")
        _check(
            results,
            bool(scores.notna().all() and scores.between(0.0, 1.0).all()),
            f"active_learning_queue: {column} in [0, 1]",
            f"active_learning_queue {column} must be numeric and in [0, 1]",
        )

    _check_vocab(results, queue_df, "acquisition_strategy", ACQUISITION_STRATEGIES, "active_learning_queue")
    _check_vocab(results, queue_df, "hard_constraint_status", HARD_CONSTRAINT_STATUSES, "active_learning_queue")
    _check_vocab(results, queue_df, "recommended_next_action", RECOMMENDED_NEXT_ACTIONS, "active_learning_queue")
    _check_vocab(results, queue_df, "status", QUEUE_STATUSES, "active_learning_queue")

    decision_ids = decision_log_df.get("decision_id", pd.Series(dtype="object")).fillna("").astype(str)
    if not decision_log_df.empty:
        duplicate_decisions = sorted(decision_ids.loc[decision_ids.duplicated() & decision_ids.str.len().gt(0)].unique().tolist())
        _check(results, not duplicate_decisions, "active_learning_decision_log: decision_id unique", f"Duplicate decision_id values: {duplicate_decisions[:10]}")
        _check(results, bool(decision_ids.str.len().gt(0).all()), "active_learning_decision_log: decision_id non-empty", "active_learning_decision_log contains blank decision_id")
        log_entries = set(decision_log_df.get("queue_entry_id", pd.Series(dtype="object")).fillna("").astype(str).tolist())
        log_entries.discard("")
        known_entries = set(entry_ids.tolist())
        known_entries.discard("")
        _check(results, not sorted(log_entries - known_entries), "active_learning_decision_log: queue_entry_id references resolve", f"Active learning decision log references missing queue entries: {sorted(log_entries - known_entries)[:10]}")
        _check_vocab(results, decision_log_df, "decision_action", DECISION_ACTIONS, "active_learning_decision_log")
        _check_vocab(results, decision_log_df, "decision_status", DECISION_STATUSES, "active_learning_decision_log")


def _check_vocab(
    results: dict[str, Any],
    frame: pd.DataFrame,
    column: str,
    vocabulary: set[str],
    table_name: str,
) -> None:
    values = set(frame.get(column, pd.Series(dtype="object")).fillna("").astype(str).tolist())
    invalid = sorted(values - vocabulary)
    _check(results, not invalid, f"{table_name}: {column} vocabulary valid", f"{table_name} {column} contains invalid values: {invalid}")


def _validate_condition_sets(
    results: dict[str, Any],
    property_observation_df: pd.DataFrame,
    observation_condition_set_df: pd.DataFrame,
) -> None:
    condition_ids = observation_condition_set_df["condition_set_id"].fillna("").astype(str) if "condition_set_id" in observation_condition_set_df.columns else pd.Series([], dtype="object")
    duplicate_ids = sorted(condition_ids.loc[condition_ids.duplicated() & condition_ids.str.len().gt(0)].unique().tolist())
    _check(results, not duplicate_ids, "observation_condition_set: condition_set_id unique", f"Duplicate condition_set_id values: {duplicate_ids[:10]}")

    if "condition_set_id" not in property_observation_df.columns:
        _check(results, False, "property_observation: condition_set_id column present", "property_observation missing condition_set_id")
        return

    observed_ids = set(property_observation_df["condition_set_id"].fillna("").astype(str))
    observed_ids.discard("")
    known_ids = set(condition_ids.tolist())
    known_ids.discard("")
    dangling = sorted(observed_ids - known_ids)
    _check(results, not dangling, "property_observation: condition_set_id references resolve", f"Dangling condition_set_id values: {dangling[:10]}")
    progress = _condition_migration_progress(property_observation_df, observation_condition_set_df)
    _check(
        results,
        True,
        f"condition migration: manual review rows recorded ({progress['needs_manual_review']})",
        "",
    )


def _validate_cycle_operating_points(
    results: dict[str, Any],
    property_observation_df: pd.DataFrame,
    observation_condition_set_df: pd.DataFrame,
    cycle_case_df: pd.DataFrame,
    cycle_operating_point_df: pd.DataFrame,
) -> None:
    cycle_case_ids = cycle_case_df["cycle_case_id"].fillna("").astype(str) if "cycle_case_id" in cycle_case_df.columns else pd.Series([], dtype="object")
    duplicate_cases = sorted(cycle_case_ids.loc[cycle_case_ids.duplicated() & cycle_case_ids.str.len().gt(0)].unique().tolist())
    _check(results, not duplicate_cases, "cycle_case: cycle_case_id unique", f"Duplicate cycle_case_id values: {duplicate_cases[:10]}")

    operating_hashes = (
        cycle_operating_point_df["operating_point_hash"].fillna("").astype(str)
        if "operating_point_hash" in cycle_operating_point_df.columns
        else pd.Series([], dtype="object")
    )
    duplicate_hashes = sorted(operating_hashes.loc[operating_hashes.duplicated() & operating_hashes.str.len().gt(0)].unique().tolist())
    _check(results, not duplicate_hashes, "cycle_operating_point: operating_point_hash unique", f"Duplicate operating_point_hash values: {duplicate_hashes[:10]}")

    case_operating_hashes = set(cycle_case_df.get("operating_point_hash", pd.Series(dtype="object")).fillna("").astype(str).tolist())
    point_hashes = set(operating_hashes.tolist())
    case_operating_hashes.discard("")
    point_hashes.discard("")
    missing_points = sorted(case_operating_hashes - point_hashes)
    _check(results, not missing_points, "cycle_case: operating_point_hash references resolve", f"cycle_case rows reference missing operating points: {missing_points[:10]}")

    cycle_rows = _cycle_observation_rows(property_observation_df)
    flags = cycle_rows.get("qc_flags", pd.Series("", index=cycle_rows.index)).fillna("").astype(str)
    resolved_rows = cycle_rows.loc[~flags.str.contains("cycle_unresolved", na=False)]
    if resolved_rows.empty:
        _check(results, True, "cycle observations: no resolved rows requiring references", "")
        return

    missing_required = resolved_rows.loc[
        resolved_rows["cycle_case_id"].fillna("").astype(str).eq("")
        | resolved_rows["operating_point_hash"].fillna("").astype(str).eq("")
        | resolved_rows["condition_set_id"].fillna("").astype(str).eq("")
    ]
    _check(
        results,
        missing_required.empty,
        "cycle observations: resolved rows carry cycle_case_id, operating_point_hash, and condition_set_id",
        f"Resolved cycle rows missing structured references: {missing_required['observation_id'].head(10).tolist()}",
    )

    known_cases = set(cycle_case_ids.tolist())
    known_cases.discard("")
    known_conditions = set(observation_condition_set_df["condition_set_id"].fillna("").astype(str).tolist())
    known_conditions.discard("")
    observed_cases = set(resolved_rows["cycle_case_id"].fillna("").astype(str).tolist())
    observed_hashes = set(resolved_rows["operating_point_hash"].fillna("").astype(str).tolist())
    observed_conditions = set(resolved_rows["condition_set_id"].fillna("").astype(str).tolist())
    observed_cases.discard("")
    observed_hashes.discard("")
    observed_conditions.discard("")
    _check(results, not sorted(observed_cases - known_cases), "cycle observations: cycle_case_id references resolve", f"Dangling cycle_case_id values: {sorted(observed_cases - known_cases)[:10]}")
    _check(results, not sorted(observed_hashes - point_hashes), "cycle observations: operating_point_hash references resolve", f"Dangling operating_point_hash values: {sorted(observed_hashes - point_hashes)[:10]}")
    _check(results, not sorted(observed_conditions - known_conditions), "cycle observations: condition_set_id references resolve", f"Dangling cycle condition_set_id values: {sorted(observed_conditions - known_conditions)[:10]}")


def _cycle_observation_rows(property_observation_df: pd.DataFrame) -> pd.DataFrame:
    if property_observation_df.empty:
        return property_observation_df.copy()
    cycle_props = {"cop_standard_cycle", "volumetric_cooling_mjm3", "pressure_ratio", "discharge_temperature_c"}
    return property_observation_df.loc[
        property_observation_df["property_name"].astype(str).isin(cycle_props)
        | property_observation_df["phase"].fillna("").astype(str).str.lower().eq("cycle")
    ].copy()


def _cycle_operating_point_summary(
    property_observation_df: pd.DataFrame,
    cycle_case_df: pd.DataFrame,
    cycle_operating_point_df: pd.DataFrame,
) -> dict[str, Any]:
    cycle_rows = _cycle_observation_rows(property_observation_df)
    if cycle_rows.empty:
        return {
            "cycle_observation_count": 0,
            "resolved_cycle_observation_count": 0,
            "unresolved_cycle_observation_count": 0,
            "cycle_case_count": int(len(cycle_case_df)),
            "cycle_operating_point_count": int(len(cycle_operating_point_df)),
        }
    flags = cycle_rows.get("qc_flags", pd.Series("", index=cycle_rows.index)).fillna("").astype(str)
    unresolved = int(flags.str.contains("cycle_unresolved", na=False).sum())
    return {
        "cycle_observation_count": int(len(cycle_rows)),
        "resolved_cycle_observation_count": int(len(cycle_rows) - unresolved),
        "unresolved_cycle_observation_count": unresolved,
        "cycle_case_count": int(len(cycle_case_df)),
        "cycle_operating_point_count": int(len(cycle_operating_point_df)),
    }


def _validate_quantum_pilot(
    results: dict[str, Any],
    property_observation_df: pd.DataFrame,
    recommended_df: pd.DataFrame,
    source_manifest_df: pd.DataFrame,
    observation_condition_set_df: pd.DataFrame,
    quantum_job_df: pd.DataFrame,
    quantum_artifact_df: pd.DataFrame,
    property_matrix_df: pd.DataFrame,
    model_ready_df: pd.DataFrame,
    model_index_df: pd.DataFrame,
) -> None:
    quantum_rows = _quantum_observation_rows(property_observation_df)
    configured = not quantum_rows.empty or not quantum_job_df.empty or not quantum_artifact_df.empty
    if not configured:
        _check(results, True, "quantum pilot: no offline rows configured", "")
        _validate_quantum_wide_boundary(results, property_matrix_df, model_ready_df, model_index_df)
        return

    manifest_sources = set(source_manifest_df.get("source_id", pd.Series(dtype="object")).fillna("").astype(str).tolist())
    _check(
        results,
        QUANTUM_SOURCE_ID in manifest_sources,
        "quantum pilot: source manifest row exists when configured",
        f"Missing source manifest row for {QUANTUM_SOURCE_ID}",
    )

    if not quantum_job_df.empty:
        request_ids = quantum_job_df.get("request_id", pd.Series(dtype="object")).fillna("").astype(str)
        duplicate_request_ids = sorted(request_ids.loc[request_ids.duplicated() & request_ids.str.len().gt(0)].unique().tolist())
        _check(results, not duplicate_request_ids, "quantum_job: request_id unique", f"Duplicate quantum request_id values: {duplicate_request_ids[:10]}")
        _check(results, bool(request_ids.str.len().gt(0).all()), "quantum_job: request_id non-empty", "quantum_job contains blank request_id")
        _check(
            results,
            set(quantum_job_df.get("source_id", pd.Series(dtype="object")).fillna("").astype(str).tolist()) <= {QUANTUM_SOURCE_ID},
            "quantum_job: source_id is quantum pilot source",
            "quantum_job contains unexpected source_id values",
        )
        statuses = set(quantum_job_df.get("status", pd.Series(dtype="object")).fillna("").astype(str).tolist())
        _check(results, statuses <= {"succeeded", "failed"}, "quantum_job: status vocabulary valid", f"Unexpected quantum_job statuses: {sorted(statuses)}")
        qualities = set(quantum_job_df.get("quality_level", pd.Series(dtype="object")).fillna("").astype(str).tolist())
        _check(results, qualities <= QUANTUM_QUALITY_LEVELS, "quantum_job: quality level vocabulary valid", f"Unexpected quantum_job quality levels: {sorted(qualities - QUANTUM_QUALITY_LEVELS)}")
        succeeded = quantum_job_df.loc[quantum_job_df.get("status", pd.Series(dtype="object")).fillna("").astype(str).eq("succeeded")]
        if not succeeded.empty:
            converged = pd.to_numeric(succeeded.get("converged", pd.Series(dtype="float64")), errors="coerce").fillna(0).astype(int)
            imag = pd.to_numeric(succeeded.get("imaginary_frequency_count", pd.Series(dtype="float64")), errors="coerce").fillna(-1).astype(int)
            derived = pd.to_numeric(succeeded.get("derived_observation_count", pd.Series(dtype="float64")), errors="coerce").fillna(0).astype(int)
            _check(
                results,
                bool(converged.eq(1).all() and imag.eq(0).all() and derived.gt(0).all()),
                "quantum_job: succeeded jobs are converged and produce observations",
                "Succeeded quantum jobs must be converged, have zero imaginary frequencies, and produce observations",
            )

    if not quantum_artifact_df.empty:
        artifact_ids = quantum_artifact_df.get("artifact_id", pd.Series(dtype="object")).fillna("").astype(str)
        duplicate_artifact_ids = sorted(artifact_ids.loc[artifact_ids.duplicated() & artifact_ids.str.len().gt(0)].unique().tolist())
        _check(results, not duplicate_artifact_ids, "quantum_artifact: artifact_id unique", f"Duplicate quantum artifact_id values: {duplicate_artifact_ids[:10]}")
        _check(results, bool(artifact_ids.str.len().gt(0).all()), "quantum_artifact: artifact_id non-empty", "quantum_artifact contains blank artifact_id")
        artifact_hashes = quantum_artifact_df.get("artifact_sha256", pd.Series(dtype="object")).fillna("").astype(str)
        _check(results, bool(artifact_hashes.str.len().gt(0).all()), "quantum_artifact: artifact hashes present", "quantum_artifact contains blank artifact_sha256")
        known_requests = set(quantum_job_df.get("request_id", pd.Series(dtype="object")).fillna("").astype(str).tolist())
        artifact_requests = set(quantum_artifact_df.get("request_id", pd.Series(dtype="object")).fillna("").astype(str).tolist())
        known_requests.discard("")
        artifact_requests.discard("")
        _check(results, not sorted(artifact_requests - known_requests), "quantum_artifact: request references resolve", f"Dangling quantum artifact request_id values: {sorted(artifact_requests - known_requests)[:10]}")

    if quantum_rows.empty:
        _check(results, True, "quantum pilot: no converged observations selected", "")
        _validate_quantum_wide_boundary(results, property_matrix_df, model_ready_df, model_index_df)
        return

    source_ids = set(quantum_rows.get("source_id", pd.Series("", index=quantum_rows.index)).fillna("").astype(str).tolist())
    _check(results, source_ids == {QUANTUM_SOURCE_ID}, "quantum observations: source_id is quantum pilot source", f"Unexpected quantum source_id values: {sorted(source_ids)}")

    canonical_keys = quantum_rows.get("canonical_feature_key", pd.Series("", index=quantum_rows.index)).fillna("").astype(str)
    _check(
        results,
        set(canonical_keys.tolist()) <= ALL_QUANTUM_CANONICAL_FEATURE_KEYS,
        "quantum observations: canonical feature keys valid",
        f"Unexpected quantum canonical feature keys: {sorted(set(canonical_keys.tolist()) - ALL_QUANTUM_CANONICAL_FEATURE_KEYS)}",
    )
    expected_property_names = canonical_keys.map({key: value["property_name"] for key, value in ALL_QUANTUM_FEATURES.items()}).fillna("")
    property_names = quantum_rows.get("property_name", pd.Series("", index=quantum_rows.index)).fillna("").astype(str)
    _check(
        results,
        bool(property_names.reset_index(drop=True).eq(expected_property_names.reset_index(drop=True)).all()),
        "quantum observations: legacy property names match canonical keys",
        "Quantum observation property_name does not match canonical_feature_key mapping",
    )

    qualities = quantum_rows.get("quality_level", pd.Series("", index=quantum_rows.index)).fillna("").astype(str)
    _check(
        results,
        bool(qualities.isin(QUANTUM_QUALITY_LEVELS).all()),
        "quantum observations: quality level vocabulary valid",
        f"Unexpected quantum observation quality levels: {sorted(set(qualities.tolist()) - QUANTUM_QUALITY_LEVELS)}",
    )
    convergence = pd.to_numeric(quantum_rows.get("convergence_flag", pd.Series(0, index=quantum_rows.index)), errors="coerce").fillna(0).astype(int)
    _check(
        results,
        bool(convergence.eq(1).all()),
        "quantum observations: rows are converged",
        "Quantum observations must have convergence_flag=1",
    )

    condition_ids = quantum_rows.get("condition_set_id", pd.Series("", index=quantum_rows.index)).fillna("").astype(str)
    _check(results, bool(condition_ids.str.len().gt(0).all()), "quantum observations: condition_set_id present", "Quantum observations missing condition_set_id")
    conditions = observation_condition_set_df.set_index("condition_set_id") if "condition_set_id" in observation_condition_set_df.columns else pd.DataFrame()
    known_condition_ids = set(conditions.index.astype(str).tolist()) if not conditions.empty else set()
    observed_condition_ids = set(condition_ids.tolist())
    observed_condition_ids.discard("")
    _check(results, not sorted(observed_condition_ids - known_condition_ids), "quantum observations: condition references resolve", f"Dangling quantum condition_set_id values: {sorted(observed_condition_ids - known_condition_ids)[:10]}")
    if observed_condition_ids and not conditions.empty:
        roles = conditions.loc[list(observed_condition_ids & known_condition_ids), "condition_role"].fillna("").astype(str)
        _check(
            results,
            bool(roles.eq("gas_phase_298k").all()),
            "quantum observations: condition role is gas_phase_298k",
            "Quantum observations must link to gas_phase_298k condition sets",
        )

    observation_request_ids = _quantum_request_ids_from_observations(quantum_rows)
    known_requests = set(quantum_job_df.get("request_id", pd.Series(dtype="object")).fillna("").astype(str).tolist())
    known_requests.discard("")
    _check(results, not sorted(observation_request_ids - known_requests), "quantum observations: job references resolve", f"Quantum observations reference missing jobs: {sorted(observation_request_ids - known_requests)[:10]}")

    artifact_requests = set(quantum_artifact_df.get("request_id", pd.Series(dtype="object")).fillna("").astype(str).tolist())
    artifact_requests.discard("")
    _check(results, not sorted(observation_request_ids - artifact_requests), "quantum observations: artifact references resolve", f"Quantum observations missing artifact rows: {sorted(observation_request_ids - artifact_requests)[:10]}")

    recommended_quantum = (
        recommended_df.loc[recommended_df["property_name"].astype(str).isin(ALL_QUANTUM_PROPERTY_NAMES)]
        if not recommended_df.empty
        else pd.DataFrame()
    )
    _check(
        results,
        set(recommended_quantum.get("property_name", pd.Series(dtype="object")).astype(str).unique().tolist()) == set(property_names.unique().tolist()),
        "quantum observations: property_recommended includes selected quantum properties",
        "property_recommended missing one or more PR-F quantum properties",
    )
    _validate_quantum_wide_boundary(results, property_matrix_df, model_ready_df, model_index_df)


def _validate_quantum_wide_boundary(
    results: dict[str, Any],
    property_matrix_df: pd.DataFrame,
    model_ready_df: pd.DataFrame,
    model_index_df: pd.DataFrame,
) -> None:
    wide_violations = {
        "property_matrix": sorted(QUANTUM_FORBIDDEN_WIDE_COLUMNS & set(property_matrix_df.columns)),
        "model_ready": sorted(QUANTUM_FORBIDDEN_WIDE_COLUMNS & set(model_ready_df.columns)),
        "model_dataset_index": sorted(QUANTUM_FORBIDDEN_WIDE_COLUMNS & set(model_index_df.columns)),
    }
    _check(
        results,
        not any(wide_violations.values()),
        "quantum pilot: wide ML outputs remain unchanged",
        f"Quantum columns leaked into wide ML outputs: {wide_violations}",
    )


def _quantum_observation_rows(property_observation_df: pd.DataFrame) -> pd.DataFrame:
    if property_observation_df.empty or "property_name" not in property_observation_df.columns:
        return pd.DataFrame()
    names = property_observation_df["property_name"].fillna("").astype(str)
    keys = property_observation_df.get("canonical_feature_key", pd.Series("", index=property_observation_df.index)).fillna("").astype(str)
    return property_observation_df.loc[names.isin(ALL_QUANTUM_PROPERTY_NAMES) | keys.isin(ALL_QUANTUM_CANONICAL_FEATURE_KEYS)].copy()


def _quantum_request_ids_from_observations(quantum_rows: pd.DataFrame) -> set[str]:
    records = quantum_rows.get("source_record_id", pd.Series("", index=quantum_rows.index)).fillna("").astype(str)
    return {record.split(":", 1)[0] for record in records.tolist() if record.split(":", 1)[0]}


def _validate_proxy_features(
    results: dict[str, Any],
    property_observation_df: pd.DataFrame,
    recommended_df: pd.DataFrame,
    source_manifest_df: pd.DataFrame,
    property_matrix_df: pd.DataFrame,
    model_ready_df: pd.DataFrame,
    model_index_df: pd.DataFrame,
) -> None:
    proxy_rows = _proxy_observation_rows(property_observation_df)
    _check(results, not proxy_rows.empty, "proxy features: observation rows exist", "Missing PR-E proxy feature observations")

    manifest_sources = set(source_manifest_df.get("source_id", pd.Series(dtype="object")).fillna("").astype(str).tolist())
    _check(
        results,
        PROXY_SOURCE_ID in manifest_sources,
        "proxy features: source manifest row exists",
        f"Missing source manifest row for {PROXY_SOURCE_ID}",
    )
    if proxy_rows.empty:
        return

    source_ids = set(proxy_rows.get("source_id", pd.Series("", index=proxy_rows.index)).fillna("").astype(str).tolist())
    _check(
        results,
        source_ids == {PROXY_SOURCE_ID},
        "proxy features: source_id is deterministic heuristic source",
        f"Unexpected proxy source_id values: {sorted(source_ids)}",
    )

    expected_keys = proxy_rows["property_name"].astype(str).map(PROXY_CANONICAL_FEATURE_KEYS).fillna("")
    actual_keys = proxy_rows.get("canonical_feature_key", pd.Series("", index=proxy_rows.index)).fillna("").astype(str)
    _check(
        results,
        bool(actual_keys.eq(expected_keys).all()),
        "proxy features: canonical feature keys match registry",
        "Proxy feature rows have unexpected canonical_feature_key values",
    )

    proxy_flags = pd.to_numeric(proxy_rows.get("is_proxy_or_screening", pd.Series(0, index=proxy_rows.index)), errors="coerce").fillna(0).astype(int)
    _check(
        results,
        bool(proxy_flags.eq(1).all()),
        "proxy features: rows marked as proxy screening",
        "Proxy feature rows are not all marked is_proxy_or_screening=1",
    )
    ml_status = proxy_rows.get("ml_use_status", pd.Series("", index=proxy_rows.index)).fillna("").astype(str)
    _check(
        results,
        bool(ml_status.eq(PROXY_ML_USE_STATUS).all()),
        "proxy features: ml_use_status marks screening-only use",
        f"Proxy feature rows must use ml_use_status={PROXY_ML_USE_STATUS}",
    )
    quality_scores = pd.to_numeric(proxy_rows.get("data_quality_score_100", pd.Series(dtype="float64")), errors="coerce")
    _check(
        results,
        bool(quality_scores.eq(PROXY_DATA_QUALITY_SCORE).all()),
        "proxy features: data quality score is fixed",
        f"Proxy feature rows must use data_quality_score_100={PROXY_DATA_QUALITY_SCORE}",
    )

    tfa_rows = proxy_rows.loc[proxy_rows["property_name"].astype(str).eq(TFA_RISK_PROPERTY)]
    tfa_values = tfa_rows.get("value", pd.Series("", index=tfa_rows.index)).fillna("unknown").astype(str)
    _check(
        results,
        bool(tfa_values.isin(TFA_RISK_VOCABULARY).all()),
        "proxy features: TFA risk vocabulary valid",
        f"TFA risk proxy values outside vocabulary: {sorted(set(tfa_values) - TFA_RISK_VOCABULARY)}",
    )
    if not tfa_rows.empty:
        expected_scores = tfa_values.map(TFA_RISK_SCORE).astype("float64")
        observed_scores = pd.to_numeric(tfa_rows.get("value_num", pd.Series(dtype="float64")), errors="coerce")
        score_match = observed_scores.fillna(-1).reset_index(drop=True).eq(expected_scores.fillna(-1).reset_index(drop=True)).all()
        _check(results, bool(score_match), "proxy features: TFA numeric helper scores match labels", "TFA risk proxy value_num does not match label")

    synthetic_rows = proxy_rows.loc[proxy_rows["property_name"].astype(str).eq(SYNTHETIC_ACCESSIBILITY_PROPERTY)]
    synthetic_values = pd.to_numeric(synthetic_rows.get("value_num", pd.Series(dtype="float64")), errors="coerce")
    _check(
        results,
        bool(synthetic_values.notna().all() and synthetic_values.between(1.0, 10.0).all()),
        "proxy features: synthetic accessibility scores in range",
        "Synthetic accessibility proxy scores must be numeric and within [1, 10]",
    )

    recommended_proxy = recommended_df.loc[recommended_df["property_name"].astype(str).isin(PROXY_PROPERTIES)] if not recommended_df.empty else pd.DataFrame()
    _check(
        results,
        set(recommended_proxy.get("property_name", pd.Series(dtype="object")).astype(str).unique().tolist()) == PROXY_PROPERTIES,
        "proxy features: property_recommended includes both proxy properties",
        "property_recommended missing one or more PR-E proxy properties",
    )

    forbidden_columns = set(PROXY_PROPERTIES) | {f"has_{name}" for name in PROXY_PROPERTIES}
    wide_violations = {
        "property_matrix": sorted(forbidden_columns & set(property_matrix_df.columns)),
        "model_ready": sorted(forbidden_columns & set(model_ready_df.columns)),
        "model_dataset_index": sorted(forbidden_columns & set(model_index_df.columns)),
    }
    _check(
        results,
        not any(wide_violations.values()),
        "proxy features: wide ML outputs remain unchanged",
        f"Proxy columns leaked into wide ML outputs: {wide_violations}",
    )


def _proxy_observation_rows(property_observation_df: pd.DataFrame) -> pd.DataFrame:
    if property_observation_df.empty or "property_name" not in property_observation_df.columns:
        return pd.DataFrame()
    return property_observation_df.loc[property_observation_df["property_name"].fillna("").astype(str).isin(PROXY_PROPERTIES)].copy()


def _condition_migration_progress(property_observation_df: pd.DataFrame, observation_condition_set_df: pd.DataFrame) -> dict[str, Any]:
    total = int(len(property_observation_df))
    with_condition = (
        int(property_observation_df["condition_set_id"].fillna("").astype(str).str.len().gt(0).sum())
        if total and "condition_set_id" in property_observation_df.columns
        else 0
    )
    unresolved = 0
    status_counts: dict[str, int] = {}
    manual_review_condition_ids: list[str] = []
    if "condition_set_id" in property_observation_df.columns and {"condition_set_id", "normalization_status"}.issubset(observation_condition_set_df.columns):
        status_by_condition = observation_condition_set_df.set_index("condition_set_id")["normalization_status"].fillna("").astype(str).to_dict()
        statuses = (
            property_observation_df["condition_set_id"]
            .fillna("")
            .astype(str)
            .map(status_by_condition)
            .fillna("")
        )
        unresolved = int(
            statuses.eq("unresolved_text").sum()
        )
        status_counts = {str(key): int(value) for key, value in statuses.value_counts().sort_index().to_dict().items()}
        manual_review_condition_ids = (
            observation_condition_set_df.loc[
                observation_condition_set_df["normalization_status"].fillna("").astype(str).eq("unresolved_text"),
                "condition_set_id",
            ]
            .fillna("")
            .astype(str)
            .tolist()
        )
    return {
        "total": total,
        "with_condition_set_id": with_condition,
        "auto_backfilled": max(with_condition - unresolved, 0),
        "needs_manual_review": unresolved,
        "condition_set_count": int(len(observation_condition_set_df)),
        "manual_review_condition_set_count": int(len(manual_review_condition_ids)),
        "manual_review_condition_set_ids_sample": manual_review_condition_ids[:10],
        "normalization_status_counts": status_counts,
        "coverage_fraction": with_condition / total if total else 0.0,
        "auto_backfill_fraction": max(with_condition - unresolved, 0) / total if total else 0.0,
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
