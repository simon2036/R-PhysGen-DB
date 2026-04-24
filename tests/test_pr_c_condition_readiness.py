from __future__ import annotations

import json

import pandas as pd

from r_physgen_db.condition_sets import backfill_condition_sets, canonicalize_condition, condition_id
from r_physgen_db.constants import SCHEMA_DIR
from r_physgen_db.pipeline import _property_observation_columns
from r_physgen_db.readiness import evaluate_research_task_readiness, validate_readiness_rule_references
from r_physgen_db.utils import load_yaml


def test_condition_id_is_stable_for_equivalent_condition() -> None:
    condition = {
        "condition_role": "normal_boiling_point",
        "temperature_value": None,
        "temperature_unit": "",
        "pressure_value": 0.101325,
        "pressure_unit": "MPa",
        "phase": "vapor_liquid_equilibrium",
        "vapor_quality_value": None,
        "vapor_quality_basis": "",
        "composition_basis": "pure",
        "mixture_composition_json": "",
        "cycle_case_id": "",
        "operating_point_hash": "",
        "reference_state": "",
    }

    first_id, first_signature = condition_id(condition)
    second_id, second_signature = condition_id(dict(condition))

    assert first_id == second_id
    assert first_signature == second_signature
    assert first_id.startswith("cond_")


def test_condition_canonicalization_roles() -> None:
    assert canonicalize_condition({"property_name": "boiling_point_c"})["condition_role"] == "normal_boiling_point"
    assert canonicalize_condition({"property_name": "critical_temp_c"})["condition_role"] == "critical_point"
    assert canonicalize_condition({"property_name": "cop_standard_cycle"})["condition_role"] == "cycle_operating_point"
    assert (
        canonicalize_condition({"property_name": "heat_capacity_gas_jmolK", "temperature": "298.15 K", "phase": "gas"})[
            "condition_role"
        ]
        == "gas_phase_298k"
    )
    assert canonicalize_condition({"property_name": "unknown"})["normalization_status"] == "unresolved_text"


def test_backfill_condition_sets_adds_ids_and_unique_condition_table() -> None:
    observation = pd.DataFrame(
        [
            {
                "observation_id": "obs_1",
                "mol_id": "mol_1",
                "property_name": "boiling_point_c",
                "value": "10",
                "value_num": 10.0,
                "unit": "degC",
                "temperature": "",
                "pressure": "0.101325 MPa",
                "phase": "vapor-liquid_equilibrium",
                "source_id": "source_a",
            },
            {
                "observation_id": "obs_2",
                "mol_id": "mol_1",
                "property_name": "critical_pressure_mpa",
                "value": "4",
                "value_num": 4.0,
                "unit": "MPa",
                "temperature": "",
                "pressure": "",
                "phase": "",
                "source_id": "source_a",
            },
        ]
    )

    backfilled, condition_set, progress = backfill_condition_sets(observation)

    assert backfilled["condition_set_id"].notna().all()
    assert condition_set["condition_set_id"].is_unique
    assert progress["with_condition_set_id"] == len(observation)
    assert progress["normalization_status_counts"]["inferred_default"] == 2
    assert progress["needs_manual_review"] == 0
    assert {"condition_set_id", "standard_value_num", "value_parse_status"}.issubset(backfilled.columns)


def test_pr_c_active_schema_and_pipeline_columns_are_registered() -> None:
    for name in [
        "canonical_feature_registry.yaml",
        "normalization_rules.yaml",
        "observation_condition_set.yaml",
        "research_task_readiness_rules.yaml",
    ]:
        assert (SCHEMA_DIR / name).exists()

    schema = load_yaml(SCHEMA_DIR / "property_observation.yaml")
    schema_columns = {column["name"] for column in schema["columns"]}
    pipeline_columns = set(_property_observation_columns())
    expected = {
        "condition_set_id",
        "value_text_normalized",
        "value_num_lower",
        "value_num_upper",
        "value_num_bound_type",
        "value_parse_status",
        "standard_value_num",
        "source_record_id",
        "ingestion_stage_id",
        "normalization_rule_id",
    }
    assert expected.issubset(schema_columns)
    assert expected.issubset(pipeline_columns)


def test_research_readiness_reference_validation_passes_for_active_rules() -> None:
    result = validate_readiness_rule_references(schema_dir=SCHEMA_DIR)

    assert result["valid"]
    assert result["rule_count"] >= 1


def test_research_readiness_evaluates_passed_and_failed_rules(tmp_path) -> None:
    schema_dir = tmp_path / "schemas"
    schema_dir.mkdir()
    (schema_dir / "canonical_feature_registry.yaml").write_text(
        """
table_name: canonical_feature_registry
registry:
  - canonical_feature_key: identity.mol_id
    legacy_property_name: mol_id
    aliases_json: '["mol_id"]'
  - canonical_feature_key: thermodynamic.normal_boiling_temperature
    legacy_property_name: boiling_point_c
    aliases_json: '["boiling_point_c"]'
  - canonical_feature_key: thermodynamic.critical_temperature
    legacy_property_name: critical_temp_c
    aliases_json: '["critical_temp_c"]'
""",
        encoding="utf-8",
    )
    (schema_dir / "research_task_readiness_rules.yaml").write_text(
        """
table_name: research_task_readiness_rules
rules:
  - readiness_rule_id: pass_rule
    task_name: pass_rule
    task_scope: unit
    entity_scope_filter: refrigerant_or_candidate
    model_inclusion_filter: any
    source_layer: property_recommended_canonical_or_legacy_recommended
    minimum_molecule_count: 2
    minimum_should_have_coverage: 0.50
    allow_proxy_rows: 1
    require_numeric_values: 1
    require_source_traceability: 1
    require_strict_layer: 0
    must_have:
      - {canonical_feature_key: identity.mol_id, required_coverage: 1.00, value_requirement: non_null}
      - {canonical_feature_key: thermodynamic.normal_boiling_temperature, required_coverage: 1.00, value_requirement: numeric}
    should_have:
      - {canonical_feature_key: thermodynamic.critical_temperature, target_coverage: 0.50, value_requirement: numeric}
  - readiness_rule_id: fail_rule
    task_name: fail_rule
    task_scope: unit
    entity_scope_filter: refrigerant_or_candidate
    model_inclusion_filter: any
    source_layer: property_recommended_canonical_or_legacy_recommended
    minimum_molecule_count: 3
    minimum_should_have_coverage: 0.50
    allow_proxy_rows: 1
    require_numeric_values: 1
    require_source_traceability: 1
    require_strict_layer: 0
    must_have:
      - {canonical_feature_key: thermodynamic.normal_boiling_temperature, required_coverage: 1.00, value_requirement: numeric}
    should_have: []
""",
        encoding="utf-8",
    )
    frames = {
        "molecule_core": pd.DataFrame(
            [
                {"mol_id": "mol_1", "entity_scope": "candidate", "molecular_weight": 10.0},
                {"mol_id": "mol_2", "entity_scope": "candidate", "molecular_weight": 20.0},
            ]
        ),
        "property_recommended": pd.DataFrame(
            [
                {
                    "mol_id": "mol_1",
                    "property_name": "boiling_point_c",
                    "value": "1",
                    "value_num": 1.0,
                    "selected_source_id": "source_a",
                },
                {
                    "mol_id": "mol_2",
                    "property_name": "boiling_point_c",
                    "value": "2",
                    "value_num": 2.0,
                    "selected_source_id": "source_a",
                },
                {
                    "mol_id": "mol_1",
                    "property_name": "critical_temp_c",
                    "value": "3",
                    "value_num": 3.0,
                    "selected_source_id": "source_a",
                },
            ]
        ),
        "property_recommended_canonical": pd.DataFrame(),
        "property_recommended_canonical_strict": pd.DataFrame(),
        "model_ready": pd.DataFrame(),
    }

    report, summary = evaluate_research_task_readiness(frames=frames, schema_dir=schema_dir)
    statuses = dict(zip(report["readiness_rule_id"], report["status"], strict=True))

    assert statuses == {"pass_rule": "passed", "fail_rule": "failed"}
    assert summary["status_counts"]["passed"] == 1
    assert json.loads(report.loc[report["readiness_rule_id"] == "fail_rule", "hard_failures_json"].iloc[0])


def test_readiness_filters_support_yaml_boolean_yes_and_seed_tiers(tmp_path) -> None:
    schema_dir = tmp_path / "schemas"
    schema_dir.mkdir()
    (schema_dir / "canonical_feature_registry.yaml").write_text(
        """
table_name: canonical_feature_registry
registry:
  - canonical_feature_key: identity.mol_id
    legacy_property_name: mol_id
    aliases_json: '["mol_id"]'
""",
        encoding="utf-8",
    )
    (schema_dir / "research_task_readiness_rules.yaml").write_text(
        """
table_name: research_task_readiness_rules
rules:
  - readiness_rule_id: promoted_tier_a
    task_name: promoted_tier_a
    task_scope: unit
    entity_scope_filter: refrigerant_or_candidate
    model_inclusion_filter: yes
    coverage_tier_filter: [A]
    source_layer: model_ready_plus_property_recommended
    minimum_molecule_count: 1
    minimum_should_have_coverage: 1.0
    allow_proxy_rows: 1
    require_numeric_values: 0
    require_source_traceability: 1
    require_strict_layer: 0
    must_have:
      - {canonical_feature_key: identity.mol_id, required_coverage: 1.00, value_requirement: non_null}
    should_have: []
""",
        encoding="utf-8",
    )
    frames = {
        "molecule_core": pd.DataFrame(
            [
                {"mol_id": "mol_1", "seed_id": "seed_1", "entity_scope": "candidate", "model_inclusion": "yes"},
                {"mol_id": "mol_2", "seed_id": "seed_2", "entity_scope": "candidate", "model_inclusion": "no"},
            ]
        ),
        "model_ready": pd.DataFrame(
            [
                {"mol_id": "mol_1", "seed_id": "seed_1", "entity_scope": "candidate", "model_inclusion": "yes"},
                {"mol_id": "mol_2", "seed_id": "seed_2", "entity_scope": "candidate", "model_inclusion": "no"},
            ]
        ),
        "seed_catalog": pd.DataFrame(
            [
                {"seed_id": "seed_1", "coverage_tier": "A"},
                {"seed_id": "seed_2", "coverage_tier": "B"},
            ]
        ),
        "property_recommended": pd.DataFrame(),
        "property_recommended_canonical": pd.DataFrame(),
        "property_recommended_canonical_strict": pd.DataFrame(),
    }

    report, _ = evaluate_research_task_readiness(frames=frames, schema_dir=schema_dir)

    row = report.iloc[0]
    assert row["status"] == "passed"
    assert row["molecule_count"] == 1


def test_readiness_filter_missing_source_column_fails_explicitly(tmp_path) -> None:
    schema_dir = tmp_path / "schemas"
    schema_dir.mkdir()
    (schema_dir / "canonical_feature_registry.yaml").write_text(
        """
table_name: canonical_feature_registry
registry:
  - canonical_feature_key: identity.mol_id
    legacy_property_name: mol_id
    aliases_json: '["mol_id"]'
""",
        encoding="utf-8",
    )
    (schema_dir / "research_task_readiness_rules.yaml").write_text(
        """
table_name: research_task_readiness_rules
rules:
  - readiness_rule_id: missing_model_inclusion
    task_name: missing_model_inclusion
    task_scope: unit
    entity_scope_filter: any
    model_inclusion_filter: "yes"
    source_layer: molecule_core_plus_property_recommended
    minimum_molecule_count: 1
    minimum_should_have_coverage: 1.0
    allow_proxy_rows: 1
    require_numeric_values: 0
    require_source_traceability: 1
    require_strict_layer: 0
    must_have:
      - {canonical_feature_key: identity.mol_id, required_coverage: 1.00, value_requirement: non_null}
    should_have: []
""",
        encoding="utf-8",
    )
    frames = {
        "molecule_core": pd.DataFrame([{"mol_id": "mol_1"}]),
        "property_recommended": pd.DataFrame(),
        "property_recommended_canonical": pd.DataFrame(),
        "property_recommended_canonical_strict": pd.DataFrame(),
        "model_ready": pd.DataFrame(),
        "seed_catalog": pd.DataFrame(),
    }

    report, _ = evaluate_research_task_readiness(frames=frames, schema_dir=schema_dir)
    hard_failures = json.loads(report.iloc[0]["hard_failures_json"])

    assert report.iloc[0]["status"] == "failed"
    assert any("requires model_inclusion column" in item for item in hard_failures)
