from __future__ import annotations

import pandas as pd

from r_physgen_db.canonical_projection import project_native_canonical_recommendations


def test_native_canonical_projection_preserves_governance_rows_and_adds_strict_rows() -> None:
    recommended = pd.DataFrame(
        [
            {
                "mol_id": "mol_a",
                "property_name": "boiling_point_c",
                "value": "12.5",
                "value_num": 12.5,
                "unit": "degC",
                "selected_source_id": "source_coolprop_session",
                "selected_source_name": "CoolProp",
                "selected_quality_level": "calculated_open_source",
                "source_priority": 80,
                "source_count": 1,
                "conflict_flag": False,
                "conflict_detail": "",
            },
            {
                "mol_id": "mol_a",
                "property_name": "gwp_100yr",
                "value": "42",
                "value_num": 42.0,
                "unit": "dimensionless",
                "selected_source_id": "source_manual",
                "selected_source_name": "Manual",
                "selected_quality_level": "manual_curated_reference",
                "source_priority": 100,
                "source_count": 1,
                "conflict_flag": False,
                "conflict_detail": "",
            },
            {
                "mol_id": "mol_a",
                "property_name": "ashrae_safety",
                "value": "A2L",
                "value_num": None,
                "unit": "class",
                "selected_source_id": "source_safety",
                "selected_source_name": "Safety",
                "selected_quality_level": "manual_curated_reference",
                "source_priority": 100,
                "source_count": 1,
                "conflict_flag": False,
                "conflict_detail": "",
            },
        ]
    )
    existing_canonical = pd.DataFrame(
        [
            {
                "mol_id": "mol_a",
                "canonical_feature_key": "environmental.gwp_100yr",
                "canonical_property_id": "PROP_ENV_GWP100",
                "canonical_property_group": "environmental",
                "canonical_property_name": "gwp_100yr",
                "value": "99",
                "value_num": 99.0,
                "unit": "dimensionless",
                "selected_source_id": "source_governance",
                "selected_source_name": "Governance",
                "selected_quality_level": "manual_curated_reference",
                "source_priority_rank": 1,
                "data_quality_score_100": 95.0,
                "is_proxy_or_screening": False,
                "ml_use_status": "recommended_numeric_candidate",
                "proxy_only_flag": False,
                "nonproxy_candidate_count": 1,
                "top_rank_source_count": 1,
                "source_divergence_flag": False,
                "source_divergence_detail": "",
                "source_count": 1,
                "conflict_flag": False,
                "conflict_detail": "",
            }
        ]
    )
    readiness = pd.DataFrame(
        [
            {
                "readiness_rule_id": "MLRULE_001",
                "canonical_property_id": "PROP_THERMO_NORMAL_BOILING_T",
                "canonical_feature_key": "thermodynamic.normal_boiling_temperature",
                "use_as_ml_feature": 1,
                "use_as_ml_target": 1,
                "minimum_quality_score": 70,
                "exclude_if_proxy_or_screening": 1,
                "preferred_standard_unit": "K",
                "normalization_recommendation": "zscore",
                "missing_value_strategy": "indicator",
                "notes": "test",
            },
            {
                "readiness_rule_id": "MLRULE_017",
                "canonical_property_id": "PROP_ENV_GWP100",
                "canonical_feature_key": "environmental.gwp_100yr",
                "use_as_ml_feature": 1,
                "use_as_ml_target": 1,
                "minimum_quality_score": 70,
                "exclude_if_proxy_or_screening": 1,
                "preferred_standard_unit": "dimensionless",
                "normalization_recommendation": "zscore",
                "missing_value_strategy": "indicator",
                "notes": "test",
            },
            {
                "readiness_rule_id": "MLRULE_030",
                "canonical_property_id": "PROP_SAFETY_GROUP",
                "canonical_feature_key": "safety.safety_group",
                "use_as_ml_feature": 1,
                "use_as_ml_target": 0,
                "minimum_quality_score": 70,
                "exclude_if_proxy_or_screening": 1,
                "preferred_standard_unit": "dimensionless",
                "normalization_recommendation": "one_hot",
                "missing_value_strategy": "unknown",
                "notes": "test",
            },
        ]
    )

    result = project_native_canonical_recommendations(
        property_recommended=recommended,
        existing_canonical_recommended=existing_canonical,
        readiness_rules=readiness,
    )

    combined = result.canonical_recommended
    strict = result.canonical_recommended_strict
    gwp = combined.loc[combined["canonical_feature_key"].eq("environmental.gwp_100yr")].iloc[0]
    safety = strict.loc[strict["canonical_feature_key"].eq("safety.safety_group")].iloc[0]

    assert result.added_count == 2
    assert gwp["value"] == "99"
    assert gwp["selected_source_id"] == "source_governance"
    assert set(strict["canonical_feature_key"]) == {
        "thermodynamic.normal_boiling_temperature",
        "safety.safety_group",
    }
    assert safety["value"] == "A2L"
    assert safety["value_num"] == 2.0
    assert safety["strict_accept_basis"] == "standard"
