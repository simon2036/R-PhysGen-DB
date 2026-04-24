from __future__ import annotations

import pandas as pd

from r_physgen_db.chemistry import compute_screening_features
from r_physgen_db.condition_sets import backfill_condition_sets
from r_physgen_db.constants import SCHEMA_DIR
from r_physgen_db.pipeline import _build_model_dataset_index, _build_model_ready, _build_property_matrix, _build_quality_report
from r_physgen_db.proxy_features import (
    PROXY_CANONICAL_FEATURE_KEYS,
    PROXY_DATA_QUALITY_SCORE,
    PROXY_ML_USE_STATUS,
    PROXY_PROPERTIES,
    PROXY_SOURCE_ID,
    SYNTHETIC_ACCESSIBILITY_PROPERTY,
    TFA_RISK_PROPERTY,
    build_proxy_feature_rows,
    proxy_feature_summary,
    synthetic_accessibility_score,
)
from r_physgen_db.readiness import evaluate_research_task_readiness, validate_readiness_rule_references
from r_physgen_db.validate import _validate_proxy_features


def test_tfa_proxy_classification_is_stable() -> None:
    rows, _ = build_proxy_feature_rows(
        pd.DataFrame(
            [
                {"mol_id": "mol_hc", "isomeric_smiles": "CC", "canonical_smiles": "CC"},
                {"mol_id": "mol_f", "isomeric_smiles": "CC(F)F", "canonical_smiles": "CC(F)F"},
                {"mol_id": "mol_high", "isomeric_smiles": "C=CC(F)(F)F", "canonical_smiles": "C=CC(F)(F)F"},
            ]
        )
    )
    tfa = pd.DataFrame(rows).loc[lambda df: df["property_name"].eq(TFA_RISK_PROPERTY)]
    values = dict(zip(tfa["mol_id"], tfa["value"], strict=True))

    assert values["mol_hc"] == "none"
    assert values["mol_f"] in {"low", "medium"}
    assert values["mol_high"] == "high"


def test_synthetic_accessibility_score_is_bounded_and_complexity_sensitive() -> None:
    simple = synthetic_accessibility_score(compute_screening_features("C"))["value_num"]
    complex_score = synthetic_accessibility_score(compute_screening_features("c1ccccc1C(F)(F)F"))["value_num"]

    assert 1.0 <= simple <= 10.0
    assert 1.0 <= complex_score <= 10.0
    assert complex_score > simple


def test_proxy_rows_include_screening_source_and_canonical_fields() -> None:
    rows, summary = build_proxy_feature_rows(pd.DataFrame([{"mol_id": "mol_a", "isomeric_smiles": "CC(F)F"}]))
    frame = pd.DataFrame(rows)

    assert set(frame["property_name"]) == PROXY_PROPERTIES
    assert set(frame["source_id"]) == {PROXY_SOURCE_ID}
    assert set(frame["is_proxy_or_screening"]) == {1}
    assert set(frame["ml_use_status"]) == {PROXY_ML_USE_STATUS}
    assert set(frame["data_quality_score_100"]) == {PROXY_DATA_QUALITY_SCORE}
    assert dict(zip(frame["property_name"], frame["canonical_feature_key"], strict=True)) == PROXY_CANONICAL_FEATURE_KEYS
    assert summary["proxy_observation_count"] == 2
    assert summary["proxy_molecule_count"] == 1


def test_proxy_condition_sets_are_not_applicable_standard_reference_state() -> None:
    rows, _ = build_proxy_feature_rows(pd.DataFrame([{"mol_id": "mol_a", "isomeric_smiles": "CC(F)F"}]))

    backfilled, condition_set, _ = backfill_condition_sets(pd.DataFrame(rows))

    assert backfilled["condition_set_id"].notna().all()
    assert set(condition_set["condition_role"]) == {"standard_reference_state"}
    assert set(condition_set["normalization_status"]) == {"not_applicable"}


def test_quality_report_and_wide_outputs_keep_proxy_boundary() -> None:
    observation = pd.DataFrame(
        [
            {
                "observation_id": "obs_tfa",
                "mol_id": "mol_a",
                "property_name": TFA_RISK_PROPERTY,
                "value": "low",
                "value_num": 1,
                "unit": "categorical",
                "source_id": PROXY_SOURCE_ID,
                "source_type": "derived_harmonized",
                "source_name": "Proxy",
                "quality_level": "snapshot_only",
            },
            {
                "observation_id": "obs_sa",
                "mol_id": "mol_a",
                "property_name": SYNTHETIC_ACCESSIBILITY_PROPERTY,
                "value": "1.500",
                "value_num": 1.5,
                "unit": "dimensionless",
                "source_id": PROXY_SOURCE_ID,
                "source_type": "derived_harmonized",
                "source_name": "Proxy",
                "quality_level": "snapshot_only",
            },
        ]
    )
    recommended = pd.DataFrame(
        [
            {
                "mol_id": "mol_a",
                "property_name": TFA_RISK_PROPERTY,
                "value": "low",
                "value_num": 1,
                "unit": "categorical",
                "selected_source_id": PROXY_SOURCE_ID,
                "selected_source_name": "Proxy",
                "selected_quality_level": "snapshot_only",
            },
            {
                "mol_id": "mol_a",
                "property_name": SYNTHETIC_ACCESSIBILITY_PROPERTY,
                "value": "1.500",
                "value_num": 1.5,
                "unit": "dimensionless",
                "selected_source_id": PROXY_SOURCE_ID,
                "selected_source_name": "Proxy",
                "selected_quality_level": "snapshot_only",
            },
        ]
    )

    report = _build_quality_report(
        seed_catalog=pd.DataFrame([{"seed_id": "seed_a", "entity_scope": "candidate", "coverage_tier": "D"}]),
        molecule_core=pd.DataFrame([{"mol_id": "mol_a", "seed_id": "seed_a", "model_inclusion": "yes"}]),
        property_observation=observation,
        property_recommended=recommended,
        model_ready=pd.DataFrame([{"mol_id": "mol_a", "split": "train"}]),
        qc_issues=pd.DataFrame(),
        resolution_df=pd.DataFrame(columns=["seed_id", "stage", "status", "detail"]),
        regulatory_status=pd.DataFrame(),
        pending_sources=pd.DataFrame(),
    )
    matrix = _build_property_matrix(recommended)
    model_index = _build_model_dataset_index(
        pd.DataFrame([{"mol_id": "mol_a", "scaffold_key": "scaf_a"}]),
        recommended,
        pd.DataFrame([{"mol_id": "mol_a", "model_inclusion": "yes"}]),
    )
    model_ready = _build_model_ready(
        pd.DataFrame([{"mol_id": "mol_a", "canonical_smiles": "CC", "isomeric_smiles": "CC", "selfies": "[C][C]", "scaffold_key": "scaf_a"}]),
        matrix,
        model_index,
    )

    assert report["proxy_feature_summary"]["proxy_observation_count"] == 2
    assert not (PROXY_PROPERTIES & set(matrix.columns))
    assert not (PROXY_PROPERTIES & set(model_ready.columns))
    assert not ({f"has_{name}" for name in PROXY_PROPERTIES} & set(model_index.columns))


def test_proxy_readiness_rule_passes_on_small_fixture() -> None:
    molecule_core = pd.DataFrame(
        [{"mol_id": f"mol_{idx}", "entity_scope": "candidate", "molecular_weight": 100.0 + idx} for idx in range(20)]
    )
    property_recommended = pd.DataFrame(
        [
            {
                "mol_id": row["mol_id"],
                "property_name": property_name,
                "value": "low" if property_name == TFA_RISK_PROPERTY else "2.000",
                "value_num": 1 if property_name == TFA_RISK_PROPERTY else 2.0,
                "selected_source_id": PROXY_SOURCE_ID,
            }
            for row in molecule_core.to_dict(orient="records")
            for property_name in [TFA_RISK_PROPERTY, SYNTHETIC_ACCESSIBILITY_PROPERTY]
        ]
    )

    reference_validation = validate_readiness_rule_references(schema_dir=SCHEMA_DIR)
    report, _ = evaluate_research_task_readiness(
        frames={
            "molecule_core": molecule_core,
            "property_recommended": property_recommended,
            "property_recommended_canonical": pd.DataFrame(),
            "property_recommended_canonical_strict": pd.DataFrame(),
            "model_ready": pd.DataFrame(),
        },
        schema_dir=SCHEMA_DIR,
    )
    status = report.loc[report["readiness_rule_id"].eq("task05_proxy_screening_seed"), "status"].iloc[0]

    assert reference_validation["valid"]
    assert status == "passed"


def test_validate_proxy_features_detects_invalid_rows() -> None:
    property_observation = pd.DataFrame(
        [
            {
                "observation_id": "obs_bad",
                "mol_id": "mol_a",
                "property_name": TFA_RISK_PROPERTY,
                "value": "bad_label",
                "value_num": 1,
                "source_id": PROXY_SOURCE_ID,
                "canonical_feature_key": PROXY_CANONICAL_FEATURE_KEYS[TFA_RISK_PROPERTY],
                "is_proxy_or_screening": 0,
                "ml_use_status": PROXY_ML_USE_STATUS,
                "data_quality_score_100": PROXY_DATA_QUALITY_SCORE,
            },
            {
                "observation_id": "obs_sa",
                "mol_id": "mol_a",
                "property_name": SYNTHETIC_ACCESSIBILITY_PROPERTY,
                "value": "11",
                "value_num": 11.0,
                "source_id": PROXY_SOURCE_ID,
                "canonical_feature_key": PROXY_CANONICAL_FEATURE_KEYS[SYNTHETIC_ACCESSIBILITY_PROPERTY],
                "is_proxy_or_screening": 1,
                "ml_use_status": PROXY_ML_USE_STATUS,
                "data_quality_score_100": PROXY_DATA_QUALITY_SCORE,
            },
        ]
    )
    recommended = property_observation.rename(columns={"source_id": "selected_source_id"})[
        ["mol_id", "property_name", "value", "value_num", "selected_source_id"]
    ]
    results = {"integration_checks": [], "errors": []}

    _validate_proxy_features(
        results,
        property_observation,
        recommended,
        pd.DataFrame([{"source_id": PROXY_SOURCE_ID}]),
        pd.DataFrame(columns=["mol_id", TFA_RISK_PROPERTY]),
        pd.DataFrame(columns=["mol_id"]),
        pd.DataFrame(columns=["mol_id"]),
    )

    assert any("TFA risk proxy values outside vocabulary" in error for error in results["errors"])
    assert any("is_proxy_or_screening=1" in error for error in results["errors"])
    assert any("within [1, 10]" in error for error in results["errors"])
    assert any("Proxy columns leaked into wide ML outputs" in error for error in results["errors"])
