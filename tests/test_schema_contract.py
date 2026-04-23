from __future__ import annotations

from pathlib import Path

from r_physgen_db.constants import PROJECT_ROOT, SCHEMA_DIR
from r_physgen_db.utils import load_yaml


def test_property_observation_contract_contains_required_fields() -> None:
    schema = load_yaml(SCHEMA_DIR / "property_observation.yaml")
    names = [column["name"] for column in schema["columns"]]
    for required in [
        "property_name",
        "value",
        "unit",
        "temperature",
        "pressure",
        "phase",
        "source_type",
        "source_name",
        "method",
        "uncertainty",
        "quality_level",
    ]:
        assert required in names


def test_seed_catalog_exists() -> None:
    path = PROJECT_ROOT / "data" / "raw" / "manual" / "seed_catalog.csv"
    assert path.exists()


def test_seed_catalog_has_wave2_columns() -> None:
    path = PROJECT_ROOT / "data" / "raw" / "manual" / "seed_catalog.csv"
    header = path.read_text(encoding="utf-8").splitlines()[0].split(",")
    for required in [
        "coverage_tier",
        "source_bundle",
        "coolprop_support_expected",
        "regulatory_priority",
        "entity_scope",
        "model_inclusion",
    ]:
        assert required in header


def test_wave2_schema_files_exist() -> None:
    for name in [
        "regulatory_status.yaml",
        "pending_sources.yaml",
        "property_observation_canonical.yaml",
        "property_recommended_canonical.yaml",
        "property_recommended_canonical_strict.yaml",
        "property_recommended_canonical_review_queue.yaml",
        "property_dictionary.yaml",
        "property_canonical_map.yaml",
        "unit_conversion_rules.yaml",
        "property_source_priority_rules.yaml",
        "property_modeling_readiness_rules.yaml",
        "property_governance_issues.yaml",
    ]:
        assert (SCHEMA_DIR / name).exists()


def test_property_governance_unresolved_curations_contract_exists() -> None:
    path = PROJECT_ROOT / "data" / "raw" / "manual" / "property_governance_20260422_unresolved_curations.csv"
    assert path.exists()
    header = path.read_text(encoding="utf-8").splitlines()[0].split(",")
    for required in [
        "substance_id",
        "refrigerant_number",
        "cas_number",
        "canonical_smiles",
        "isomeric_smiles",
        "inchi",
        "inchikey",
        "resolution_source",
        "resolution_source_url",
        "resolution_confidence",
        "notes",
    ]:
        assert required in header


def test_property_governance_canonical_review_decisions_contract_exists() -> None:
    path = PROJECT_ROOT / "data" / "raw" / "manual" / "property_governance_20260422_canonical_review_decisions.csv"
    assert path.exists()
    header = path.read_text(encoding="utf-8").splitlines()[0].split(",")
    for required in [
        "mol_id",
        "canonical_feature_key",
        "review_reason",
        "decision_action",
        "expected_selected_source_id",
        "expected_selected_value",
        "resolution_basis",
        "resolution_source_url",
        "notes",
    ]:
        assert required in header


def test_property_governance_proxy_acceptance_rules_contract_exists() -> None:
    path = PROJECT_ROOT / "data" / "raw" / "manual" / "property_governance_20260422_proxy_acceptance_rules.csv"
    assert path.exists()
    header = path.read_text(encoding="utf-8").splitlines()[0].split(",")
    for required in [
        "proxy_policy_id",
        "canonical_feature_key",
        "selected_source_id",
        "allow_in_strict_if_proxy_only",
        "rationale",
        "notes",
    ]:
        assert required in header


def test_controlled_vocabularies_support_inventory_expansion() -> None:
    vocab = load_yaml(SCHEMA_DIR / "controlled_vocabularies.yaml")
    assert "D" in vocab["coverage_tiers"]
    assert "inventory" in vocab["selection_roles"]
    assert sorted(vocab["entity_scopes"]) == ["candidate", "refrigerant"]
    assert sorted(vocab["model_inclusion"]) == ["no", "yes"]
    assert {"Halon", "HFE", "PFC"}.issubset(set(vocab["families"]))
