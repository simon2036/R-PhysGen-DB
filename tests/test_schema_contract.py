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
    for name in ["regulatory_status.yaml", "pending_sources.yaml"]:
        assert (SCHEMA_DIR / name).exists()


def test_controlled_vocabularies_support_inventory_expansion() -> None:
    vocab = load_yaml(SCHEMA_DIR / "controlled_vocabularies.yaml")
    assert "D" in vocab["coverage_tiers"]
    assert "inventory" in vocab["selection_roles"]
    assert sorted(vocab["entity_scopes"]) == ["candidate", "refrigerant"]
    assert sorted(vocab["model_inclusion"]) == ["no", "yes"]
