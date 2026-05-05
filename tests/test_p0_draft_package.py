from __future__ import annotations

import json
from pathlib import Path

from r_physgen_db.blueprints.pipeline_staged_blueprint import build_dataset_staged
from r_physgen_db.constants import PROJECT_ROOT, SCHEMA_DIR
from r_physgen_db.utils import load_yaml


P0_DOCS = [
    Path("plans/p0/p0_scope_and_exit_criteria.md"),
    Path("plans/p0/p0_phase2_interfaces_draft.md"),
    Path("plans/p0/p0_pipeline_stage_refactor_plan.md"),
    Path("plans/p0/p0_validation_rules_draft.md"),
    Path("reviews/p0_review_response_matrix.md"),
]

P0_DRAFT_SCHEMAS = [
    "canonical_feature_registry.yaml",
    "observation_condition_set.yaml",
    "property_observation_v2.yaml",
    "normalization_rules.yaml",
    "stage_run_manifest.yaml",
    "research_task_readiness_rules.yaml",
    "mixture_composition.yaml",
    "molecule_split_definition.yaml",
]


def test_p0_draft_assets_exist() -> None:
    for name in P0_DOCS:
        assert (PROJECT_ROOT / "docs" / name).exists()
    for name in P0_DRAFT_SCHEMAS:
        assert (SCHEMA_DIR / "drafts" / name).exists()
    for name in ["backfill_condition_set.py", "pr_b_equivalence_check.py"]:
        assert (PROJECT_ROOT / "scripts" / name).exists()
    assert (PROJECT_ROOT / "src" / "r_physgen_db" / "blueprints" / "pipeline_staged_blueprint.py").exists()


def test_p0_draft_yaml_contracts_parse() -> None:
    for name in P0_DRAFT_SCHEMAS:
        schema = load_yaml(SCHEMA_DIR / "drafts" / name)
        assert schema["table_name"]
        assert schema["columns"]

    condition_schema = load_yaml(SCHEMA_DIR / "drafts" / "observation_condition_set.yaml")
    assert "normal_boiling_point" in condition_schema["condition_role_vocabulary"]
    assert condition_schema["id_generation_rule"]["prefix"] == "cond_"

    observation_schema = load_yaml(SCHEMA_DIR / "drafts" / "property_observation_v2.yaml")
    observation_columns = {column["name"] for column in observation_schema["columns"]}
    assert {"canonical_feature_key", "condition_set_id", "normalization_rule_id"}.issubset(observation_columns)

    manifest_schema = load_yaml(SCHEMA_DIR / "drafts" / "stage_run_manifest.yaml")
    assert {"run_id", "stage_id", "attempt_id"} == set(manifest_schema["primary_key"])


def test_research_readiness_rules_reference_registered_canonical_keys() -> None:
    registry = load_yaml(SCHEMA_DIR / "drafts" / "canonical_feature_registry.yaml")
    registry_keys = {item["canonical_feature_key"] for item in registry["registry"]}
    rules = load_yaml(SCHEMA_DIR / "drafts" / "research_task_readiness_rules.yaml")

    assert rules["rules"]
    for rule in rules["rules"]:
        for item in rule.get("must_have", []) + rule.get("should_have", []):
            assert item["canonical_feature_key"] in registry_keys


def test_pipeline_staged_blueprint_writes_attempt_manifest(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    results = build_dataset_staged(PROJECT_ROOT, data_dir, run_id="run_test_p0", refresh_remote=False)

    assert results
    assert all(result.status == "succeeded" for result in results)

    manifest_path = data_dir / "bronze" / "stage_run_manifest.blueprint.jsonl"
    assert manifest_path.exists()
    rows = [json.loads(line) for line in manifest_path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == len(results)
    assert rows[0]["run_id"] == "run_test_p0"
    assert rows[0]["stage_id"] == "00"
    assert rows[0]["attempt_id"] == "run_test_p0_00_attempt1"
    assert rows[0]["attempt_number"] == 1
    assert rows[-1]["status"] == "succeeded"


def test_condition_set_id_generation_is_stable() -> None:
    import importlib.util

    script_path = PROJECT_ROOT / "scripts" / "backfill_condition_set.py"
    spec = importlib.util.spec_from_file_location("backfill_condition_set", script_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

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

    first_id, first_signature = module.condition_id(condition)
    second_id, second_signature = module.condition_id(dict(condition))

    assert first_id == second_id
    assert first_signature == second_signature
    assert first_id.startswith("cond_")
