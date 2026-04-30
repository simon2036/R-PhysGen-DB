from __future__ import annotations

import json

import pandas as pd

from r_physgen_db.active_learning import (
    ACTIVE_LEARNING_SOURCE_ID,
    build_active_learning_queue,
    build_deterministic_active_learning_queue,
)
from r_physgen_db.validate import _validate_active_learning


def test_active_learning_missing_csv_returns_empty_tables(tmp_path) -> None:
    result = build_active_learning_queue(tmp_path / "missing.csv", pd.DataFrame([{"mol_id": "mol_a"}]))

    assert result.queue.empty
    assert result.decision_log.empty
    assert result.summary["input_status"] == "not_configured"
    assert result.summary["queue_entry_count"] == 0


def test_active_learning_csv_builds_queue_rows(tmp_path) -> None:
    csv_path = tmp_path / "active_learning_queue.csv"
    pd.DataFrame(
        [
            {
                "mol_id": "mol_a",
                "campaign_id": "campaign_01",
                "model_version": "model_v1",
                "acquisition_strategy": "coverage_gap",
                "priority_score": 0.9,
                "uncertainty_score": 0.2,
                "novelty_score": 0.4,
                "feasibility_score": 0.8,
                "hard_constraint_status": "passed",
                "recommended_next_action": "literature_search",
                "payload_json": "{}",
                "status": "proposed",
                "expires_at": "2026-12-31T00:00:00Z",
            }
        ]
    ).to_csv(csv_path, index=False)

    result = build_active_learning_queue(csv_path, pd.DataFrame([{"mol_id": "mol_a"}]))

    assert len(result.queue) == 1
    assert result.queue.iloc[0]["queue_entry_id"].startswith("alq_")
    assert result.queue.iloc[0]["source_id"] == ACTIVE_LEARNING_SOURCE_ID
    assert result.queue.iloc[0]["expires_at"] == "2026-12-31T00:00:00Z"
    assert result.summary["input_status"] == "loaded"
    assert result.summary["recommended_next_action_counts"] == {"literature_search": 1}


def test_active_learning_decision_log_csv_ingests_and_validates(tmp_path) -> None:
    queue_path = tmp_path / "active_learning_queue.csv"
    decision_path = tmp_path / "active_learning_decision_log.csv"
    pd.DataFrame(
        [
            {
                "queue_entry_id": "alq_a",
                "mol_id": "mol_a",
                "campaign_id": "campaign_01",
                "model_version": "model_v1",
                "acquisition_strategy": "manual_triage",
                "priority_score": 0.5,
                "uncertainty_score": 0.1,
                "novelty_score": 0.1,
                "feasibility_score": 0.9,
                "hard_constraint_status": "passed",
                "recommended_next_action": "manual_curation",
                "payload_json": "{}",
                "status": "approved",
            }
        ]
    ).to_csv(queue_path, index=False)
    pd.DataFrame(
        [
            {
                "queue_entry_id": "alq_a",
                "decision_action": "approve",
                "decision_status": "closed",
                "decided_by": "curator",
                "decided_at": "2026-04-24T00:00:00Z",
                "evidence_source_id": "source_manual_property_observations",
            }
        ]
    ).to_csv(decision_path, index=False)

    result = build_active_learning_queue(queue_path, pd.DataFrame([{"mol_id": "mol_a"}]), decision_log_path=decision_path)
    checks = {"integration_checks": [], "errors": []}

    _validate_active_learning(
        checks,
        result.queue,
        result.decision_log,
        pd.DataFrame([{"mol_id": "mol_a"}]),
        pd.DataFrame([{"source_id": ACTIVE_LEARNING_SOURCE_ID}]),
    )

    assert len(result.decision_log) == 1
    assert result.decision_log.iloc[0]["decision_id"].startswith("ald_")
    assert result.summary["decision_log_input_status"] == "loaded"
    assert checks["errors"] == []


def test_active_learning_validation_detects_bad_rows() -> None:
    queue = pd.DataFrame(
        [
            {
                "queue_entry_id": "alq_a",
                "mol_id": "mol_missing",
                "campaign_id": "campaign",
                "model_version": "model",
                "acquisition_strategy": "bad_strategy",
                "priority_score": 1.5,
                "uncertainty_score": 0.0,
                "novelty_score": 0.0,
                "feasibility_score": 0.0,
                "hard_constraint_status": "passed",
                "recommended_next_action": "run_quantum",
                "payload_json": "{}",
                "status": "proposed",
                "source_id": ACTIVE_LEARNING_SOURCE_ID,
            }
        ]
    )
    decision_log = pd.DataFrame(
        [
            {
                "decision_id": "ald_a",
                "queue_entry_id": "alq_missing",
                "decision_action": "approve",
                "decision_status": "closed",
            }
        ]
    )
    results = {"integration_checks": [], "errors": []}

    _validate_active_learning(
        results,
        queue,
        decision_log,
        pd.DataFrame([{"mol_id": "mol_a"}]),
        pd.DataFrame([{"source_id": ACTIVE_LEARNING_SOURCE_ID}]),
    )

    assert any("missing mol_ids" in error for error in results["errors"])
    assert any("priority_score must be numeric and in [0, 1]" in error for error in results["errors"])
    assert any("acquisition_strategy contains invalid values" in error for error in results["errors"])
    assert any("decision log references missing queue entries" in error for error in results["errors"])


def test_deterministic_active_learning_queue_scores_weighted_components() -> None:
    molecule_core = pd.DataFrame(
        [
            {
                "mol_id": "mol_a",
                "seed_id": "seed_a",
                "model_inclusion": "yes",
                "coverage_tier": "A",
                "heavy_atom_count": 5,
                "scaffold_key": "scaf_a",
            }
        ]
    )
    recommended = pd.DataFrame(
        [
            {"mol_id": "mol_a", "property_name": "cop_standard_cycle", "value_num": 4.0, "value": "4.0"},
            {"mol_id": "mol_a", "property_name": "boiling_point_c", "value_num": 10.0, "value": "10.0"},
        ]
    )

    queue = build_deterministic_active_learning_queue(
        molecule_core=molecule_core,
        property_recommended=recommended,
        max_entries=1,
        now="2026-04-29T00:00:00Z",
    )
    row = queue.iloc[0]
    payload = json.loads(row["payload_json"])
    expected = (
        0.30 * payload["score_components"]["performance"]
        + 0.25 * payload["score_components"]["uncertainty"]
        + 0.20 * payload["score_components"]["novelty"]
        + 0.15 * payload["score_components"]["feasibility"]
        + 0.10 * payload["score_components"]["coverage_gap"]
    )

    assert len(queue) == 1
    assert row["queue_entry_id"].startswith("alq_")
    assert row["source_id"] == ACTIVE_LEARNING_SOURCE_ID
    assert row["recommended_next_action"] == "literature_search"
    assert row["status"] == "proposed"
    assert row["priority_score"] == round(expected, 6)


def test_deterministic_active_learning_queue_includes_promoted_gaps_and_tier_d_candidates() -> None:
    molecule_core = pd.DataFrame(
        [
            {
                "mol_id": "mol_promoted_env_gap",
                "seed_id": "seed_promoted_env_gap",
                "canonical_smiles": "CC",
                "model_inclusion": "yes",
                "heavy_atom_count": 2,
                "scaffold_key": "ethane",
            },
            {
                "mol_id": "mol_promoted_cycle_gap",
                "seed_id": "seed_promoted_cycle_gap",
                "canonical_smiles": "CCC",
                "model_inclusion": "yes",
                "heavy_atom_count": 3,
                "scaffold_key": "propane",
            },
            {
                "mol_id": "mol_tier_d",
                "seed_id": "seed_tier_d",
                "canonical_smiles": "CCCC",
                "model_inclusion": "no",
                "heavy_atom_count": 4,
                "scaffold_key": "butane",
            },
        ]
    )
    seed_catalog = pd.DataFrame(
        [
            {
                "seed_id": "seed_promoted_env_gap",
                "coverage_tier": "A",
                "model_inclusion": "yes",
                "entity_scope": "refrigerant",
            },
            {
                "seed_id": "seed_promoted_cycle_gap",
                "coverage_tier": "B",
                "model_inclusion": "yes",
                "entity_scope": "candidate",
            },
            {
                "seed_id": "seed_tier_d",
                "coverage_tier": "D",
                "model_inclusion": "no",
                "entity_scope": "candidate",
            },
        ]
    )
    recommended = pd.DataFrame(
        [
            # Environment/safety gap remains; should be sent to literature search.
            {"mol_id": "mol_promoted_env_gap", "property_name": "boiling_point_c", "value_num": -50.0, "value": "-50"},
            {"mol_id": "mol_promoted_env_gap", "property_name": "critical_temp_c", "value_num": 30.0, "value": "30"},
            {"mol_id": "mol_promoted_env_gap", "property_name": "critical_pressure_mpa", "value_num": 4.0, "value": "4"},
            {"mol_id": "mol_promoted_env_gap", "property_name": "cop_standard_cycle", "value_num": 3.0, "value": "3"},
            {"mol_id": "mol_promoted_env_gap", "property_name": "viscosity_liquid_pas", "value_num": 0.001, "value": "0.001"},
            {
                "mol_id": "mol_promoted_env_gap",
                "property_name": "thermal_conductivity_liquid_wmk",
                "value_num": 0.1,
                "value": "0.1",
            },
            # Environment/safety complete but cycle/transport gap remains.
            {"mol_id": "mol_promoted_cycle_gap", "property_name": "boiling_point_c", "value_num": -40.0, "value": "-40"},
            {"mol_id": "mol_promoted_cycle_gap", "property_name": "critical_temp_c", "value_num": 35.0, "value": "35"},
            {
                "mol_id": "mol_promoted_cycle_gap",
                "property_name": "critical_pressure_mpa",
                "value_num": 4.5,
                "value": "4.5",
            },
            {"mol_id": "mol_promoted_cycle_gap", "property_name": "gwp_100yr", "value_num": 1.0, "value": "1"},
            {"mol_id": "mol_promoted_cycle_gap", "property_name": "odp", "value_num": 0.0, "value": "0"},
            {"mol_id": "mol_promoted_cycle_gap", "property_name": "ashrae_safety", "value_num": None, "value": "A2L"},
            {"mol_id": "mol_promoted_cycle_gap", "property_name": "toxicity_class", "value_num": None, "value": "A"},
        ]
    )

    queue = build_deterministic_active_learning_queue(
        molecule_core=molecule_core,
        seed_catalog=seed_catalog,
        property_recommended=recommended,
        max_entries=3,
    )

    actions = dict(zip(queue["mol_id"], queue["recommended_next_action"], strict=True))
    payloads = {row["mol_id"]: json.loads(row["payload_json"]) for row in queue.to_dict(orient="records")}

    assert set(actions) == {"mol_promoted_env_gap", "mol_promoted_cycle_gap", "mol_tier_d"}
    assert actions["mol_promoted_env_gap"] == "literature_search"
    assert actions["mol_promoted_cycle_gap"] == "run_cycle"
    assert actions["mol_tier_d"] == "run_quantum"
    assert payloads["mol_tier_d"]["coverage_tier"] == "D"
    assert set(queue["recommended_next_action"]) != {"run_quantum"}


def test_deterministic_active_learning_queue_marks_completed_quantum_mol_ids() -> None:
    molecule_core = pd.DataFrame(
        [
            {
                "mol_id": "mol_completed",
                "seed_id": "seed_completed",
                "canonical_smiles": "CC",
                "model_inclusion": "no",
                "coverage_tier": "D",
                "heavy_atom_count": 2,
                "scaffold_key": "ethane",
            }
        ]
    )

    queue = build_deterministic_active_learning_queue(
        molecule_core=molecule_core,
        property_recommended=pd.DataFrame(),
        completed_quantum_mol_ids={"mol_completed"},
        max_entries=1,
        now="2026-04-30T00:00:00Z",
    )

    row = queue.iloc[0]
    assert row["recommended_next_action"] == "run_quantum"
    assert row["status"] == "completed"
    assert "completed from quantum pilot results" in row["notes"]
    assert row["updated_at"] == "2026-04-30T00:00:00Z"


def test_deterministic_active_learning_queue_keeps_pending_quantum_rows_without_completed_result() -> None:
    molecule_core = pd.DataFrame(
        [
            {
                "mol_id": "mol_pending",
                "seed_id": "seed_pending",
                "canonical_smiles": "CC",
                "model_inclusion": "no",
                "coverage_tier": "D",
                "heavy_atom_count": 2,
                "scaffold_key": "ethane",
            }
        ]
    )

    queue = build_deterministic_active_learning_queue(
        molecule_core=molecule_core,
        property_recommended=pd.DataFrame(),
        completed_quantum_mol_ids=set(),
        max_entries=1,
    )

    row = queue.iloc[0]
    assert row["recommended_next_action"] == "run_quantum"
    assert row["status"] == "proposed"


def test_deterministic_active_learning_queue_does_not_treat_quantum_pilot_results_as_closing_run_quantum_scope() -> None:
    molecule_core = pd.DataFrame(
        [
            {
                "mol_id": "mol_quantum_only_gap",
                "seed_id": "seed_quantum_only_gap",
                "canonical_smiles": "CC",
                "model_inclusion": "yes",
                "heavy_atom_count": 2,
                "scaffold_key": "ethane",
            }
        ]
    )
    seed_catalog = pd.DataFrame(
        [
            {
                "seed_id": "seed_quantum_only_gap",
                "coverage_tier": "A",
                "model_inclusion": "yes",
                "entity_scope": "candidate",
            }
        ]
    )
    recommended = pd.DataFrame(
        [
            {"mol_id": "mol_quantum_only_gap", "property_name": "boiling_point_c", "value_num": -50.0, "value": "-50"},
            {"mol_id": "mol_quantum_only_gap", "property_name": "critical_temp_c", "value_num": 30.0, "value": "30"},
            {"mol_id": "mol_quantum_only_gap", "property_name": "critical_pressure_mpa", "value_num": 4.0, "value": "4"},
            {"mol_id": "mol_quantum_only_gap", "property_name": "gwp_100yr", "value_num": 1.0, "value": "1"},
            {"mol_id": "mol_quantum_only_gap", "property_name": "odp", "value_num": 0.0, "value": "0"},
            {"mol_id": "mol_quantum_only_gap", "property_name": "ashrae_safety", "value_num": None, "value": "A2L"},
            {"mol_id": "mol_quantum_only_gap", "property_name": "toxicity_class", "value_num": None, "value": "A"},
            {"mol_id": "mol_quantum_only_gap", "property_name": "viscosity_liquid_pas", "value_num": 0.001, "value": "0.001"},
            {
                "mol_id": "mol_quantum_only_gap",
                "property_name": "thermal_conductivity_liquid_wmk",
                "value_num": 0.1,
                "value": "0.1",
            },
            {"mol_id": "mol_quantum_only_gap", "property_name": "cop_standard_cycle", "value_num": 3.0, "value": "3"},
            {"mol_id": "mol_quantum_only_gap", "property_name": "homo_ev", "value_num": -8.0, "value": "-8"},
            {"mol_id": "mol_quantum_only_gap", "property_name": "lumo_ev", "value_num": -1.0, "value": "-1"},
            {"mol_id": "mol_quantum_only_gap", "property_name": "gap_ev", "value_num": 7.0, "value": "7"},
        ]
    )

    queue = build_deterministic_active_learning_queue(
        molecule_core=molecule_core,
        seed_catalog=seed_catalog,
        property_recommended=recommended,
        max_entries=1,
    )

    assert queue.iloc[0]["recommended_next_action"] == "run_quantum"


def test_deterministic_active_learning_queue_can_reserve_production_quantum_capacity() -> None:
    promoted_rows = [
        {
            "mol_id": f"mol_promoted_{idx}",
            "seed_id": f"seed_promoted_{idx}",
            "canonical_smiles": "CC",
            "model_inclusion": "yes",
            "coverage_tier": "A",
            "heavy_atom_count": 2,
            "scaffold_key": "promoted",
        }
        for idx in range(5)
    ]
    tier_d_rows = [
        {
            "mol_id": f"mol_tier_d_{idx:04d}",
            "seed_id": f"seed_tier_d_{idx:04d}",
            "canonical_smiles": "C" * ((idx % 6) + 1),
            "model_inclusion": "no",
            "coverage_tier": "D",
            "heavy_atom_count": (idx % 6) + 1,
            "scaffold_key": f"scaffold_{idx % 17}",
        }
        for idx in range(2050)
    ]
    molecule_core = pd.DataFrame([*promoted_rows, *tier_d_rows])
    recommended = pd.DataFrame(
        [
            {
                "mol_id": row["mol_id"],
                "property_name": "boiling_point_c",
                "value_num": -40.0,
                "value": "-40",
            }
            for row in promoted_rows
        ]
    )

    queue = build_deterministic_active_learning_queue(
        molecule_core=molecule_core,
        property_recommended=recommended,
        max_entries=25,
        min_quantum_entries=2000,
    )

    assert len(queue) > 25
    assert int(queue["recommended_next_action"].eq("run_quantum").sum()) == 2000
    assert queue["mol_id"].duplicated().sum() == 0
