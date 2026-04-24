from __future__ import annotations

import pandas as pd

from r_physgen_db.active_learning import ACTIVE_LEARNING_SOURCE_ID, build_active_learning_queue
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
