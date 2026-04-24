from __future__ import annotations

import pandas as pd

from r_physgen_db.condition_sets import backfill_condition_sets
from r_physgen_db.pipeline import _build_model_dataset_index, _build_model_ready, _build_property_matrix, _build_quality_report
from r_physgen_db.quantum_pilot import (
    QUANTUM_CANONICAL_FEATURE_KEYS,
    QUANTUM_FORBIDDEN_WIDE_COLUMNS,
    QUANTUM_PROPERTY_NAMES,
    QUANTUM_SOURCE_ID,
    build_quantum_pilot,
)
from r_physgen_db.validate import _validate_quantum_pilot


def test_quantum_pilot_csv_builds_jobs_artifacts_and_observations(tmp_path) -> None:
    csv_path = tmp_path / "quantum_pilot_results.csv"
    pd.DataFrame(
        [
            _quantum_csv_row("quantum.homo_energy", -8.1),
            _quantum_csv_row("quantum.lumo_energy", -1.2),
            _quantum_csv_row("quantum.homo_lumo_gap", 6.9),
        ]
    ).to_csv(csv_path, index=False)

    result = build_quantum_pilot(csv_path, pd.DataFrame([{"mol_id": "mol_a"}]))
    observation = pd.DataFrame(result.property_rows)
    backfilled, condition_set, _ = backfill_condition_sets(observation)

    assert len(result.quantum_job) == 1
    assert len(result.quantum_artifact) == 1
    assert len(result.property_rows) == 3
    assert set(observation["source_id"]) == {QUANTUM_SOURCE_ID}
    assert set(observation["canonical_feature_key"]) == QUANTUM_CANONICAL_FEATURE_KEYS
    assert set(observation["property_name"]) == QUANTUM_PROPERTY_NAMES
    assert set(observation["quality_level"]) == {"computed_standard"}
    assert backfilled["condition_set_id"].notna().all()
    assert set(condition_set["condition_role"]) == {"gas_phase_298k"}
    assert result.summary["quantum_observation_count"] == 3


def test_quantum_pilot_missing_csv_returns_empty_tables(tmp_path) -> None:
    result = build_quantum_pilot(tmp_path / "missing.csv", pd.DataFrame([{"mol_id": "mol_a"}]))

    assert result.property_rows == []
    assert result.quantum_job.empty
    assert result.quantum_artifact.empty
    assert result.summary["input_status"] == "not_configured"
    assert result.summary["quantum_observation_count"] == 0


def test_quantum_quality_report_and_wide_outputs_keep_boundary(tmp_path) -> None:
    csv_path = tmp_path / "quantum_pilot_results.csv"
    pd.DataFrame([_quantum_csv_row("quantum.homo_energy", -8.1)]).to_csv(csv_path, index=False)
    result = build_quantum_pilot(csv_path, pd.DataFrame([{"mol_id": "mol_a"}]))
    observation = pd.DataFrame(result.property_rows)
    recommended = pd.DataFrame(
        [
            {
                "mol_id": "mol_a",
                "property_name": "homo_ev",
                "value": "-8.1",
                "value_num": -8.1,
                "unit": "eV",
                "selected_source_id": QUANTUM_SOURCE_ID,
                "selected_source_name": "Quantum",
                "selected_quality_level": "computed_standard",
            }
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
        quantum_summary=result.summary,
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

    assert report["quantum_pilot_summary"]["quantum_observation_count"] == 1
    assert not (QUANTUM_FORBIDDEN_WIDE_COLUMNS & set(matrix.columns))
    assert not (QUANTUM_FORBIDDEN_WIDE_COLUMNS & set(model_ready.columns))
    assert not (QUANTUM_FORBIDDEN_WIDE_COLUMNS & set(model_index.columns))


def test_validate_quantum_pilot_detects_invalid_rows() -> None:
    property_observation = pd.DataFrame(
        [
            {
                "observation_id": "obs_bad",
                "mol_id": "mol_a",
                "property_name": "homo_ev",
                "value": "-1",
                "value_num": -1.0,
                "unit": "eV",
                "source_id": QUANTUM_SOURCE_ID,
                "source_record_id": "q_bad:quantum.bad",
                "canonical_feature_key": "quantum.bad",
                "quality_level": "computed_standard",
                "convergence_flag": 0,
                "condition_set_id": "",
            }
        ]
    )
    recommended = pd.DataFrame(
        [{"mol_id": "mol_a", "property_name": "homo_ev", "value": "-1", "value_num": -1.0, "selected_source_id": QUANTUM_SOURCE_ID}]
    )
    quantum_job = pd.DataFrame(
        [
            {
                "request_id": "q_bad",
                "mol_id": "mol_a",
                "status": "succeeded",
                "quality_level": "computed_standard",
                "converged": 1,
                "imaginary_frequency_count": 0,
                "derived_observation_count": 1,
                "source_id": QUANTUM_SOURCE_ID,
            }
        ]
    )
    quantum_artifact = pd.DataFrame(
        [
            {
                "artifact_id": "qart_bad",
                "request_id": "q_bad",
                "mol_id": "mol_a",
                "artifact_uri": "manual://bad",
                "artifact_sha256": "",
                "source_id": QUANTUM_SOURCE_ID,
            }
        ]
    )
    results = {"integration_checks": [], "errors": []}

    _validate_quantum_pilot(
        results,
        property_observation,
        recommended,
        pd.DataFrame([{"source_id": QUANTUM_SOURCE_ID}]),
        pd.DataFrame(columns=["condition_set_id", "condition_role"]),
        quantum_job,
        quantum_artifact,
        pd.DataFrame(columns=["mol_id", "homo_ev"]),
        pd.DataFrame(columns=["mol_id"]),
        pd.DataFrame(columns=["mol_id"]),
    )

    assert any("Unexpected quantum canonical feature keys" in error for error in results["errors"])
    assert any("convergence_flag=1" in error for error in results["errors"])
    assert any("missing condition_set_id" in error for error in results["errors"])
    assert any("blank artifact_sha256" in error for error in results["errors"])
    assert any("Quantum columns leaked into wide ML outputs" in error for error in results["errors"])


def _quantum_csv_row(canonical_feature_key: str, value_num: float) -> dict[str, object]:
    return {
        "request_id": "q_mol_a_b3lyp",
        "mol_id": "mol_a",
        "canonical_feature_key": canonical_feature_key,
        "value_num": value_num,
        "unit": "eV",
        "program": "ORCA",
        "program_version": "6.0",
        "method_family": "DFT",
        "theory_level": "B3LYP",
        "basis_set": "6-311+G(2d,p)",
        "solvation_model": "gas_phase",
        "converged": 1,
        "imaginary_frequency_count": 0,
        "artifact_uri": "manual://quantum/q_mol_a_b3lyp",
        "artifact_sha256": "a" * 64,
        "quality_level": "computed_standard",
        "notes": "fixture",
    }
