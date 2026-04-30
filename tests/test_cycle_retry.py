from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from r_physgen_db.pipeline import _manual_property_rows
from r_physgen_db.cycle_retry import build_run_cycle_retry_manifest, run_cycle_retry


def test_run_cycle_retry_manifest_filters_queue_and_records_alias_candidates() -> None:
    queue = pd.DataFrame(
        [
            {
                "queue_entry_id": "alq_cycle",
                "mol_id": "mol_a",
                "recommended_next_action": "run_cycle",
                "payload_json": json.dumps(
                    {
                        "missing_properties": ["cop_standard_cycle", "viscosity_liquid_pas"],
                        "seed_id": "seed_a",
                        "r_number": "R-50",
                    }
                ),
            },
            {
                "queue_entry_id": "alq_lit",
                "mol_id": "mol_b",
                "recommended_next_action": "literature_search",
                "payload_json": "{}",
            },
        ]
    )
    molecule_core = pd.DataFrame([{"mol_id": "mol_a", "seed_id": "seed_a"}])
    molecule_alias = pd.DataFrame(
        [
            {"mol_id": "mol_a", "alias_type": "r_number", "alias_value": "R-50"},
            {"mol_id": "mol_a", "alias_type": "coolprop_fluid", "alias_value": "Methane"},
        ]
    )
    seed_catalog = pd.DataFrame([{"seed_id": "seed_a", "r_number": "R-50", "coolprop_fluid": "Methane"}])

    manifest = build_run_cycle_retry_manifest(
        active_learning_queue=queue,
        molecule_core=molecule_core,
        molecule_alias=molecule_alias,
        seed_catalog=seed_catalog,
        coolprop_aliases={"R-50": "Methane"},
    )

    assert len(manifest) == 1
    row = manifest.iloc[0]
    assert row["queue_entry_id"] == "alq_cycle"
    assert json.loads(row["missing_properties_json"]) == ["cop_standard_cycle", "viscosity_liquid_pas"]
    assert json.loads(row["coolprop_alias_candidates_json"])[:2] == ["Methane", "R-50"]
    assert "REFPROP::Methane" in json.loads(row["refprop_alias_candidates_json"])


def test_refprop_retry_without_backend_root_writes_blocker_not_results(tmp_path: Path) -> None:
    manifest = pd.DataFrame(
        [
            {
                "queue_entry_id": "alq_cycle",
                "mol_id": "mol_a",
                "seed_id": "seed_a",
                "r_number": "R-134a",
                "coolprop_alias_candidates_json": json.dumps(["R134a"]),
                "refprop_alias_candidates_json": json.dumps(["REFPROP::R134a"]),
                "missing_properties_json": json.dumps(["cop_standard_cycle"]),
            }
        ]
    )

    run = run_cycle_retry(
        manifest,
        backend="refprop",
        refprop_root=None,
        artifact_dir=tmp_path,
        write_results=False,
    )

    assert run.results.empty
    assert len(run.blockers) == 1
    assert run.blockers.iloc[0]["status"] == "blocked_on_external_backend"
    assert run.blockers.iloc[0]["blocker_reason"] == "refprop_root_not_configured"
    assert run.summary["succeeded_queue_entries"] == 0
    assert run.summary["blocked_queue_entries"] == 1


def test_fake_refprop_success_uses_transcritical_case_when_subcritical_is_impossible(tmp_path: Path) -> None:
    refprop_root = tmp_path / "REFPROP"
    (refprop_root / "FLUIDS").mkdir(parents=True)
    (refprop_root / "MIXTURES").mkdir()
    (refprop_root / "librefprop.so").write_text("fake", encoding="utf-8")
    manifest = pd.DataFrame(
        [
            {
                "queue_entry_id": "alq_cycle",
                "mol_id": "mol_lowcrit",
                "seed_id": "seed_lowcrit",
                "r_number": "LOWCRIT",
                "coolprop_alias_candidates_json": json.dumps(["LOWCRIT"]),
                "refprop_alias_candidates_json": json.dumps(["REFPROP::LOWCRIT"]),
                "missing_properties_json": json.dumps(
                    ["cop_standard_cycle", "viscosity_liquid_pas", "thermal_conductivity_liquid_wmk"]
                ),
            }
        ]
    )

    def fake_props(output: str, *args: Any) -> float:
        if output == "Tcrit":
            return 310.0
        if output == "P":
            return 1_000_000.0
        if output == "Hmass" and args[:4] == ("T", 308.15, "P", 9_000_000.0):
            return 300_000.0
        if output == "Hmass" and args[:4] == ("T", 273.15, "P", 1_000_000.0):
            return 500_000.0
        if output == "Hmass" and args[:4] == ("P", 9_000_000.0, "Smass", 2_000.0):
            return 600_000.0
        if output == "Smass":
            return 2_000.0
        if output == "Dmass":
            return 30.0
        if output == "T" and args[:4] == ("P", 9_000_000.0, "Hmass", 642_857.1428571428):
            return 350.0
        if output == "V":
            return 0.001
        if output == "L":
            return 0.1
        raise AssertionError(f"unexpected PropsSI call: {output!r}, {args!r}")

    run = run_cycle_retry(
        manifest,
        backend="refprop",
        refprop_root=refprop_root,
        artifact_dir=tmp_path / "artifacts",
        write_results=False,
        props_si=fake_props,
    )

    assert run.blockers.empty
    assert {"cop_standard_cycle", "viscosity_liquid_pas", "thermal_conductivity_liquid_wmk"}.issubset(
        set(run.results["property_name"])
    )
    cycle_rows = run.results.loc[run.results["phase"].eq("cycle")]
    assert set(cycle_rows["cycle_model"]) == {"transcritical_generalized"}
    assert set(cycle_rows["cycle_case_id"]) == {"transcritical_generalized_cycle"}
    assert cycle_rows["artifact_sha256"].astype(str).str.len().gt(0).all()
    assert run.summary["succeeded_queue_entries"] == 1


def test_manual_cycle_result_ingestion_preserves_cycle_provenance_fields() -> None:
    manual = pd.DataFrame(
        [
            {
                "seed_id": "seed_a",
                "r_number": "R-134a",
                "property_name": "cop_standard_cycle",
                "value": "4.2",
                "value_num": 4.2,
                "unit": "dimensionless",
                "temperature": "5 degC evaporating / 50 degC condensing",
                "phase": "cycle",
                "source_type": "calculated_open_source",
                "source_name": "REFPROP via CoolProp",
                "method": "REFPROP subcritical vapor-compression cycle",
                "quality_level": "computed_high",
                "notes": "artifact_sha256=abc",
                "manual_source_id": "source_r_physgen_cycle_backend_results",
                "source_record_id": "cycle_result_1",
                "cycle_case_id": "standard_subcritical_cycle",
                "operating_point_hash": "op_abc",
                "cycle_model": "subcritical_vapor_compression",
                "eos_source": "REFPROP",
                "convergence_flag": 1,
            }
        ]
    )

    rows = _manual_property_rows(manual, {"seed_a": "mol_a"}, {})

    assert rows[0]["source_id"] == "source_r_physgen_cycle_backend_results"
    assert rows[0]["source_record_id"] == "cycle_result_1"
    assert rows[0]["cycle_case_id"] == "standard_subcritical_cycle"
    assert rows[0]["operating_point_hash"] == "op_abc"
    assert rows[0]["cycle_model"] == "subcritical_vapor_compression"
    assert rows[0]["eos_source"] == "REFPROP"
    assert rows[0]["convergence_flag"] == 1
