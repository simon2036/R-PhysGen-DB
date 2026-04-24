from __future__ import annotations

import pandas as pd

from r_physgen_db.condition_sets import backfill_condition_sets
from r_physgen_db.constants import STANDARD_CYCLE, TRANSCRITICAL_CO2_CYCLE
from r_physgen_db.cycle_conditions import (
    build_cycle_tables,
    built_in_cycle_cases,
    fill_cycle_observation_fields,
    operating_point_hash,
)
from r_physgen_db.sources.coolprop_source import CoolPropSource


def test_operating_point_hash_is_stable_and_changes_with_conditions() -> None:
    point = {
        "evaporating_temperature_c": 5.0,
        "condensing_temperature_c": 50.0,
        "gas_cooler_outlet_temperature_c": None,
        "high_side_pressure_mpa": None,
        "superheat_k": 5.0,
        "subcooling_k": 5.0,
        "compressor_isentropic_efficiency": 0.75,
    }

    first_hash, first_json = operating_point_hash(point)
    second_hash, second_json = operating_point_hash(dict(reversed(list(point.items()))))
    changed_hash, _ = operating_point_hash({**point, "condensing_temperature_c": 45.0})

    assert first_hash == second_hash
    assert first_json == second_json
    assert first_hash != changed_hash
    assert first_hash.startswith("op_")


def test_builtin_cycle_cases_match_constants() -> None:
    cases = built_in_cycle_cases()

    standard = cases["standard_subcritical_cycle"]
    transcritical = cases["transcritical_co2_cycle"]

    assert standard["cycle_model"] == "subcritical_vapor_compression"
    assert transcritical["cycle_model"] == "transcritical_co2"
    assert str(STANDARD_CYCLE["evaporating_temp_c"]) in standard["operating_point_json"]
    assert str(TRANSCRITICAL_CO2_CYCLE["high_side_pressure_mpa"]) in transcritical["operating_point_json"]
    assert standard["operating_point_hash"] != transcritical["operating_point_hash"]


def test_coolprop_cycle_observations_include_structured_cycle_fields() -> None:
    source = CoolPropSource()
    observations = source.generate_observations("mol_co2", "CarbonDioxide", "source_coolprop_session")
    cycle_rows = [row for row in observations if row.get("phase") == "cycle" and row.get("qc_status") == "pass"]
    property_names = {row["property_name"] for row in cycle_rows}

    assert {"cop_standard_cycle", "volumetric_cooling_mjm3", "pressure_ratio", "discharge_temperature_c"}.issubset(property_names)
    assert {row["cycle_case_id"] for row in cycle_rows} == {"transcritical_co2_cycle"}
    assert all(str(row["operating_point_hash"]).startswith("op_") for row in cycle_rows)
    assert all(row["eos_source"] == "CoolProp" for row in cycle_rows)
    assert all(row["convergence_flag"] == 1 for row in cycle_rows)


def test_condition_sets_distinguish_cycle_operating_points() -> None:
    observation = pd.DataFrame(
        [
            {
                "observation_id": "obs_1",
                "mol_id": "mol_1",
                "property_name": "cop_standard_cycle",
                "value": "4.0",
                "value_num": 4.0,
                "unit": "dimensionless",
                "temperature": "5 degC evap / 50 degC cond",
                "pressure": "",
                "phase": "cycle",
                "source_id": "source_coolprop_session",
                "source_name": "CoolProp test",
                "qc_status": "pass",
                "qc_flags": "",
            },
            {
                "observation_id": "obs_2",
                "mol_id": "mol_2",
                "property_name": "cop_standard_cycle",
                "value": "2.0",
                "value_num": 2.0,
                "unit": "dimensionless",
                "temperature": "-5 degC evap / 35 degC gas cooler / 9 MPa high side",
                "pressure": "",
                "phase": "cycle",
                "source_id": "source_coolprop_session",
                "source_name": "CoolProp test",
                "method": "CoolProp transcritical CO2 cycle",
                "notes": "resolved:transcritical_co2",
                "qc_status": "pass",
                "qc_flags": "",
            },
        ]
    )

    backfilled, condition_set, _ = backfill_condition_sets(observation)

    assert backfilled["condition_set_id"].nunique() == 2
    assert condition_set["operating_point_hash"].nunique() == 2
    assert set(condition_set["cycle_case_id"]) == {"standard_subcritical_cycle", "transcritical_co2_cycle"}


def test_cycle_tables_are_built_from_resolved_cycle_observations() -> None:
    observation = fill_cycle_observation_fields(
        pd.DataFrame(
            [
                {
                    "observation_id": "obs_1",
                    "mol_id": "mol_1",
                    "property_name": "cop_standard_cycle",
                    "value": "4.0",
                    "value_num": 4.0,
                    "unit": "dimensionless",
                    "phase": "cycle",
                    "source_id": "source_coolprop_session",
                    "source_name": "CoolProp test",
                    "qc_status": "pass",
                    "qc_flags": "",
                    "condition_set_id": "cond_a",
                },
                {
                    "observation_id": "obs_2",
                    "mol_id": "mol_2",
                    "property_name": "cop_standard_cycle",
                    "value": "",
                    "value_num": None,
                    "unit": "dimensionless",
                    "phase": "cycle",
                    "source_id": "source_coolprop_session",
                    "source_name": "CoolProp test",
                    "qc_status": "warning",
                    "qc_flags": "cycle_unresolved",
                    "condition_set_id": "cond_b",
                },
            ]
        )
    )

    cycle_case, cycle_operating_point, summary = build_cycle_tables(observation)

    assert len(cycle_case) == 1
    assert len(cycle_operating_point) == 1
    assert cycle_case.iloc[0]["cycle_case_id"] == "standard_subcritical_cycle"
    assert summary["resolved_cycle_observation_count"] == 1
    assert summary["unresolved_cycle_observation_count"] == 1
