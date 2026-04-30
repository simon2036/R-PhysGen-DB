from __future__ import annotations

import pytest

from r_physgen_db.sources.coolprop_source import CoolPropSource, UnsupportedCoolPropFluidError


def test_coolprop_supports_transcritical_co2_cycle() -> None:
    source = CoolPropSource()
    observations = source.generate_observations("mol_co2", "CarbonDioxide", "source_coolprop_session")
    cycle_rows = [row for row in observations if row["property_name"] == "cop_standard_cycle"]
    assert len(cycle_rows) >= 2
    assert {row["cycle_model"] for row in cycle_rows} == {"transcritical_co2"}
    assert {row["operating_point_hash"] for row in cycle_rows if row["value_num"]} == {
        row["operating_point_hash"] for row in cycle_rows
    }
    assert all(row["value_num"] > 0 for row in cycle_rows)
    assert all("transcritical" in row["notes"] for row in cycle_rows)


def test_coolprop_emits_multiple_subcritical_operating_points() -> None:
    source = CoolPropSource()
    observations = source.generate_observations("mol_r134a", "R134a", "source_coolprop_session")
    cycle_rows = [row for row in observations if row.get("phase") == "cycle" and row.get("qc_status") == "pass"]
    rows_by_case: dict[str, set[str]] = {}
    for row in cycle_rows:
        rows_by_case.setdefault(row["cycle_case_id"], set()).add(row["property_name"])

    assert len(rows_by_case) >= 3
    assert len({row["operating_point_hash"] for row in cycle_rows}) == len(rows_by_case)
    assert all(
        {"cop_standard_cycle", "volumetric_cooling_mjm3", "pressure_ratio", "discharge_temperature_c"}.issubset(properties)
        for properties in rows_by_case.values()
    )


def test_coolprop_emits_saturated_liquid_transport_rows() -> None:
    source = CoolPropSource()
    observations = source.generate_observations("mol_r134a", "R134a", "source_coolprop_session")
    transport_rows = {
        row["property_name"]: row
        for row in observations
        if row["property_name"] in {"viscosity_liquid_pas", "thermal_conductivity_liquid_wmk"}
    }

    assert set(transport_rows) == {"viscosity_liquid_pas", "thermal_conductivity_liquid_wmk"}
    assert transport_rows["viscosity_liquid_pas"]["value_num"] > 0
    assert transport_rows["thermal_conductivity_liquid_wmk"]["value_num"] > 0
    assert transport_rows["viscosity_liquid_pas"]["phase"] == "saturated_liquid"
    assert "CoolProp.PropsSI(V" in transport_rows["viscosity_liquid_pas"]["method"]


def test_coolprop_retains_thermo_when_cycle_path_is_unresolved() -> None:
    source = CoolPropSource()
    observations = source.generate_observations("mol_methyl_palmitate", "MethylPalmitate", "source_coolprop_session")
    rows_by_property = {row["property_name"]: row for row in observations}

    assert rows_by_property["critical_temp_c"]["value_num"] > 0
    assert rows_by_property["critical_pressure_mpa"]["value_num"] > 0
    assert rows_by_property["cop_standard_cycle"]["qc_status"] == "warning"
    assert rows_by_property["cop_standard_cycle"]["qc_flags"] == "cycle_unresolved"
    assert "cycle_unresolved" in rows_by_property["cop_standard_cycle"]["notes"]


def test_coolprop_requires_explicit_supported_fluid() -> None:
    source = CoolPropSource()
    with pytest.raises(UnsupportedCoolPropFluidError):
        source.generate_observations("mol_x", "DefinitelyNotAFluid", "source_coolprop_session")
