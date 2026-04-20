from __future__ import annotations

import pytest

from r_physgen_db.sources.coolprop_source import CoolPropSource, UnsupportedCoolPropFluidError


def test_coolprop_supports_transcritical_co2_cycle() -> None:
    source = CoolPropSource()
    observations = source.generate_observations("mol_co2", "CarbonDioxide", "source_coolprop_session")
    cycle_rows = [row for row in observations if row["property_name"] == "cop_standard_cycle"]
    assert len(cycle_rows) == 1
    assert cycle_rows[0]["value_num"] > 0
    assert "transcritical" in cycle_rows[0]["notes"]


def test_coolprop_requires_explicit_supported_fluid() -> None:
    source = CoolPropSource()
    with pytest.raises(UnsupportedCoolPropFluidError):
        source.generate_observations("mol_x", "DefinitelyNotAFluid", "source_coolprop_session")
