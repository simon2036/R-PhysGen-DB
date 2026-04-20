from __future__ import annotations

import pandas as pd

from r_physgen_db.pipeline import (
    _build_alias_lookup,
    _epa_gwp_reference_property_rows,
    _match_epa_alias_candidates,
)


def test_match_epa_alias_candidates_expands_parenthetical_and_direct_variants() -> None:
    alias_df = pd.DataFrame(
        [
            {"mol_id": "mol_co2", "alias_type": "synonym", "alias_value": "Carbon Dioxide", "is_primary": False, "source_name": "test"},
            {"mol_id": "mol_hfo", "alias_type": "synonym", "alias_value": "HFO-1234ze(E)", "is_primary": False, "source_name": "test"},
            {"mol_id": "mol_hcfc31", "alias_type": "synonym", "alias_value": "Monochlorofluoromethane", "is_primary": False, "source_name": "test"},
        ]
    )
    alias_lookup = _build_alias_lookup(alias_df)

    assert _match_epa_alias_candidates(alias_lookup, ["R-744 (Carbon Dioxide)"]) == {"mol_co2"}
    assert _match_epa_alias_candidates(alias_lookup, ["Direct HFO-1234ze(E) Expansion"]) == {"mol_hfo"}
    assert _match_epa_alias_candidates(alias_lookup, ["HCFC-31 (CH2FCl) Monochlorofluoromethane"]) == {"mol_hcfc31"}


def test_epa_gwp_reference_property_rows_emit_exact_and_grouped_tier_c_matches() -> None:
    alias_df = pd.DataFrame(
        [
            {"mol_id": "mol_acetone", "alias_type": "synonym", "alias_value": "Acetone", "is_primary": True, "source_name": "test"},
        ]
    )
    alias_lookup = _build_alias_lookup(alias_df)
    molecule_context = pd.DataFrame(
        [
            {
                "mol_id": "mol_acetone",
                "seed_id": "seed_acetone",
                "family": "Ketone",
                "formula": "C3H6O",
                "pubchem_query": "67-64-1",
                "coverage_tier": "B",
                "selection_role": "candidate",
            },
            {
                "mol_id": "mol_benzene",
                "seed_id": "seed_benzene",
                "family": "Aromatic",
                "formula": "C6H6",
                "pubchem_query": "71-43-2",
                "coverage_tier": "C",
                "selection_role": "candidate",
            },
            {
                "mol_id": "mol_pxylene",
                "seed_id": "seed_p_xylene",
                "family": "Aromatic",
                "formula": "C8H10",
                "pubchem_query": "106-42-3",
                "coverage_tier": "C",
                "selection_role": "candidate",
            },
            {
                "mol_id": "mol_argon",
                "seed_id": "seed_argon",
                "family": "Inert gas",
                "formula": "Ar",
                "pubchem_query": "7440-37-1",
                "coverage_tier": "C",
                "selection_role": "candidate",
            },
        ]
    )
    gwp_df = pd.DataFrame(
        [
            {
                "substance_name": "Acetone",
                "gwp_text": "0.5",
                "gwp_100yr": 0.5,
                "gwp_range_min": None,
                "gwp_range_max": None,
                "reference": "IPCC 2007",
                "is_range": False,
            },
            {
                "substance_name": "Hydrocarbons (C5-C20)",
                "gwp_text": "1.3-3.7",
                "gwp_100yr": None,
                "gwp_range_min": 1.3,
                "gwp_range_max": 3.7,
                "reference": "Calculated",
                "is_range": True,
            },
        ]
    )

    rows = _epa_gwp_reference_property_rows(
        gwp_df=gwp_df,
        alias_lookup=alias_lookup,
        molecule_context=molecule_context,
        source_id="source_epa_gwp",
    )

    assert len(rows) == 3

    by_mol = {row["mol_id"]: row for row in rows}
    assert by_mol["mol_acetone"]["value_num"] == 0.5
    assert "IPCC 2007" in by_mol["mol_acetone"]["notes"]
    assert by_mol["mol_benzene"]["value_num"] == 3.7
    assert by_mol["mol_pxylene"]["value_num"] == 3.7
    assert "conservative upper-bound mapping" in by_mol["mol_benzene"]["notes"]
    assert "grouped class upper-bound parse" in by_mol["mol_benzene"]["method"]
    assert "mol_argon" not in by_mol
