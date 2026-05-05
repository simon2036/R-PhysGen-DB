from __future__ import annotations

import pandas as pd

from r_physgen_db.coverage_worklist import build_promoted_coverage_outputs


def test_promoted_coverage_worklist_ignores_tier_d_inventory_rows() -> None:
    seed_catalog = pd.DataFrame(
        [
            {
                "seed_id": "seed_a",
                "r_number": "R-A",
                "coverage_tier": "A",
                "model_inclusion": "yes",
                "entity_scope": "refrigerant",
            },
            {
                "seed_id": "seed_d",
                "r_number": "R-D",
                "coverage_tier": "D",
                "model_inclusion": "no",
                "entity_scope": "candidate",
            },
        ]
    )
    molecule_core = pd.DataFrame(
        [
            {"seed_id": "seed_a", "mol_id": "mol_a", "canonical_smiles": "C"},
            {"seed_id": "seed_d", "mol_id": "mol_d", "canonical_smiles": "CC"},
        ]
    )
    recommended = pd.DataFrame(
        [
            {"mol_id": "mol_a", "property_name": "boiling_point_c"},
            {"mol_id": "mol_d", "property_name": "boiling_point_c"},
        ]
    )

    coverage, worklist = build_promoted_coverage_outputs(
        seed_catalog,
        molecule_core,
        recommended,
        required_properties=["boiling_point_c", "odp"],
    )

    assert set(coverage["molecule_count"]) == {0, 1}
    assert coverage.loc[coverage["coverage_tier"].eq("A"), "molecule_count"].unique().tolist() == [1]
    assert coverage.loc[coverage["property_name"].eq("boiling_point_c"), "coverage_fraction"].iloc[0] == 1.0
    assert {
        "r_number",
        "canonical_smiles",
        "canonical_feature_key",
        "missing_feature_key",
    } <= set(worklist.columns)
    assert worklist[
        ["mol_id", "r_number", "canonical_smiles", "coverage_tier", "property_name", "missing_feature_key"]
    ].to_dict(orient="records") == [
        {
            "mol_id": "mol_a",
            "r_number": "R-A",
            "canonical_smiles": "C",
            "coverage_tier": "A",
            "property_name": "odp",
            "missing_feature_key": "environmental.odp",
        }
    ]
