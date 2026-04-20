from __future__ import annotations

from r_physgen_db.chemistry import compute_structure_features, standardize_smiles


def test_standardize_preserves_ez_isomer() -> None:
    e = standardize_smiles("F/C=C(/F)C(F)(F)F")
    z = standardize_smiles("F/C=C(\\F)C(F)(F)F")
    assert e["inchikey"] != z["inchikey"]
    assert e["ez_isomer"] != z["ez_isomer"]


def test_feature_generation_produces_selfies_and_scaffold() -> None:
    features = compute_structure_features("C=C(C(F)(F)F)F")
    assert features["selfies"]
    assert features["scaffold_key"]
    assert features["double_bond_count"] >= 1
