from __future__ import annotations

import pandas as pd

from r_physgen_db.pipeline import _assign_scaffold_splits


def test_scaffold_assignment_has_no_leakage() -> None:
    df = pd.DataFrame(
        [
            {"mol_id": "a", "scaffold_key": "S1"},
            {"mol_id": "b", "scaffold_key": "S1"},
            {"mol_id": "c", "scaffold_key": "S2"},
            {"mol_id": "d", "scaffold_key": "S3"},
            {"mol_id": "e", "scaffold_key": "S3"},
            {"mol_id": "f", "scaffold_key": "S4"},
        ]
    )
    split_map = _assign_scaffold_splits(df)
    by_scaffold = {}
    for _, row in df.iterrows():
        by_scaffold.setdefault(row["scaffold_key"], set()).add(split_map[row["mol_id"]])
    assert all(len(splits) == 1 for splits in by_scaffold.values())
