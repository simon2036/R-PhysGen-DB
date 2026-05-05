from __future__ import annotations

import importlib.util

import pandas as pd

from r_physgen_db.constants import DATA_DIR, PROJECT_ROOT


_SPEC = importlib.util.spec_from_file_location(
    "pr_b_equivalence_check",
    PROJECT_ROOT / "scripts" / "pr_b_equivalence_check.py",
)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
compare_table = _MODULE.compare_table


def test_dataset_version_file_matches_quality_report() -> None:
    version_path = DATA_DIR / "gold" / "VERSION"
    quality_report_path = DATA_DIR / "gold" / "quality_report.json"

    assert version_path.exists()
    assert quality_report_path.exists()
    assert version_path.read_text(encoding="utf-8").strip() in quality_report_path.read_text(encoding="utf-8")


def test_equivalence_checker_passes_identical_fixture(tmp_path) -> None:
    before = tmp_path / "before.parquet"
    after = tmp_path / "after.parquet"
    frame = pd.DataFrame({"mol_id": ["a", "b"], "value": [1.0, 2.0]})
    frame.to_parquet(before, index=False)
    frame.to_parquet(after, index=False)

    result = compare_table(before, after, tolerance=1e-12)

    assert result["status"] == "passed"


def test_equivalence_checker_fails_column_and_numeric_drift(tmp_path) -> None:
    before = tmp_path / "before.parquet"
    after = tmp_path / "after.parquet"
    pd.DataFrame({"mol_id": ["a", "b"], "value": [1.0, 2.0]}).to_parquet(before, index=False)
    pd.DataFrame({"mol_id": ["a", "b"], "extra": ["x", "y"], "value": [1.0, 2.5]}).to_parquet(after, index=False)

    result = compare_table(before, after, tolerance=1e-12)

    assert result["status"] == "failed"
    assert any("column order/set mismatch" in error for error in result["errors"])
    assert any("value.mean diff" in error for error in result["errors"])
