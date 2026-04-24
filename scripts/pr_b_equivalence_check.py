#!/usr/bin/env python3
"""Check output equivalence before/after PR-B pipeline stage refactor.

This script compares selected Parquet outputs from the old monolithic pipeline
and the staged pipeline. It is intended as a CI gate for PR-B.

Usage:
  python scripts/pr_b_equivalence_check.py \
      --before /path/to/before/data/gold \
      --after /path/to/after/data/gold \
      --tables model_ready.parquet property_matrix.parquet molecule_master.parquet
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def _numeric_summary(df: pd.DataFrame) -> dict[str, dict[str, float | int]]:
    result: dict[str, dict[str, float | int]] = {}
    for col in df.select_dtypes(include="number").columns:
        s = df[col]
        result[col] = {
            "mean": float(s.mean()) if len(s) else 0.0,
            "std": float(s.std()) if len(s) else 0.0,
            "null_count": int(s.isna().sum()),
        }
    return result


def _null_rate(df: pd.DataFrame) -> dict[str, float]:
    n = len(df)
    if n == 0:
        return {col: 0.0 for col in df.columns}
    return {col: float(df[col].isna().sum() / n) for col in df.columns}


def compare_table(before_path: Path, after_path: Path, *, tolerance: float) -> dict:
    before = pd.read_parquet(before_path)
    after = pd.read_parquet(after_path)

    errors: list[str] = []
    warnings: list[str] = []

    if len(before) != len(after):
        errors.append(f"row_count mismatch: {len(before)} != {len(after)}")

    before_cols = list(before.columns)
    after_cols = list(after.columns)
    if before_cols != after_cols:
        errors.append("column order/set mismatch")
        missing = sorted(set(before_cols) - set(after_cols))
        extra = sorted(set(after_cols) - set(before_cols))
        if missing:
            warnings.append(f"missing columns: {missing}")
        if extra:
            warnings.append(f"extra columns: {extra}")

    common_cols = [c for c in before_cols if c in after.columns]
    before_null = _null_rate(before[common_cols])
    after_null = _null_rate(after[common_cols])
    for col in common_cols:
        if before_null[col] != after_null[col]:
            errors.append(f"null_rate mismatch for {col}: {before_null[col]} != {after_null[col]}")

    before_num = _numeric_summary(before[common_cols])
    after_num = _numeric_summary(after[common_cols])
    for col, stats in before_num.items():
        if col not in after_num:
            continue
        for metric in ("mean", "std"):
            diff = abs(float(stats[metric]) - float(after_num[col][metric]))
            if diff > tolerance:
                errors.append(f"{col}.{metric} diff {diff} > tolerance {tolerance}")

    return {
        "table": before_path.name,
        "before_rows": len(before),
        "after_rows": len(after),
        "status": "failed" if errors else "passed",
        "errors": errors,
        "warnings": warnings,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--before", required=True, help="Directory containing baseline parquet files")
    parser.add_argument("--after", required=True, help="Directory containing staged parquet files")
    parser.add_argument("--tables", nargs="+", required=True)
    parser.add_argument("--tolerance", type=float, default=1e-12)
    parser.add_argument("--out-report", default="")
    args = parser.parse_args()

    before_dir = Path(args.before)
    after_dir = Path(args.after)
    results = []
    for table in args.tables:
        result = compare_table(before_dir / table, after_dir / table, tolerance=args.tolerance)
        results.append(result)

    report = {
        "status": "failed" if any(r["status"] == "failed" for r in results) else "passed",
        "tolerance": args.tolerance,
        "tables": results,
    }

    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.out_report:
        Path(args.out_report).write_text(text, encoding="utf-8")
    print(text)

    if report["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
