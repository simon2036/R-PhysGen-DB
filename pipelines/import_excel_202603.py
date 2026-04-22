"""Import workbook-derived observations and Tier D candidates from 制冷剂数据库202603.xlsx."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from r_physgen_db.sources.excel_202603 import (  # noqa: E402
    build_excel_202603_outputs,
    find_default_workbook_path,
)
from r_physgen_db.utils import ensure_directory, write_text  # noqa: E402


DEFAULT_MOLECULE_CORE = ROOT / "data" / "silver" / "molecule_core.parquet"
DEFAULT_MOLECULE_ALIAS = ROOT / "data" / "silver" / "molecule_alias.parquet"
DEFAULT_PROPERTY_RECOMMENDED = ROOT / "data" / "gold" / "property_recommended.parquet"
DEFAULT_OBSERVATIONS_OUT = ROOT / "data" / "raw" / "manual" / "observations" / "excel_202603_observations.csv"
DEFAULT_CANDIDATES_OUT = ROOT / "data" / "raw" / "generated" / "excel_202603_tierd_candidates.csv"
DEFAULT_STAGING_OUT = ROOT / "data" / "raw" / "generated" / "excel_202603_name_only_staging.csv"
DEFAULT_REPORT_OUT = ROOT / "docs" / "excel_202603_brief_report.md"


def _read_required_parquet(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required {label} parquet is missing: {path}")
    return pd.read_parquet(path)


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    ensure_directory(path.parent)
    frame.to_csv(path, index=False, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Import workbook-derived observations and Tier D candidates from 制冷剂数据库202603.xlsx.")
    parser.add_argument("--workbook", type=Path, default=None, help="Path to the Excel workbook. Defaults to methods/*202603*.xlsx.")
    parser.add_argument("--molecule-core", type=Path, default=DEFAULT_MOLECULE_CORE, help="Path to silver/molecule_core.parquet.")
    parser.add_argument("--molecule-alias", type=Path, default=DEFAULT_MOLECULE_ALIAS, help="Path to silver/molecule_alias.parquet.")
    parser.add_argument("--property-recommended", type=Path, default=DEFAULT_PROPERTY_RECOMMENDED, help="Path to gold/property_recommended.parquet.")
    parser.add_argument("--observations-out", type=Path, default=DEFAULT_OBSERVATIONS_OUT, help="Output CSV for workbook-derived observation rows.")
    parser.add_argument("--candidates-out", type=Path, default=DEFAULT_CANDIDATES_OUT, help="Output CSV for generated Tier D seed rows.")
    parser.add_argument("--staging-out", type=Path, default=DEFAULT_STAGING_OUT, help="Output CSV for workbook-only name staging rows.")
    parser.add_argument("--report-out", type=Path, default=DEFAULT_REPORT_OUT, help="Output Markdown path for the brief report.")
    args = parser.parse_args()

    workbook_path = args.workbook or find_default_workbook_path(ROOT)
    molecule_core = _read_required_parquet(args.molecule_core, "molecule_core")
    molecule_alias = _read_required_parquet(args.molecule_alias, "molecule_alias")
    property_recommended = _read_required_parquet(args.property_recommended, "property_recommended")

    outputs = build_excel_202603_outputs(
        workbook_path=workbook_path,
        molecule_core=molecule_core,
        molecule_alias=molecule_alias,
        property_recommended=property_recommended,
    )

    _write_csv(outputs["supplement_rows"], args.observations_out)
    _write_csv(outputs["candidate_rows"], args.candidates_out)
    _write_csv(outputs["name_only_staging_rows"], args.staging_out)
    ensure_directory(args.report_out.parent)
    write_text(args.report_out, outputs["report_markdown"])

    summary = {
        **outputs["summary"],
        "workbook_path": str(workbook_path),
        "observations_out": str(args.observations_out),
        "candidates_out": str(args.candidates_out),
        "staging_out": str(args.staging_out),
        "report_out": str(args.report_out),
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
