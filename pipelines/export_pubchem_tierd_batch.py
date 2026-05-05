"""Export a controlled Tier D seed batch from the PubChem bulk candidate pool."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from r_physgen_db.sources.pubchem_bulk import export_tierd_seed_rows  # noqa: E402
from r_physgen_db.constants import DATA_DIR  # noqa: E402
from pipelines.generate_wave2_seed_catalog import FIELDNAMES  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Export PubChem bulk Tier D seed rows.")
    parser.add_argument("--input-pool", default=str(DATA_DIR / "bronze" / "pubchem_candidate_pool.parquet"))
    parser.add_argument("--output", default=str(DATA_DIR / "raw" / "generated" / "pubchem_tierd_candidates.csv"))
    parser.add_argument("--limit", type=int, default=5000)
    args = parser.parse_args()

    input_pool = Path(args.input_pool)
    output = Path(args.output)

    candidate_pool = pd.read_parquet(input_pool)
    rows = export_tierd_seed_rows(candidate_pool, limit=args.limit)

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(json.dumps({"output": str(output), "row_count": len(rows)}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
