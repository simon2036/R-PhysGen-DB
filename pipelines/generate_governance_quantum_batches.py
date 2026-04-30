#!/usr/bin/env python
"""Generate governance-driven xTB pregeometry or Psi4/DFT singlepoint batches."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from r_physgen_db.governance_quantum_batches import (
    DEFAULT_DFT_XYZ_DIR,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_XTB_XYZ_DIR,
    GOVERNANCE_QUEUE_PATH,
    MOLECULE_CORE_PATH,
    QUANTUM_ARTIFACT_PATH,
    QUANTUM_JOB_PATH,
    QUANTUM_RESULTS_PATH,
    SEED_CATALOG_PATH,
    materialize_governance_quantum_batches,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["xtb-pregeometry", "psi4-singlepoint"], required=True)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--queue", type=Path, default=GOVERNANCE_QUEUE_PATH)
    parser.add_argument("--seed-catalog", type=Path, default=SEED_CATALOG_PATH)
    parser.add_argument("--molecule-core", type=Path, default=MOLECULE_CORE_PATH)
    parser.add_argument("--quantum-job", type=Path, default=QUANTUM_JOB_PATH)
    parser.add_argument("--quantum-results", type=Path, default=QUANTUM_RESULTS_PATH)
    parser.add_argument("--quantum-artifact", type=Path, default=QUANTUM_ARTIFACT_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--xyz-dir", type=Path, default=None)
    parser.add_argument(
        "--executor-available",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override local executor detection for request status materialization.",
    )
    args = parser.parse_args()

    xyz_dir = args.xyz_dir or (DEFAULT_XTB_XYZ_DIR if args.mode == "xtb-pregeometry" else DEFAULT_DFT_XYZ_DIR)
    summary = materialize_governance_quantum_batches(
        mode=args.mode,
        queue=_read_frame(args.queue),
        seed_catalog=_read_frame(args.seed_catalog),
        molecule_core=_read_frame(args.molecule_core),
        quantum_job=_read_frame(args.quantum_job, required=False),
        quantum_results=_read_frame(args.quantum_results, required=False),
        quantum_artifact=_read_frame(args.quantum_artifact, required=False),
        output_dir=args.output_dir,
        xyz_dir=xyz_dir,
        batch_size=args.batch_size,
        tools_available=args.executor_available,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


def _read_frame(path: Path, *, required: bool = True) -> pd.DataFrame:
    if not path.exists():
        if required:
            raise FileNotFoundError(path)
        return pd.DataFrame()
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path).fillna("")
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path).fillna("")
    raise ValueError(f"unsupported tabular input format: {path}")


if __name__ == "__main__":
    main()
