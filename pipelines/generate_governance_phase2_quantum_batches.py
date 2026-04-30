#!/usr/bin/env python
"""Generate phase-2 governance xTB Hessian, CREST, and ORCA opt/freq batches."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from r_physgen_db.phase2_quantum import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_PHASE2_XYZ_DIR,
    GOVERNANCE_QUEUE_PATH,
    MOLECULE_CORE_PATH,
    QUANTUM_ARTIFACT_PATH,
    QUANTUM_JOB_PATH,
    QUANTUM_RESULTS_PATH,
    SEED_CATALOG_PATH,
    materialize_governance_phase2_batches,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mapped-only", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--batch-size-orca", type=int, default=20)
    parser.add_argument("--orca-smoke-size", type=int, default=3)
    parser.add_argument("--crest-heavy-atom-min", type=int, default=6)
    parser.add_argument("--queue", type=Path, default=GOVERNANCE_QUEUE_PATH)
    parser.add_argument("--seed-catalog", type=Path, default=SEED_CATALOG_PATH)
    parser.add_argument("--molecule-core", type=Path, default=MOLECULE_CORE_PATH)
    parser.add_argument("--quantum-job", type=Path, default=QUANTUM_JOB_PATH)
    parser.add_argument("--quantum-results", type=Path, default=QUANTUM_RESULTS_PATH)
    parser.add_argument("--quantum-artifact", type=Path, default=QUANTUM_ARTIFACT_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--xyz-dir", type=Path, default=DEFAULT_PHASE2_XYZ_DIR)
    parser.add_argument("--xtb-available", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--crest-available", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--orca-available", action=argparse.BooleanOptionalAction, default=None)
    args = parser.parse_args()

    summary = materialize_governance_phase2_batches(
        queue=_read_frame(args.queue),
        seed_catalog=_read_frame(args.seed_catalog),
        molecule_core=_read_frame(args.molecule_core),
        quantum_job=_read_frame(args.quantum_job, required=False),
        quantum_results=_read_frame(args.quantum_results, required=False),
        quantum_artifact=_read_frame(args.quantum_artifact, required=False),
        output_dir=args.output_dir,
        xyz_dir=args.xyz_dir,
        crest_heavy_atom_min=args.crest_heavy_atom_min,
        orca_smoke_size=args.orca_smoke_size,
        batch_size_orca=args.batch_size_orca,
        mapped_only=args.mapped_only,
        xtb_available=args.xtb_available,
        crest_available=args.crest_available,
        orca_available=args.orca_available,
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
