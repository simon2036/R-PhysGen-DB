#!/usr/bin/env python
"""Run ORCA B3LYP-D3BJ/def2-SVP opt/freq jobs for phase-2 governance requests."""

from __future__ import annotations

import argparse
from pathlib import Path

from r_physgen_db.phase2_quantum import (
    DEFAULT_PHASE2_ARTIFACT_DIR,
    DEFAULT_PHASE2_ATOMIC_CHARGES_PATH,
    DEFAULT_PHASE2_VIBRATIONAL_MODES_PATH,
    QUANTUM_RESULTS_PATH,
    run_orca_phase2_optfreq,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=Path, required=True)
    parser.add_argument("--xyz-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=QUANTUM_RESULTS_PATH)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_PHASE2_ARTIFACT_DIR / "orca_optfreq")
    parser.add_argument("--vibrational-modes", type=Path, default=DEFAULT_PHASE2_VIBRATIONAL_MODES_PATH)
    parser.add_argument("--atomic-charges", type=Path, default=DEFAULT_PHASE2_ATOMIC_CHARGES_PATH)
    parser.add_argument("--orca-bin", type=Path, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--nprocs-per-job", type=int, default=8)
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--allow-missing-executor", action="store_true")
    parser.add_argument("--retry-failed-only", action="store_true")
    parser.add_argument("--completion-required", action="store_true")
    args = parser.parse_args()
    summary = run_orca_phase2_optfreq(
        requests_path=args.requests,
        xyz_manifest_path=args.xyz_manifest,
        output_path=args.output,
        artifact_dir=args.artifact_dir,
        vibrational_modes_path=args.vibrational_modes,
        atomic_charges_path=args.atomic_charges,
        orca_bin=args.orca_bin,
        limit=args.limit,
        jobs=args.jobs,
        nprocs_per_job=args.nprocs_per_job,
        resume=args.resume,
        allow_missing_executor=args.allow_missing_executor,
        retry_failed_only=args.retry_failed_only,
        completion_required=args.completion_required,
    )
    print(summary)


if __name__ == "__main__":
    main()
