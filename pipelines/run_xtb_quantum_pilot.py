#!/usr/bin/env python
"""Run xTB for the generated quantum pilot request manifest."""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

from r_physgen_db.constants import DATA_DIR
from r_physgen_db.xtb_quantum import run_xtb_quantum_pilot


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=Path, default=DATA_DIR / "raw" / "generated" / "quantum_pilot_requests.csv")
    parser.add_argument("--xyz-manifest", type=Path, default=DATA_DIR / "raw" / "generated" / "quantum_pilot_xyz_manifest.csv")
    parser.add_argument("--output", type=Path, default=DATA_DIR / "raw" / "manual" / "quantum_pilot_results.csv")
    parser.add_argument("--artifact-dir", type=Path, default=DATA_DIR / "raw" / "manual" / "quantum_pilot_artifacts")
    parser.add_argument("--xtb-bin", type=Path, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--threads-per-job", type=int, default=1)
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--allow-missing-executor", action="store_true")
    parser.add_argument("--retry-failed-only", action="store_true", help="Only rerun request IDs with existing failed xTB audit rows.")
    parser.add_argument("--completion-required", action="store_true", help="Exit nonzero if targeted xTB requests remain incomplete after retry.")
    parser.add_argument(
        "--retry-profile",
        action="append",
        dest="retry_profiles",
        help="Retry profile to run, in order. May be passed multiple times; defaults to the hardened production profile ladder.",
    )
    args = parser.parse_args()

    xtb_bin = args.xtb_bin or _default_xtb_bin()
    if xtb_bin is None and not args.allow_missing_executor:
        raise SystemExit("xTB executable not found; set R_PHYSGEN_XTB_BIN or pass --xtb-bin")

    summary = run_xtb_quantum_pilot(
        requests_path=args.requests,
        xyz_manifest_path=args.xyz_manifest,
        output_path=args.output,
        artifact_dir=args.artifact_dir,
        xtb_bin=xtb_bin,
        limit=args.limit,
        jobs=args.jobs,
        threads_per_job=args.threads_per_job,
        resume=args.resume,
        allow_missing_executor=args.allow_missing_executor,
        retry_failed_only=args.retry_failed_only,
        completion_required=args.completion_required,
        retry_profiles=args.retry_profiles,
    )
    print(summary)


def _default_xtb_bin() -> Path | None:
    env_value = os.getenv("R_PHYSGEN_XTB_BIN", "").strip()
    if env_value:
        path = Path(env_value)
        if path.exists():
            return path
        resolved = shutil.which(env_value)
        if resolved:
            return Path(resolved)
    resolved = shutil.which("xtb")
    return Path(resolved) if resolved else None


if __name__ == "__main__":
    main()
