#!/usr/bin/env python
"""Run Psi4/DFT for the generated production quantum DFT request manifest."""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

from r_physgen_db.constants import DATA_DIR
from r_physgen_db.psi4_quantum import run_psi4_quantum_pilot


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=Path, default=DATA_DIR / "raw" / "generated" / "quantum_dft_requests.csv")
    parser.add_argument("--xyz-manifest", type=Path, default=DATA_DIR / "raw" / "generated" / "quantum_dft_xyz_manifest.csv")
    parser.add_argument("--output", type=Path, default=DATA_DIR / "raw" / "manual" / "quantum_pilot_results.csv")
    parser.add_argument("--artifact-dir", type=Path, default=DATA_DIR / "raw" / "manual" / "quantum_pilot_artifacts")
    parser.add_argument("--scratch-dir", type=Path, default=DATA_DIR / "raw" / "manual" / "quantum_pilot_artifacts" / "_psi4_scratch")
    parser.add_argument("--psi4-bin", type=Path, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--allow-missing-executor", action="store_true")
    parser.add_argument("--retry-failed-only", action="store_true", help="Only rerun request IDs with existing failed Psi4 audit rows.")
    parser.add_argument("--completion-required", action="store_true", help="Exit nonzero if targeted Psi4 requests remain incomplete after retry.")
    args = parser.parse_args()

    psi4_bin = args.psi4_bin or _default_psi4_bin()
    summary = run_psi4_quantum_pilot(
        requests_path=args.requests,
        xyz_manifest_path=args.xyz_manifest,
        output_path=args.output,
        artifact_dir=args.artifact_dir,
        psi4_bin=psi4_bin,
        limit=args.limit,
        resume=args.resume,
        scratch_dir=args.scratch_dir,
        allow_missing_executor=args.allow_missing_executor,
        jobs=args.jobs,
        retry_failed_only=args.retry_failed_only,
        completion_required=args.completion_required,
    )
    print(summary)


def _default_psi4_bin() -> Path | None:
    env_value = os.getenv("R_PHYSGEN_PSI4_BIN", "").strip()
    if env_value:
        path = Path(env_value)
        if path.exists():
            return path
        resolved = shutil.which(env_value)
        if resolved:
            return Path(resolved)
    resolved = shutil.which("psi4")
    return Path(resolved) if resolved else None


if __name__ == "__main__":
    main()
