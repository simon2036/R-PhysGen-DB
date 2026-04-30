#!/usr/bin/env python
"""Run CREST conformer searches or singleton artifacts for phase-2 governance requests."""

from __future__ import annotations

import argparse
from pathlib import Path

from r_physgen_db.phase2_quantum import (
    DEFAULT_PHASE2_ARTIFACT_DIR,
    DEFAULT_PHASE2_CONFORMER_DETAIL_PATH,
    QUANTUM_RESULTS_PATH,
    run_crest_conformer_phase2,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=Path, required=True)
    parser.add_argument("--xyz-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=QUANTUM_RESULTS_PATH)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_PHASE2_ARTIFACT_DIR / "crest_conformer")
    parser.add_argument("--conformer-detail", type=Path, default=DEFAULT_PHASE2_CONFORMER_DETAIL_PATH)
    parser.add_argument("--crest-bin", type=Path, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--threads-per-job", type=int, default=1)
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--allow-missing-executor", action="store_true")
    parser.add_argument("--retry-failed-only", action="store_true")
    parser.add_argument("--completion-required", action="store_true")
    args = parser.parse_args()
    summary = run_crest_conformer_phase2(
        requests_path=args.requests,
        xyz_manifest_path=args.xyz_manifest,
        output_path=args.output,
        artifact_dir=args.artifact_dir,
        conformer_detail_path=args.conformer_detail,
        crest_bin=args.crest_bin,
        limit=args.limit,
        jobs=args.jobs,
        threads_per_job=args.threads_per_job,
        resume=args.resume,
        allow_missing_executor=args.allow_missing_executor,
        retry_failed_only=args.retry_failed_only,
        completion_required=args.completion_required,
    )
    print(summary)


if __name__ == "__main__":
    main()
