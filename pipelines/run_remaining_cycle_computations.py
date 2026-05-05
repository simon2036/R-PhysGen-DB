"""Run or probe the remaining active-learning cycle computations."""

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

from r_physgen_db.cycle_retry import (  # noqa: E402
    DEFAULT_ARTIFACT_DIR,
    DEFAULT_RESULTS_PATH,
    build_run_cycle_retry_manifest,
    run_cycle_retry,
)
from r_physgen_db.constants import DATA_DIR  # noqa: E402
from r_physgen_db.utils import ensure_directory, load_yaml, write_json  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Retry remaining run_cycle active-learning computations.")
    parser.add_argument("--backend", choices=["auto", "coolprop", "refprop"], default="auto")
    parser.add_argument("--refprop-root", default="", help="REFPROP root directory; defaults to COOLPROP_REFPROP_ROOT.")
    parser.add_argument("--completion-required", action="store_true", help="Exit non-zero unless every manifest entry succeeds.")
    parser.add_argument("--write-results", action="store_true", help="Write successful property rows to cycle_backend_results.csv.")
    parser.add_argument("--probe-only", action="store_true", help="Write manifest/probe/blockers but do not persist result rows.")
    parser.add_argument("--active-learning-queue", type=Path, default=DATA_DIR / "gold" / "active_learning_queue.parquet")
    parser.add_argument("--molecule-core", type=Path, default=DATA_DIR / "silver" / "molecule_core.parquet")
    parser.add_argument("--molecule-alias", type=Path, default=DATA_DIR / "silver" / "molecule_alias.parquet")
    parser.add_argument("--seed-catalog", type=Path, default=DATA_DIR / "raw" / "manual" / "seed_catalog.csv")
    parser.add_argument("--coolprop-aliases", type=Path, default=DATA_DIR / "raw" / "manual" / "coolprop_aliases.yaml")
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    parser.add_argument("--results-path", type=Path, default=DEFAULT_RESULTS_PATH)
    args = parser.parse_args()

    active_learning_queue = _read_table(args.active_learning_queue)
    molecule_core = _read_table(args.molecule_core)
    molecule_alias = _read_table(args.molecule_alias)
    seed_catalog = pd.read_csv(args.seed_catalog).fillna("") if args.seed_catalog.exists() else pd.DataFrame()
    coolprop_aliases = load_yaml(args.coolprop_aliases).get("mappings", {}) if args.coolprop_aliases.exists() else {}

    manifest = build_run_cycle_retry_manifest(
        active_learning_queue=active_learning_queue,
        molecule_core=molecule_core,
        molecule_alias=molecule_alias,
        seed_catalog=seed_catalog,
        coolprop_aliases=coolprop_aliases,
    )

    ensure_directory(args.artifact_dir)
    manifest_path = args.artifact_dir / "remaining_cycle_retry_manifest.csv"
    attempts_path = args.artifact_dir / "remaining_cycle_attempts.csv"
    blockers_path = args.artifact_dir / "remaining_cycle_blockers.csv"
    summary_path = args.artifact_dir / "remaining_cycle_summary.json"
    manifest.to_csv(manifest_path, index=False)

    run = run_cycle_retry(
        manifest,
        backend=args.backend,
        refprop_root=args.refprop_root or None,
        artifact_dir=args.artifact_dir,
        write_results=bool(args.write_results and not args.probe_only),
        results_path=args.results_path,
    )
    run.attempts.to_csv(attempts_path, index=False)
    run.blockers.to_csv(blockers_path, index=False)
    summary = {
        **run.summary,
        "manifest_path": str(manifest_path),
        "attempts_path": str(attempts_path),
        "blockers_path": str(blockers_path),
        "probe_only": bool(args.probe_only),
    }
    write_json(summary_path, summary)
    print(json.dumps(summary, indent=2, ensure_ascii=False))

    if args.completion_required and run.summary["succeeded_queue_entries"] != run.summary["queue_entry_count"]:
        raise SystemExit(2)


def _read_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path).fillna("")


if __name__ == "__main__":
    main()
