"""Acquire and filter bulk PubChem Tier D candidate pool."""

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

from r_physgen_db.constants import DATA_DIR  # noqa: E402
from r_physgen_db.sources.pubchem_bulk import (  # noqa: E402
    DEFAULT_EXTRAS_FILES,
    build_pubchem_coarse_filter_summary,
    build_pubchem_candidate_pool,
    build_pubchem_candidate_pool_streaming,
    download_pubchem_extras,
)


def _build_coarse_summary_payload(summary: pd.DataFrame, metadata: dict[str, object]) -> dict[str, object]:
    reason_totals = (
        summary.groupby(["coarse_filter_reason", "reason_order"], as_index=False)["cid_count"]
        .sum()
        .sort_values(["reason_order", "cid_count"], ascending=[True, False])
    )
    carbon_bucket_totals = (
        summary.groupby(
            ["coarse_filter_reason", "reason_order", "carbon_bucket", "carbon_bucket_order"],
            as_index=False,
        )["cid_count"]
        .sum()
        .sort_values(["reason_order", "carbon_bucket_order"])
    )
    mass_bucket_totals = (
        summary.groupby(
            ["coarse_filter_reason", "reason_order", "mass_bucket", "mass_bucket_order"],
            as_index=False,
        )["cid_count"]
        .sum()
        .sort_values(["reason_order", "mass_bucket_order"])
    )
    element_pattern_totals = (
        summary.groupby(["coarse_filter_reason", "reason_order", "element_pattern"], as_index=False)["cid_count"]
        .sum()
        .sort_values(["reason_order", "cid_count", "element_pattern"], ascending=[True, False, True])
    )
    top_element_patterns = (
        element_pattern_totals.groupby("coarse_filter_reason", group_keys=False)
        .head(25)
        .reset_index(drop=True)
    )
    top_combination_rows = (
        summary.sort_values(["cid_count", "reason_order", "element_pattern"], ascending=[False, True, True])
        .head(250)
        .reset_index(drop=True)
    )
    return {
        **metadata,
        "summary_row_count": int(len(summary)),
        "reason_totals": json.loads(reason_totals.to_json(orient="records")),
        "carbon_bucket_totals": json.loads(carbon_bucket_totals.to_json(orient="records")),
        "mass_bucket_totals": json.loads(mass_bucket_totals.to_json(orient="records")),
        "top_element_patterns": json.loads(top_element_patterns.to_json(orient="records")),
        "top_combination_rows": json.loads(top_combination_rows.to_json(orient="records")),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Acquire PubChem bulk candidate pool for Tier D screening.")
    parser.add_argument("--extras-dir", default=str(DATA_DIR / "raw" / "pubchem_bulk" / "extras"))
    parser.add_argument("--refresh-remote", action="store_true")
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--download-only", action="store_true", help="Download PubChem Extras files without building the candidate pool.")
    parser.add_argument("--summary-only", action="store_true", help="Build only the DuckDB coarse-filter aggregate summary from CID-Mass.gz.")
    parser.add_argument("--cid-limit", type=int, default=None, help="Optional cap for local trial runs.")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--chunk-size", type=int, default=1024 * 1024, help="Per-request streaming chunk size in bytes.")
    parser.add_argument("--max-retries", type=int, default=4)
    parser.add_argument("--retry-sleep-seconds", type=float, default=8.0)
    parser.add_argument("--file-sleep-seconds", type=float, default=3.0, help="Pause between sequential file downloads.")
    parser.add_argument("--max-mib-per-second", type=float, default=None, help="Optional bandwidth cap per sequential download.")
    args = parser.parse_args()

    extras_dir = Path(args.extras_dir)
    if args.skip_download:
        extras_paths = {key: extras_dir / filename for key, filename in DEFAULT_EXTRAS_FILES.items()}
    else:
        extras_paths = download_pubchem_extras(
            extras_dir,
            refresh_remote=args.refresh_remote,
            timeout=args.timeout,
            chunk_size=args.chunk_size,
            max_retries=args.max_retries,
            retry_sleep_seconds=args.retry_sleep_seconds,
            file_sleep_seconds=args.file_sleep_seconds,
            max_bytes_per_second=int(args.max_mib_per_second * 1024 * 1024) if args.max_mib_per_second else None,
        )

    if args.download_only:
        payload = {
            "extras_dir": str(extras_dir),
            "downloaded_files": {
                key: {
                    "path": str(path),
                    "size_bytes": path.stat().st_size if path.exists() else 0,
                }
                for key, path in extras_paths.items()
            },
        }
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return

    coarse_summary_path = DATA_DIR / "bronze" / "coarse_filter_summary.parquet"
    coarse_summary_json_path = DATA_DIR / "bronze" / "coarse_filter_summary.json"
    coarse_summary_path.parent.mkdir(parents=True, exist_ok=True)

    coarse_summary, coarse_summary_metadata = build_pubchem_coarse_filter_summary(
        extras_paths["mass"],
        cid_limit=args.cid_limit,
    )
    coarse_summary.to_parquet(coarse_summary_path, index=False)
    coarse_summary_json_path.write_text(
        json.dumps(_build_coarse_summary_payload(coarse_summary, coarse_summary_metadata), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    if args.summary_only:
        payload = {
            "coarse_filter_summary_path": str(coarse_summary_path),
            "coarse_filter_summary_json_path": str(coarse_summary_json_path),
            **coarse_summary_metadata,
            "summary_row_count": int(len(coarse_summary)),
        }
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return

    existing_molecule_core_path = DATA_DIR / "silver" / "molecule_core.parquet"
    existing_molecule_core = pd.read_parquet(existing_molecule_core_path) if existing_molecule_core_path.exists() else pd.DataFrame()

    builder = build_pubchem_candidate_pool if args.cid_limit is not None else build_pubchem_candidate_pool_streaming
    candidate_pool, audit = builder(
        smiles_path=extras_paths["smiles"],
        inchikey_path=extras_paths["inchi_key"],
        mass_path=extras_paths["mass"],
        component_path=extras_paths["component"],
        synonym_path=extras_paths["synonym"],
        existing_molecule_core=existing_molecule_core,
        cid_limit=args.cid_limit,
    )

    candidate_pool_path = DATA_DIR / "bronze" / "pubchem_candidate_pool.parquet"
    audit_path = DATA_DIR / "bronze" / "pubchem_candidate_filter_audit.parquet"
    candidate_pool_path.parent.mkdir(parents=True, exist_ok=True)
    candidate_pool.to_parquet(candidate_pool_path, index=False)
    audit.to_parquet(audit_path, index=False)

    print(
        json.dumps(
            {
                "coarse_filter_summary_path": str(coarse_summary_path),
                "coarse_filter_summary_json_path": str(coarse_summary_json_path),
                "coarse_filter_summary_count": int(len(coarse_summary)),
                **coarse_summary_metadata,
                "candidate_pool_path": str(candidate_pool_path),
                "candidate_pool_count": int(len(candidate_pool)),
                "audit_path": str(audit_path),
                "audit_count": int(len(audit)),
            },
            indent=2,
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
