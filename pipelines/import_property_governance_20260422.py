"""Stage the 2026-04-22 property governance bundle into derived artifacts."""

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

from r_physgen_db.constants import PARSER_VERSION  # noqa: E402
from r_physgen_db.sources.property_governance_bundle import (  # noqa: E402
    default_bundle_path,
    default_unresolved_curation_path,
    integrate_property_governance_bundle,
)
from r_physgen_db.utils import now_iso  # noqa: E402


DEFAULT_MOLECULE_CORE = ROOT / "data" / "silver" / "molecule_core.parquet"
DEFAULT_MOLECULE_ALIAS = ROOT / "data" / "silver" / "molecule_alias.parquet"
DEFAULT_SEED_CATALOG = ROOT / "data" / "raw" / "manual" / "seed_catalog.csv"


def _read_required_parquet(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required {label} parquet is missing: {path}")
    return pd.read_parquet(path).fillna("")


def _read_required_csv(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required {label} csv is missing: {path}")
    return pd.read_csv(path).fillna("")


def main() -> None:
    parser = argparse.ArgumentParser(description="Import the 2026-04-22 property governance bundle.")
    parser.add_argument("--bundle", type=Path, default=default_bundle_path(ROOT), help="Path to the governance bundle zip.")
    parser.add_argument("--seed-catalog", type=Path, default=DEFAULT_SEED_CATALOG, help="Path to raw/manual/seed_catalog.csv.")
    parser.add_argument("--molecule-core", type=Path, default=DEFAULT_MOLECULE_CORE, help="Path to silver/molecule_core.parquet.")
    parser.add_argument("--molecule-alias", type=Path, default=DEFAULT_MOLECULE_ALIAS, help="Path to silver/molecule_alias.parquet.")
    parser.add_argument(
        "--unresolved-curations",
        type=Path,
        default=default_unresolved_curation_path(ROOT),
        help="Path to raw/manual/property_governance_20260422_unresolved_curations.csv.",
    )
    args = parser.parse_args()

    result = integrate_property_governance_bundle(
        bundle_path=args.bundle,
        output_root=ROOT,
        seed_catalog=_read_required_csv(args.seed_catalog, "seed_catalog"),
        molecule_core=_read_required_parquet(args.molecule_core, "molecule_core"),
        alias_df=_read_required_parquet(args.molecule_alias, "molecule_alias"),
        parser_version=PARSER_VERSION,
        retrieved_at=now_iso(),
        unresolved_curation_path=args.unresolved_curations,
    )

    summary = {
        "bundle_present": result["bundle_present"],
        "generated_seed_rows": int(len(result["generated_seed_rows"])) if hasattr(result["generated_seed_rows"], "__len__") else 0,
        "generated_molecule_rows": int(len(result["generated_molecule_rows"])),
        "generated_alias_rows": int(len(result["generated_alias_rows"])),
        "legacy_property_rows": int(len(result["legacy_property_rows"])),
        "canonical_observation_rows": int(len(result["canonical_observation"])) if hasattr(result["canonical_observation"], "__len__") else 0,
        "canonical_recommended_rows": int(len(result["canonical_recommended"])) if hasattr(result["canonical_recommended"], "__len__") else 0,
        "canonical_recommended_strict_rows": int(len(result["canonical_recommended_strict"]))
        if hasattr(result["canonical_recommended_strict"], "__len__")
        else 0,
        "canonical_review_queue_rows": int(len(result["canonical_review_queue"]))
        if hasattr(result["canonical_review_queue"], "__len__")
        else 0,
        "crosswalk_rows": int(len(result["crosswalk"])) if hasattr(result["crosswalk"], "__len__") else 0,
        "unresolved_rows": int(len(result["unresolved"])) if hasattr(result["unresolved"], "__len__") else 0,
        "extension_manifest_rows": int(len(result["extension_manifest"])) if hasattr(result["extension_manifest"], "__len__") else 0,
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
