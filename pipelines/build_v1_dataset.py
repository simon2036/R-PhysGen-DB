"""Build R-PhysGen-DB V1 outputs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from r_physgen_db.pipeline import build_dataset  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Build R-PhysGen-DB V1 dataset.")
    parser.add_argument("--refresh-remote", action="store_true", help="Reserved flag for future refresh semantics.")
    args = parser.parse_args()

    report = build_dataset(refresh_remote=args.refresh_remote)
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
