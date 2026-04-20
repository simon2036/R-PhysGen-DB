"""Validate R-PhysGen-DB V1 outputs."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from r_physgen_db.validate import validate_dataset  # noqa: E402


def main() -> None:
    report = validate_dataset()
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if report["errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
