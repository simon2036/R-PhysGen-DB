"""Artifact and stage result structures for staged dataset builds."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from r_physgen_db.utils import sha256_file


@dataclass(slots=True)
class ArtifactRef:
    name: str
    path: Path | None = None
    kind: str = "logical"
    exists: bool = True
    row_count: int | None = None
    checksum_sha256: str = ""
    notes: str = ""

    def refresh(self) -> "ArtifactRef":
        if self.path is not None and self.kind in {"file", "table"}:
            self.exists = self.path.exists()
            if self.exists and self.path.is_file():
                self.checksum_sha256 = sha256_file(self.path)
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": "" if self.path is None else str(self.path),
            "kind": self.kind,
            "exists": self.exists,
            "row_count": self.row_count,
            "checksum_sha256": self.checksum_sha256,
            "notes": self.notes,
        }


@dataclass(slots=True)
class StageResult:
    stage_id: str
    status: str
    outputs: list[ArtifactRef] = field(default_factory=list)
    row_count_summary: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    notes: str = ""
    error_message: str = ""
    exception_traceback: str = ""


def artifacts_json(refs: list[ArtifactRef]) -> str:
    return json.dumps([ref.to_dict() for ref in refs], ensure_ascii=False)

