"""Optional CompTox client with API-key detection only for Wave 2."""

from __future__ import annotations

import os
from typing import Any

from r_physgen_db.constants import COMPTOX_ENV_VAR_NAMES


class CompToxClient:
    def __init__(self) -> None:
        self.env_var_name = next((name for name in COMPTOX_ENV_VAR_NAMES if os.getenv(name)), "")
        self.api_key = os.getenv(self.env_var_name) if self.env_var_name else ""

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def pending_record(self, *, seed_id: str, r_number: str, mol_id: str = "", detail: str = "") -> dict[str, Any]:
        return {
            "seed_id": seed_id,
            "r_number": r_number,
            "mol_id": mol_id,
            "requested_source": "EPA CompTox Chemical Details Resource",
            "status": "pending_api_key",
            "detail": detail or "CompTox API key not configured; skipped without blocking build.",
            "required_env_var": self.env_var_name or "COMPTOX_API_KEY",
        }
