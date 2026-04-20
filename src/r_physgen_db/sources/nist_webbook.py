"""NIST Chemistry WebBook snapshot adapter."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote

from r_physgen_db.sources.http_utils import build_retry_session


class NISTWebBookClient:
    base_url = "https://webbook.nist.gov/cgi/cbook.cgi"

    def __init__(self, timeout: int = 30) -> None:
        self.timeout = timeout
        self.session = build_retry_session()

    def snapshot_url(self, query: str, query_type: str = "name") -> str:
        param_name = "ID" if query_type == "id" else "Name"
        return f"{self.base_url}?{param_name}={quote(query)}&Units=SI&Mask=4#Thermo-Phase"

    def fetch_snapshot(self, query: str, query_type: str = "name") -> dict[str, Any]:
        url = self.snapshot_url(query, query_type)
        response = self.session.get(url, timeout=self.timeout)
        if response.status_code >= 400:
            response.raise_for_status()
        title_match = re.search(r"<title>(.*?)</title>", response.text, flags=re.IGNORECASE | re.DOTALL)
        title = title_match.group(1).strip() if title_match else ""
        status = "ok"
        if "Query Error" in title:
            status = "failed"
        return {
            "url": response.url,
            "status_code": response.status_code,
            "title": title,
            "status": status,
            "html": response.text,
        }
