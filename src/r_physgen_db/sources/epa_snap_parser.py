"""Parse EPA SNAP substitute tables for refrigeration and air-conditioning end uses."""

from __future__ import annotations

import re
from io import StringIO
from typing import Any

import pandas as pd


class EPASNAPParser:
    def parse(self, html: str, *, end_use: str) -> pd.DataFrame:
        tables = pd.read_html(StringIO(html))
        if not tables:
            return pd.DataFrame(columns=self.columns())

        table = tables[0].copy()
        table.columns = [self._normalize_column(str(col)) for col in table.columns]
        expected = {"substitute", "trade_names", "retrofit_new", "odp", "gwp", "ashrae_safety", "listing_date", "listing_status"}
        if not expected.issubset(set(table.columns)):
            return pd.DataFrame(columns=self.columns())

        table["substitute"] = table["substitute"].map(_clean_text)
        table["trade_names"] = table["trade_names"].map(_clean_text)
        table["retrofit_new"] = table["retrofit_new"].map(_normalize_retrofit_new)
        table["odp"] = table["odp"].map(_to_float)
        table["gwp"] = table["gwp"].map(_to_float)
        table["ashrae_safety"] = table["ashrae_safety"].map(_clean_text)
        table["listing_date"] = table["listing_date"].map(_clean_text)
        table["listing_status"] = table["listing_status"].map(_clean_text)
        table["acceptability"] = table["listing_status"].map(_derive_acceptability)
        table["effective_date"] = table["listing_status"].map(_extract_effective_date)
        table["use_conditions"] = table["listing_status"].map(_extract_use_conditions)
        table["end_use"] = end_use
        return table[self.columns()].dropna(subset=["substitute"], how="all").drop_duplicates()

    @staticmethod
    def columns() -> list[str]:
        return [
            "end_use",
            "substitute",
            "trade_names",
            "retrofit_new",
            "odp",
            "gwp",
            "ashrae_safety",
            "listing_date",
            "listing_status",
            "acceptability",
            "effective_date",
            "use_conditions",
        ]

    @staticmethod
    def _normalize_column(value: str) -> str:
        lowered = " ".join(value.replace("\xa0", " ").split()).lower()
        if lowered.startswith("substitute"):
            return "substitute"
        if lowered.startswith("trade name"):
            return "trade_names"
        if lowered.startswith("retrofit"):
            return "retrofit_new"
        if lowered.startswith("odp"):
            return "odp"
        if lowered.startswith("gwp"):
            return "gwp"
        if "ashrae" in lowered:
            return "ashrae_safety"
        if lowered.startswith("snap listing date"):
            return "listing_date"
        if lowered.startswith("listing status"):
            return "listing_status"
        return lowered


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ").strip()
    if text.lower() == "nan":
        return ""
    return " ".join(text.split())


def _to_float(value: Any) -> float | None:
    text = _clean_text(value)
    if not text:
        return None
    text = text.replace(",", "")
    if text in {"-", "NA", "N/A"}:
        return None
    if text.startswith("<"):
        text = text[1:]
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    return float(match.group(0))


def _derive_acceptability(listing_status: str) -> str:
    lowered = listing_status.lower()
    if "subject to use conditions" in lowered:
        return "acceptable_subject_to_use_conditions"
    if "subject to narrowed use limits" in lowered:
        return "acceptable_subject_to_narrowed_use_limits"
    if "unacceptable" in lowered:
        return "unacceptable"
    if lowered.startswith("acceptable") or lowered == "acceptable":
        return "acceptable"
    if not lowered:
        return "pending"
    return "other"


def _extract_effective_date(listing_status: str) -> str:
    match = re.search(r"as of ([A-Za-z]+ \d{1,2}, \d{4})", listing_status)
    return match.group(1) if match else ""


def _extract_use_conditions(listing_status: str) -> str:
    lowered = listing_status.lower()
    if "subject to use conditions" in lowered or "subject to narrowed use limits" in lowered:
        return listing_status
    return ""


def _normalize_retrofit_new(value: Any) -> str:
    text = _clean_text(value).upper()
    return text if text in {"R", "N", "R/N"} else "unknown"
