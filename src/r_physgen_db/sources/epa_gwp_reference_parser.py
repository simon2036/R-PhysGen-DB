"""Parse EPA Technology Transitions GWP reference tables."""

from __future__ import annotations

import re
from io import StringIO
from typing import Any

import pandas as pd


class EPATechnologyTransitionsGWPParser:
    def parse(self, html: str) -> pd.DataFrame:
        tables = pd.read_html(StringIO(html))
        if not tables:
            return pd.DataFrame(columns=self.columns())

        target = None
        for table in tables:
            normalized = {self._normalize_column(str(col)) for col in table.columns}
            if {"substance_name", "gwp_text", "reference"}.issubset(normalized):
                target = table.copy()
                break

        if target is None:
            return pd.DataFrame(columns=self.columns())

        target.columns = [self._normalize_column(str(col)) for col in target.columns]
        target["substance_name"] = target["substance_name"].map(_clean_text)
        target["reference"] = target["reference"].map(_clean_text)
        target["gwp_text"] = target["gwp_text"].map(_clean_text)
        target["gwp_100yr"] = target["gwp_text"].map(_to_scalar_float)
        target["is_range"] = target["gwp_text"].map(_looks_like_range)
        target[["gwp_range_min", "gwp_range_max"]] = target["gwp_text"].apply(_to_range_bounds).apply(pd.Series)
        return target[self.columns()].dropna(subset=["substance_name"], how="all").drop_duplicates()

    @staticmethod
    def columns() -> list[str]:
        return [
            "substance_name",
            "gwp_text",
            "gwp_100yr",
            "reference",
            "is_range",
            "gwp_range_min",
            "gwp_range_max",
        ]

    @staticmethod
    def _normalize_column(value: str) -> str:
        lowered = " ".join(value.replace("\xa0", " ").split()).lower()
        if lowered.startswith("substance name"):
            return "substance_name"
        if "100-year" in lowered and "warming potential" in lowered:
            return "gwp_text"
        if lowered.startswith("reference"):
            return "reference"
        return lowered


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ").strip()
    if text.lower() == "nan":
        return ""
    return " ".join(text.split())


def _looks_like_range(value: str) -> bool:
    text = _clean_text(value).replace(",", "")
    if not text:
        return False
    return len(re.findall(r"\d+(?:\.\d+)?", text)) > 1


def _to_range_bounds(value: Any) -> tuple[float | None, float | None]:
    text = _clean_text(value).replace(",", "")
    if not text or not _looks_like_range(text):
        return (None, None)
    numbers = [float(number) for number in re.findall(r"\d+(?:\.\d+)?", text)]
    if len(numbers) < 2:
        return (None, None)
    return (min(numbers), max(numbers))


def _to_scalar_float(value: Any) -> float | None:
    text = _clean_text(value).replace(",", "")
    if not text or _looks_like_range(text):
        return None
    if text in {"-", "NA", "N/A"}:
        return None
    if text.startswith("<"):
        text = text[1:].strip()
    match = re.fullmatch(r"-?\d+(?:\.\d+)?", text)
    if match:
        return float(match.group(0))
    return None
