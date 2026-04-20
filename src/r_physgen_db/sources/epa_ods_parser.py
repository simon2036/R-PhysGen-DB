"""Parse EPA ozone-depleting substances tables."""

from __future__ import annotations

from io import StringIO
from typing import Any

import pandas as pd


class EPAODSParser:
    def parse(self, html: str) -> pd.DataFrame:
        tables = pd.read_html(StringIO(html))
        if not tables:
            return pd.DataFrame(columns=self.columns())

        frames = []
        for table in tables:
            cols = {str(col).strip() for col in table.columns}
            if {"Chemical Name", "CAS Number"}.issubset(cols):
                frames.append(table.copy())

        if not frames:
            return pd.DataFrame(columns=self.columns())

        merged = pd.concat(frames, ignore_index=True)
        merged.columns = [str(col).strip() for col in merged.columns]
        renamed = merged.rename(
            columns={
                "Chemical Name": "chemical_name",
                "Lifetime, in years": "atmospheric_lifetime_yr",
                "ODP1 (Montreal Protocol)": "odp_montreal_protocol",
                "ODP2 (WMO 2011)": "odp_wmo_2011",
                "GWP1 (AR4)": "gwp_ar4_100yr",
                "GWP2 (AR5)": "gwp_ar5_100yr",
                "CAS Number": "cas_number",
            }
        )
        for column in ["atmospheric_lifetime_yr", "odp_montreal_protocol", "odp_wmo_2011", "gwp_ar4_100yr", "gwp_ar5_100yr"]:
            renamed[column] = renamed[column].map(_to_float)
        renamed["chemical_name"] = renamed["chemical_name"].astype(str).str.strip()
        renamed["cas_number"] = renamed["cas_number"].astype(str).str.strip()
        return renamed[self.columns()].dropna(subset=["chemical_name"], how="all").drop_duplicates()

    @staticmethod
    def columns() -> list[str]:
        return [
            "chemical_name",
            "cas_number",
            "atmospheric_lifetime_yr",
            "odp_montreal_protocol",
            "odp_wmo_2011",
            "gwp_ar4_100yr",
            "gwp_ar5_100yr",
        ]


def _to_float(value: Any) -> float | None:
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    text = text.replace(",", "").replace("~", "")
    if text in {"-", "NA", "N/A"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None
