"""Parse phase-change thermodynamic data from NIST WebBook HTML snapshots."""

from __future__ import annotations

from io import StringIO
from typing import Any

import pandas as pd


class NISTThermoParser:
    """Extract a compact set of thermodynamic labels from NIST phase tables."""

    def parse(self, html: str) -> list[dict[str, Any]]:
        tables = pd.read_html(StringIO(html))
        if not tables:
            return []

        quantity_table = self._find_quantity_table(tables)
        hvap_table = self._find_hvap_table(tables)

        observations: list[dict[str, Any]] = []
        boiling_point_k: float | None = None

        if quantity_table is not None:
            normalized = quantity_table.copy()
            normalized.columns = [str(col) for col in normalized.columns]
            for row in normalized.to_dict(orient="records"):
                quantity = str(row.get("Quantity", "")).strip()
                value = _to_float(row.get("Value"))
                unit = str(row.get("Units", "")).strip()
                reference = str(row.get("Reference", "")).strip()
                comment = str(row.get("Comment", "")).strip()
                if quantity == "Tboil" and value is not None:
                    boiling_point_k = value if boiling_point_k is None else min(boiling_point_k, value)
                    converted = _convert_temperature_to_c(value, unit)
                    if converted is not None:
                        observations.append(
                            _make_obs(
                                property_name="boiling_point_c",
                                value_num=converted,
                                unit="degC",
                                method="NIST phase change data (Tboil)",
                                pressure="0.101325 MPa",
                                phase="vapor-liquid_equilibrium",
                                notes=_combine_notes(reference, comment),
                            )
                        )
                elif quantity in {"Tc", "T_c"} and value is not None:
                    converted = _convert_temperature_to_c(value, unit)
                    if converted is not None:
                        observations.append(
                            _make_obs(
                                property_name="critical_temp_c",
                                value_num=converted,
                                unit="degC",
                                method="NIST phase change data (Tc)",
                                notes=_combine_notes(reference, comment),
                            )
                        )
                elif quantity in {"Pc", "P_c"} and value is not None:
                    converted = _convert_pressure_to_mpa(value, unit)
                    if converted is not None:
                        observations.append(
                            _make_obs(
                                property_name="critical_pressure_mpa",
                                value_num=converted,
                                unit="MPa",
                                method="NIST phase change data (Pc)",
                                notes=_combine_notes(reference, comment),
                            )
                        )
                elif quantity in {"ρc", "rhoc", "rho_c"} and value is not None:
                    converted = _convert_density_to_kgm3(value, unit)
                    if converted is not None:
                        observations.append(
                            _make_obs(
                                property_name="critical_density_kgm3",
                                value_num=converted,
                                unit="kg/m3",
                                method="NIST phase change data (critical density)",
                                notes=_combine_notes(reference, comment),
                            )
                        )

        if hvap_table is not None:
            normalized = hvap_table.copy()
            normalized.columns = [str(col) for col in normalized.columns]
            hvap_column = next((col for col in normalized.columns if "vap" in col.lower()), None)
            temp_column = next((col for col in normalized.columns if "temperature" in col.lower()), None)
            if hvap_column is not None:
                candidate_rows = normalized.copy()
                candidate_rows["_hvap"] = candidate_rows[hvap_column].map(_to_float)
                if temp_column is not None:
                    candidate_rows["_temp_k"] = candidate_rows[temp_column].map(_to_float)
                else:
                    candidate_rows["_temp_k"] = None
                candidate_rows = candidate_rows.loc[candidate_rows["_hvap"].notna()]
                if not candidate_rows.empty:
                    if boiling_point_k is not None and candidate_rows["_temp_k"].notna().any():
                        candidate_rows["_distance"] = (candidate_rows["_temp_k"] - boiling_point_k).abs()
                        selected = candidate_rows.sort_values("_distance", kind="stable").iloc[0]
                    else:
                        selected = candidate_rows.iloc[0]
                    notes = _combine_notes(str(selected.get("Reference", "")), str(selected.get("Comment", "")))
                    temperature = ""
                    if pd.notna(selected.get("_temp_k")):
                        temperature = f"{float(selected['_temp_k']):.6g} K"
                    observations.append(
                        _make_obs(
                            property_name="vaporization_enthalpy_kjmol",
                            value_num=float(selected["_hvap"]),
                            unit="kJ/mol",
                            method="NIST phase change data (enthalpy of vaporization)",
                            temperature=temperature,
                            phase="vapor-liquid_equilibrium",
                            notes=notes,
                        )
                    )

        deduped: list[dict[str, Any]] = []
        seen: set[tuple[str, float]] = set()
        for item in observations:
            key = (item["property_name"], round(item["value_num"], 8))
            if key not in seen:
                seen.add(key)
                deduped.append(item)
        return deduped

    @staticmethod
    def _find_quantity_table(tables: list[pd.DataFrame]) -> pd.DataFrame | None:
        for table in tables:
            cols = {str(col).strip() for col in table.columns}
            if {"Quantity", "Value", "Units"}.issubset(cols):
                return table
        return None

    @staticmethod
    def _find_hvap_table(tables: list[pd.DataFrame]) -> pd.DataFrame | None:
        for table in tables:
            cols = [str(col) for col in table.columns]
            if any("vap" in col.lower() for col in cols):
                return table
        return None


def _make_obs(
    *,
    property_name: str,
    value_num: float,
    unit: str,
    method: str,
    temperature: str = "",
    pressure: str = "",
    phase: str = "",
    notes: str = "",
) -> dict[str, Any]:
    return {
        "property_name": property_name,
        "value_num": float(value_num),
        "value": f"{float(value_num):.8g}",
        "unit": unit,
        "temperature": temperature,
        "pressure": pressure,
        "phase": phase,
        "method": method,
        "notes": notes,
    }


def _combine_notes(reference: str, comment: str) -> str:
    parts = [part for part in [reference.strip(), comment.strip()] if part]
    return " | ".join(parts)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _convert_temperature_to_c(value: float, unit: str) -> float | None:
    label = unit.strip().lower()
    if label == "k":
        return value - 273.15
    if label in {"degc", "c", "°c"}:
        return value
    return None


def _convert_pressure_to_mpa(value: float, unit: str) -> float | None:
    label = unit.strip().lower()
    if label == "mpa":
        return value
    if label == "pa":
        return value / 1e6
    if label == "kpa":
        return value / 1e3
    if label == "bar":
        return value * 0.1
    return None


def _convert_density_to_kgm3(value: float, unit: str) -> float | None:
    label = unit.strip().lower()
    if label == "kg/m3":
        return value
    if label in {"g/cm3", "g/ml"}:
        return value * 1000.0
    return None
