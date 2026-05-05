"""Production mixture table helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


MIXTURE_CORE_SOURCE_ID = "source_property_governance_mixture_core"
MIXTURE_COMPONENT_SOURCE_ID = "source_property_governance_mixture_component"
MIXTURE_CORE_SOURCE_NAME = "Property Governance Normalized Mixture Core"
MIXTURE_COMPONENT_SOURCE_NAME = "Property Governance Normalized Mixture Component"
MIXTURE_COMPONENT_CURATION_SOURCE_ID = "source_manual_mixture_component_curations"
MIXTURE_COMPONENT_CURATION_SOURCE_NAME = "Manual Mixture Component Curations"
MIXTURE_FRACTION_CURATION_SOURCE_ID = "source_manual_mixture_fraction_curations"
MIXTURE_FRACTION_CURATION_SOURCE_NAME = "Manual Mixture Fraction Curations"

MIXTURE_CORE_COLUMNS = [
    "mixture_id",
    "mixture_name",
    "ashrae_blend_designation",
    "source_id",
    "source_name",
    "notes",
]

MIXTURE_COMPOSITION_COLUMNS = [
    "mixture_id",
    "mixture_name",
    "component_mol_id",
    "component_role",
    "composition_basis",
    "fraction_value",
    "source_id",
    "source_name",
    "notes",
]

MIXTURE_COMPOSITION_BASIS = {"mass_fraction", "mole_fraction", "volume_fraction"}
MIXTURE_FORBIDDEN_WIDE_COLUMNS = {"mixture_id", "mixture_name", "component_mol_id"}
FRACTION_SUM_TOLERANCE = 1e-6


@dataclass(slots=True)
class MixtureBuild:
    mixture_core: pd.DataFrame
    mixture_composition: pd.DataFrame
    summary: dict[str, Any]


def build_mixture_tables(
    extension_mixture_core: pd.DataFrame | None,
    extension_mixture_component: pd.DataFrame | None,
    molecule_core: pd.DataFrame | None = None,
    *,
    component_curations: pd.DataFrame | None = None,
    fraction_curations: pd.DataFrame | None = None,
) -> MixtureBuild:
    """Promote governance mixture extension tables into production silver tables."""

    if extension_mixture_core is None or extension_mixture_component is None:
        return _empty_build()
    if extension_mixture_core.empty and extension_mixture_component.empty:
        return _empty_build()

    core = extension_mixture_core.copy().fillna("")
    components = extension_mixture_component.copy()
    core_out = _build_core(core)
    composition_out = _build_composition(components, core_out)
    composition_out = apply_mixture_component_curations(composition_out, component_curations)
    composition_out = apply_mixture_fraction_curations(composition_out, fraction_curations)
    summary = mixture_summary(composition_out, core_out, molecule_core)
    return MixtureBuild(
        mixture_core=core_out,
        mixture_composition=composition_out,
        summary=summary,
    )


def load_mixture_fraction_curations(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=_mixture_fraction_curation_columns())
    return pd.read_csv(path).fillna("")


def load_mixture_component_curations(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=_mixture_component_curation_columns())
    return pd.read_csv(path).fillna("")


def apply_mixture_component_curations(
    mixture_composition: pd.DataFrame,
    component_curations: pd.DataFrame | None,
) -> pd.DataFrame:
    """Apply traceable manual mixture component identity replacements."""

    out = _ensure_columns(mixture_composition.copy(), MIXTURE_COMPOSITION_COLUMNS)
    if component_curations is None or component_curations.empty:
        return out

    curations = _ensure_columns(component_curations.copy().fillna(""), _mixture_component_curation_columns())
    for row in curations.to_dict(orient="records"):
        mixture_id = _clean(row.get("mixture_id"))
        current_component_mol_id = _clean(row.get("current_component_mol_id"))
        replacement_component_mol_id = _clean(row.get("replacement_component_mol_id"))
        composition_basis = _clean(row.get("composition_basis"))
        source_id = _clean(row.get("source_id")) or MIXTURE_COMPONENT_CURATION_SOURCE_ID
        source_name = _clean(row.get("source_name"))
        source_url = _clean(row.get("source_url"))
        if not mixture_id or not current_component_mol_id or not replacement_component_mol_id:
            raise ValueError(
                "Mixture component curation rows require mixture_id, current_component_mol_id, "
                "and replacement_component_mol_id"
            )
        if not source_id or not source_name:
            raise ValueError(
                f"Mixture component curation requires source_id and source_name: "
                f"{mixture_id}/{current_component_mol_id}"
            )

        mask = (
            out["mixture_id"].fillna("").astype(str).eq(mixture_id)
            & out["component_mol_id"].fillna("").astype(str).eq(current_component_mol_id)
        )
        if composition_basis:
            mask &= out["composition_basis"].fillna("").astype(str).eq(composition_basis)
        if not bool(mask.any()):
            basis_suffix = f"/{composition_basis}" if composition_basis else ""
            raise ValueError(
                f"Mixture component curation target not found: "
                f"{mixture_id}/{current_component_mol_id}{basis_suffix}"
            )

        duplicate_mask = (
            out["mixture_id"].fillna("").astype(str).eq(mixture_id)
            & out["component_mol_id"].fillna("").astype(str).eq(replacement_component_mol_id)
            & out["composition_basis"].fillna("").astype(str).isin(
                out.loc[mask, "composition_basis"].fillna("").astype(str).tolist()
            )
            & ~mask
        )
        if bool(duplicate_mask.any()):
            raise ValueError(
                f"Mixture component curation would create duplicate target rows: "
                f"{mixture_id}/{replacement_component_mol_id}"
            )

        for index in out.index[mask]:
            note = _join_notes(
                _clean(out.at[index, "notes"]),
                "component_curated_from_manual_source",
                f"replaced_component_mol_id={current_component_mol_id}",
                f"source_url={source_url}" if source_url else "",
                _clean(row.get("notes")),
            )
            out.at[index, "component_mol_id"] = replacement_component_mol_id
            out.at[index, "source_id"] = source_id
            out.at[index, "source_name"] = source_name
            out.at[index, "notes"] = note
    return _ensure_columns(out, MIXTURE_COMPOSITION_COLUMNS)


def apply_mixture_fraction_curations(
    mixture_composition: pd.DataFrame,
    fraction_curations: pd.DataFrame | None,
) -> pd.DataFrame:
    """Apply traceable manual mixture fraction values without imputing missing rows."""

    out = _ensure_columns(mixture_composition.copy(), MIXTURE_COMPOSITION_COLUMNS)
    if fraction_curations is None or fraction_curations.empty:
        return out

    curations = _ensure_columns(fraction_curations.copy().fillna(""), _mixture_fraction_curation_columns())
    for row in curations.to_dict(orient="records"):
        mixture_id = _clean(row.get("mixture_id"))
        component_mol_id = _clean(row.get("component_mol_id"))
        composition_basis = _clean(row.get("composition_basis"))
        source_id = _clean(row.get("source_id")) or MIXTURE_FRACTION_CURATION_SOURCE_ID
        source_name = _clean(row.get("source_name"))
        source_url = _clean(row.get("source_url"))
        numeric = pd.to_numeric(pd.Series([row.get("fraction_value")]), errors="coerce").iloc[0]
        if not mixture_id or not component_mol_id or not composition_basis:
            raise ValueError("Mixture fraction curation rows require mixture_id, component_mol_id, and composition_basis")
        if not source_id or not source_name:
            raise ValueError(f"Mixture fraction curation requires source_id and source_name: {mixture_id}/{component_mol_id}")
        if pd.isna(numeric) or not (0.0 <= float(numeric) <= 1.0):
            raise ValueError(f"Mixture fraction curation fraction_value must be in [0, 1]: {mixture_id}/{component_mol_id}")
        mask = (
            out["mixture_id"].fillna("").astype(str).eq(mixture_id)
            & out["component_mol_id"].fillna("").astype(str).eq(component_mol_id)
            & out["composition_basis"].fillna("").astype(str).eq(composition_basis)
        )
        if not bool(mask.any()):
            raise ValueError(f"Mixture fraction curation target not found: {mixture_id}/{component_mol_id}/{composition_basis}")
        note = _join_notes(
            out.loc[mask, "notes"].fillna("").astype(str).iloc[0],
            "fraction_curated_from_manual_source",
            f"source_url={source_url}" if source_url else "",
            _clean(row.get("notes")),
        )
        out.loc[mask, "fraction_value"] = float(numeric)
        out.loc[mask, "source_id"] = source_id
        out.loc[mask, "source_name"] = source_name
        out.loc[mask, "notes"] = note
    return _ensure_columns(out, MIXTURE_COMPOSITION_COLUMNS)


def mixture_summary(
    mixture_composition: pd.DataFrame,
    mixture_core: pd.DataFrame | None = None,
    molecule_core: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Return quality-report counters for production mixture tables."""

    mixture_core = mixture_core if mixture_core is not None else pd.DataFrame(columns=MIXTURE_CORE_COLUMNS)
    mixture_composition = _ensure_columns(mixture_composition.copy(), MIXTURE_COMPOSITION_COLUMNS)

    basis_counts = (
        mixture_composition["composition_basis"].fillna("").astype(str).value_counts().sort_index().to_dict()
        if not mixture_composition.empty
        else {}
    )
    dangling_count = 0
    if molecule_core is not None and not molecule_core.empty and "mol_id" in molecule_core.columns and not mixture_composition.empty:
        known = set(molecule_core["mol_id"].fillna("").astype(str).tolist())
        observed = set(mixture_composition["component_mol_id"].fillna("").astype(str).tolist())
        observed.discard("")
        dangling_count = len(observed - known)

    fraction_audit = fraction_sum_audit(mixture_composition)
    return {
        "mixture_count": int(len(mixture_core)),
        "component_count": int(len(mixture_composition)),
        "composition_basis_counts": basis_counts,
        "dangling_component_count": int(dangling_count),
        "fraction_sum_error_count": int(len(fraction_audit["error_groups"])),
        "fraction_sum_unresolved_count": int(len(fraction_audit["unresolved_groups"])),
        "fraction_sum_max_abs_error": float(fraction_audit["max_abs_error"]),
    }


def fraction_sum_audit(mixture_composition: pd.DataFrame, *, tolerance: float = FRACTION_SUM_TOLERANCE) -> dict[str, Any]:
    """Audit composition sums, separating missing source fractions from numeric errors."""

    if mixture_composition.empty:
        return {"error_groups": [], "unresolved_groups": [], "max_abs_error": 0.0}
    frame = mixture_composition.copy()
    frame["fraction_value_num"] = pd.to_numeric(frame["fraction_value"], errors="coerce")
    error_groups: list[dict[str, Any]] = []
    unresolved_groups: list[dict[str, Any]] = []
    max_abs_error = 0.0
    for (mixture_id, basis), group in frame.groupby(["mixture_id", "composition_basis"], dropna=False):
        if group["fraction_value_num"].isna().any():
            unresolved_groups.append(
                {
                    "mixture_id": _clean(mixture_id),
                    "composition_basis": _clean(basis),
                    "missing_fraction_count": int(group["fraction_value_num"].isna().sum()),
                }
            )
            continue
        total = float(group["fraction_value_num"].sum())
        abs_error = abs(total - 1.0)
        max_abs_error = max(max_abs_error, abs_error)
        if abs_error > tolerance:
            error_groups.append(
                {
                    "mixture_id": _clean(mixture_id),
                    "composition_basis": _clean(basis),
                    "fraction_sum": total,
                    "abs_error": abs_error,
                }
            )
    return {"error_groups": error_groups, "unresolved_groups": unresolved_groups, "max_abs_error": max_abs_error}


def _build_core(core: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for row in core.to_dict(orient="records"):
        rows.append(
            {
                "mixture_id": _clean(row.get("mixture_id")),
                "mixture_name": _clean(row.get("mixture_name")),
                "ashrae_blend_designation": _clean(row.get("ashrae_blend_designation")) or _clean(row.get("mixture_name")),
                "source_id": MIXTURE_CORE_SOURCE_ID,
                "source_name": MIXTURE_CORE_SOURCE_NAME,
                "notes": _clean(row.get("notes")),
            }
        )
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.drop_duplicates(subset=["mixture_id"], keep="first").sort_values("mixture_id").reset_index(drop=True)
    return _ensure_columns(out, MIXTURE_CORE_COLUMNS)


def _build_composition(components: pd.DataFrame, mixture_core: pd.DataFrame) -> pd.DataFrame:
    if components.empty:
        return pd.DataFrame(columns=MIXTURE_COMPOSITION_COLUMNS)
    name_lookup = (
        mixture_core.set_index("mixture_id")["mixture_name"].fillna("").astype(str).to_dict()
        if not mixture_core.empty
        else {}
    )
    rows = []
    for row in components.to_dict(orient="records"):
        mixture_id = _clean(row.get("mixture_id"))
        raw_basis = _clean(row.get("composition_basis"))
        basis, fraction_value, note = _normalize_basis_and_fraction(raw_basis, row.get("fraction_value"))
        rows.append(
            {
                "mixture_id": mixture_id,
                "mixture_name": name_lookup.get(mixture_id, ""),
                "component_mol_id": _clean(row.get("mol_id")) or _clean(row.get("component_mol_id")),
                "component_role": "component",
                "composition_basis": basis,
                "fraction_value": fraction_value,
                "source_id": MIXTURE_COMPONENT_SOURCE_ID,
                "source_name": MIXTURE_COMPONENT_SOURCE_NAME,
                "notes": note,
            }
        )
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values(["mixture_id", "component_mol_id", "composition_basis"]).reset_index(drop=True)
    return _ensure_columns(out, MIXTURE_COMPOSITION_COLUMNS)


def _normalize_basis_and_fraction(raw_basis: str, raw_fraction: Any) -> tuple[str, float | None, str]:
    numeric = pd.to_numeric(pd.Series([raw_fraction]), errors="coerce").iloc[0]
    fraction = None if pd.isna(numeric) else float(numeric)
    basis = raw_basis
    note = ""
    if raw_basis == "mass_pct":
        basis = "mass_fraction"
        fraction = None if fraction is None else fraction / 100.0
        note = "normalized_from_mass_pct"
    if basis not in MIXTURE_COMPOSITION_BASIS:
        note = "; ".join(part for part in [note, f"unrecognized_basis={raw_basis}"] if part)
    if fraction is None:
        note = "; ".join(part for part in [note, "source_fraction_missing"] if part)
    return basis, fraction, note


def _empty_build() -> MixtureBuild:
    core = pd.DataFrame(columns=MIXTURE_CORE_COLUMNS)
    composition = pd.DataFrame(columns=MIXTURE_COMPOSITION_COLUMNS)
    return MixtureBuild(mixture_core=core, mixture_composition=composition, summary=mixture_summary(composition, core))


def _mixture_fraction_curation_columns() -> list[str]:
    return [
        "mixture_id",
        "component_mol_id",
        "composition_basis",
        "fraction_value",
        "source_id",
        "source_name",
        "source_url",
        "notes",
    ]


def _mixture_component_curation_columns() -> list[str]:
    return [
        "mixture_id",
        "current_component_mol_id",
        "replacement_component_mol_id",
        "composition_basis",
        "source_id",
        "source_name",
        "source_url",
        "notes",
    ]


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in df.columns:
            df[column] = None
    return df[columns]


def _join_notes(*parts: str) -> str:
    return "; ".join(part for part in parts if part)


def _clean(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()
