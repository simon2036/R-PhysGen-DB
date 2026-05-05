"""Coverage matrix and enrichment worklist helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


DEFAULT_PROMOTED_PROPERTIES = [
    "boiling_point_c",
    "critical_temp_c",
    "critical_pressure_mpa",
    "gwp_100yr",
    "odp",
    "ashrae_safety",
    "toxicity_class",
    "viscosity_liquid_pas",
    "thermal_conductivity_liquid_wmk",
    "cop_standard_cycle",
]

DEFAULT_CANONICAL_FEATURE_KEYS = {
    "boiling_point_c": "thermodynamic.normal_boiling_temperature",
    "critical_temp_c": "thermodynamic.critical_temperature",
    "critical_pressure_mpa": "thermodynamic.critical_pressure",
    "gwp_100yr": "environmental.gwp_100yr",
    "odp": "environmental.odp",
    "ashrae_safety": "safety.safety_group",
    "toxicity_class": "safety.toxicity_class",
    "viscosity_liquid_pas": "transport.liquid_viscosity",
    "thermal_conductivity_liquid_wmk": "transport.liquid_thermal_conductivity",
    "cop_standard_cycle": "cycle.cop",
}


def build_promoted_coverage_outputs(
    seed_catalog: pd.DataFrame,
    molecule_core: pd.DataFrame,
    property_recommended: pd.DataFrame,
    *,
    required_properties: Iterable[str] = DEFAULT_PROMOTED_PROPERTIES,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build promoted A/B/C coverage matrix and missing-property worklist."""

    properties = list(required_properties)
    promoted = _promoted_entities(seed_catalog, molecule_core)
    available = (
        property_recommended.groupby("mol_id")["property_name"].apply(set).to_dict()
        if property_recommended is not None and not property_recommended.empty
        else {}
    )

    coverage_rows: list[dict[str, object]] = []
    worklist_rows: list[dict[str, object]] = []
    for tier in ["A", "B", "C"]:
        tier_frame = promoted.loc[promoted["coverage_tier"].astype(str).eq(tier)].copy()
        mol_ids = tier_frame["mol_id"].astype(str).tolist()
        for property_name in properties:
            present = [mol_id for mol_id in mol_ids if property_name in available.get(mol_id, set())]
            missing = tier_frame.loc[~tier_frame["mol_id"].astype(str).isin(present)]
            coverage_rows.append(
                {
                    "coverage_tier": tier,
                    "property_name": property_name,
                    "molecule_count": int(len(mol_ids)),
                    "present_count": int(len(present)),
                    "missing_count": int(len(missing)),
                    "coverage_fraction": float(len(present) / len(mol_ids)) if mol_ids else 0.0,
                }
            )
            for record in missing.to_dict(orient="records"):
                canonical_feature_key = DEFAULT_CANONICAL_FEATURE_KEYS.get(property_name, property_name)
                worklist_rows.append(
                    {
                        "mol_id": record["mol_id"],
                        "seed_id": record.get("seed_id", ""),
                        "r_number": record.get("r_number", ""),
                        "canonical_smiles": record.get("canonical_smiles", ""),
                        "coverage_tier": tier,
                        "entity_scope": record.get("entity_scope", ""),
                        "property_name": property_name,
                        "canonical_feature_key": canonical_feature_key,
                        "missing_feature_key": canonical_feature_key,
                        "recommended_action": _recommended_action(property_name),
                        "priority": _priority(tier, property_name),
                    }
                )

    coverage = pd.DataFrame(
        coverage_rows,
        columns=[
            "coverage_tier",
            "property_name",
            "molecule_count",
            "present_count",
            "missing_count",
            "coverage_fraction",
        ],
    )
    worklist = pd.DataFrame(
        worklist_rows,
        columns=[
            "mol_id",
            "seed_id",
            "r_number",
            "canonical_smiles",
            "coverage_tier",
            "entity_scope",
            "property_name",
            "canonical_feature_key",
            "missing_feature_key",
            "recommended_action",
            "priority",
        ],
    )
    if not worklist.empty:
        worklist = worklist.sort_values(["priority", "coverage_tier", "property_name", "mol_id"], kind="stable").reset_index(drop=True)
    return coverage, worklist


def write_promoted_coverage_outputs(
    *,
    seed_catalog: pd.DataFrame,
    molecule_core: pd.DataFrame,
    property_recommended: pd.DataFrame,
    coverage_path: Path,
    worklist_path: Path,
) -> dict[str, int]:
    coverage, worklist = build_promoted_coverage_outputs(seed_catalog, molecule_core, property_recommended)
    coverage_path.parent.mkdir(parents=True, exist_ok=True)
    worklist_path.parent.mkdir(parents=True, exist_ok=True)
    coverage.to_csv(coverage_path, index=False)
    worklist.to_csv(worklist_path, index=False)
    return {
        "coverage_matrix_rows": int(len(coverage)),
        "worklist_rows": int(len(worklist)),
    }


def _promoted_entities(seed_catalog: pd.DataFrame, molecule_core: pd.DataFrame) -> pd.DataFrame:
    if seed_catalog.empty or molecule_core.empty:
        return pd.DataFrame(
            columns=[
                "seed_id",
                "mol_id",
                "r_number",
                "canonical_smiles",
                "coverage_tier",
                "model_inclusion",
                "entity_scope",
            ]
        )
    seed_catalog = seed_catalog.copy()
    molecule_core = molecule_core.copy()
    seed_columns = ["seed_id", "r_number", "coverage_tier", "model_inclusion", "entity_scope"]
    for column in seed_columns:
        if column not in seed_catalog.columns:
            seed_catalog[column] = ""
    molecule_columns = ["seed_id", "mol_id", "canonical_smiles"]
    for column in molecule_columns:
        if column not in molecule_core.columns:
            molecule_core[column] = ""
    merged = molecule_core[molecule_columns].merge(seed_catalog[seed_columns], on="seed_id", how="left")
    return merged.loc[
        merged["coverage_tier"].astype(str).isin({"A", "B", "C"})
        & merged["model_inclusion"].astype(str).eq("yes")
    ].copy()


def _recommended_action(property_name: str) -> str:
    if property_name in {"gwp_100yr", "odp", "ashrae_safety", "toxicity_class"}:
        return "literature_search"
    if property_name in {"viscosity_liquid_pas", "thermal_conductivity_liquid_wmk", "cop_standard_cycle"}:
        return "run_cycle"
    return "manual_curation"


def _priority(tier: str, property_name: str) -> int:
    tier_rank = {"A": 1, "B": 2, "C": 3}.get(tier, 9)
    property_rank = {
        "boiling_point_c": 1,
        "critical_temp_c": 2,
        "critical_pressure_mpa": 3,
        "gwp_100yr": 4,
        "odp": 5,
        "ashrae_safety": 6,
        "toxicity_class": 7,
    }.get(property_name, 8)
    return tier_rank * 100 + property_rank
