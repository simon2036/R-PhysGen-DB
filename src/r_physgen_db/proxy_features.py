"""Deterministic screening proxies for degradation and synthesis triage."""

from __future__ import annotations

from statistics import mean
from typing import Any

import pandas as pd
from rdkit import Chem

from r_physgen_db.chemistry import compute_screening_features


PROXY_SOURCE_ID = "source_r_physgen_proxy_heuristics"
PROXY_SOURCE_NAME = "R-PhysGen-DB Proxy Heuristics"
PROXY_ASSESSMENT_VERSION = "pr-e-heuristic-v1"
PROXY_DATA_QUALITY_SCORE = 35
PROXY_ML_USE_STATUS = "screening_proxy_not_recommended_as_target"

TFA_RISK_PROPERTY = "tfa_risk_proxy"
SYNTHETIC_ACCESSIBILITY_PROPERTY = "synthetic_accessibility"
PROXY_PROPERTIES = {TFA_RISK_PROPERTY, SYNTHETIC_ACCESSIBILITY_PROPERTY}
PROXY_CANONICAL_FEATURE_KEYS = {
    TFA_RISK_PROPERTY: "environmental.tfa_risk_proxy",
    SYNTHETIC_ACCESSIBILITY_PROPERTY: "synthesis.synthetic_accessibility_score",
}
PROXY_FEATURE_KEYS = set(PROXY_PROPERTIES) | set(PROXY_CANONICAL_FEATURE_KEYS.values())
TFA_RISK_SCORE = {"none": 0, "low": 1, "medium": 2, "high": 3}
TFA_RISK_VOCABULARY = set(TFA_RISK_SCORE) | {"unknown"}


def proxy_feature_metadata(summary: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "source_id": PROXY_SOURCE_ID,
        "source_name": PROXY_SOURCE_NAME,
        "assessment_version": PROXY_ASSESSMENT_VERSION,
        "method": "deterministic RDKit structural screening heuristics",
        "disclaimer": "Screening proxy only; not an experimental, literature, OPERA, or CompTox result.",
        "properties": sorted(PROXY_PROPERTIES),
        "summary": summary or {},
    }


def build_proxy_feature_rows(molecule_core: pd.DataFrame) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if molecule_core.empty:
        return rows, proxy_feature_summary(pd.DataFrame())

    for record in molecule_core.to_dict(orient="records"):
        mol_id = _clean(record.get("mol_id"))
        smiles = _clean(record.get("isomeric_smiles")) or _clean(record.get("canonical_smiles"))
        if not mol_id or not smiles:
            continue

        try:
            features = compute_screening_features(smiles)
            tfa = tfa_risk_proxy(features, smiles)
            synthesis = synthetic_accessibility_score(features)
        except Exception as exc:  # noqa: BLE001 - proxy generation must not break the build
            tfa = {"value": "unknown", "value_num": None, "notes": f"proxy_generation_failed:{exc}"}
            synthesis = {"value": "", "value_num": None, "notes": f"proxy_generation_failed:{exc}"}

        rows.append(
            _proxy_observation_row(
                mol_id=mol_id,
                property_name=TFA_RISK_PROPERTY,
                value=tfa["value"],
                value_num=tfa["value_num"],
                unit="categorical",
                notes=tfa["notes"],
            )
        )
        rows.append(
            _proxy_observation_row(
                mol_id=mol_id,
                property_name=SYNTHETIC_ACCESSIBILITY_PROPERTY,
                value=synthesis["value"],
                value_num=synthesis["value_num"],
                unit="dimensionless",
                notes=synthesis["notes"],
            )
        )

    return rows, proxy_feature_summary(pd.DataFrame(rows))


def tfa_risk_proxy(features: dict[str, Any], smiles: str) -> dict[str, Any]:
    if not bool(features.get("allowed_elements_only", True)):
        return {"value": "unknown", "value_num": None, "notes": "disallowed_elements_out_of_proxy_domain"}

    fluorine_count = int(features.get("atom_count_f") or 0)
    if fluorine_count <= 0:
        return {"value": "none", "value_num": 0, "notes": "no_fluorine_atoms"}

    has_cf3 = _has_substructure(smiles, "[CX4](F)(F)F")
    has_double_bond = bool(features.get("has_c_c_double_bond"))
    has_oxygen_functionality = bool(features.get("has_ether")) or bool(features.get("has_carbonyl"))

    if has_cf3 and (has_double_bond or has_oxygen_functionality):
        value = "high"
        rationale = "cf3_with_unsaturation_or_oxygen_functionality"
    elif has_cf3 or (fluorine_count >= 3 and (has_double_bond or has_oxygen_functionality)):
        value = "medium"
        rationale = "fluorinated_structural_motif"
    else:
        value = "low"
        rationale = "fluorinated_without_high_risk_motif"
    return {"value": value, "value_num": TFA_RISK_SCORE[value], "notes": rationale}


def synthetic_accessibility_score(features: dict[str, Any]) -> dict[str, Any]:
    heavy_atoms = int(features.get("heavy_atom_count") or 0)
    ring_count = int(features.get("ring_count") or 0)
    halogens = int(features.get("halogen_count_total") or 0)
    double_bonds = int(features.get("double_bond_count") or 0)
    hetero_atoms = sum(int(features.get(f"atom_count_{key}") or 0) for key in ["o", "n", "s"])

    score = 1.0
    score += 0.08 * heavy_atoms
    score += 0.35 * ring_count
    score += 0.10 * halogens
    score += 0.08 * hetero_atoms
    score += 0.08 * double_bonds
    if bool(features.get("stereo_flag")):
        score += 0.40
    if bool(features.get("has_carbonyl")):
        score += 0.18
    if bool(features.get("has_ether")):
        score += 0.12
    if not bool(features.get("allowed_elements_only", True)):
        score += 1.0

    score = max(1.0, min(10.0, score))
    rounded = round(float(score), 3)
    return {"value": f"{rounded:.3f}", "value_num": rounded, "notes": "rdkit_structural_complexity_heuristic"}


def proxy_feature_summary(property_observation: pd.DataFrame) -> dict[str, Any]:
    if property_observation.empty or "property_name" not in property_observation.columns:
        return _empty_proxy_summary()

    proxy_rows = property_observation.loc[property_observation["property_name"].fillna("").astype(str).isin(PROXY_PROPERTIES)].copy()
    if proxy_rows.empty:
        return _empty_proxy_summary()

    tfa_rows = proxy_rows.loc[proxy_rows["property_name"].astype(str).eq(TFA_RISK_PROPERTY)]
    synth_rows = proxy_rows.loc[proxy_rows["property_name"].astype(str).eq(SYNTHETIC_ACCESSIBILITY_PROPERTY)]
    synthetic_values = pd.to_numeric(synth_rows.get("value_num", pd.Series(dtype="float64")), errors="coerce").dropna().tolist()

    return {
        "proxy_observation_count": int(len(proxy_rows)),
        "proxy_molecule_count": int(proxy_rows["mol_id"].nunique()) if "mol_id" in proxy_rows.columns else 0,
        "tfa_risk_proxy_count": int(len(tfa_rows)),
        "synthetic_accessibility_count": int(len(synth_rows)),
        "tfa_risk_distribution": tfa_rows["value"].fillna("unknown").astype(str).value_counts().sort_index().to_dict()
        if not tfa_rows.empty
        else {},
        "synthetic_accessibility_score": {
            "count": int(len(synthetic_values)),
            "min": float(min(synthetic_values)) if synthetic_values else None,
            "mean": float(mean(synthetic_values)) if synthetic_values else None,
            "max": float(max(synthetic_values)) if synthetic_values else None,
        },
    }


def _empty_proxy_summary() -> dict[str, Any]:
    return {
        "proxy_observation_count": 0,
        "proxy_molecule_count": 0,
        "tfa_risk_proxy_count": 0,
        "synthetic_accessibility_count": 0,
        "tfa_risk_distribution": {},
        "synthetic_accessibility_score": {"count": 0, "min": None, "mean": None, "max": None},
    }


def _proxy_observation_row(
    *,
    mol_id: str,
    property_name: str,
    value: str,
    value_num: float | int | None,
    unit: str,
    notes: str,
) -> dict[str, Any]:
    return {
        "observation_id": None,
        "mol_id": mol_id,
        "property_name": property_name,
        "value": str(value),
        "value_num": value_num,
        "unit": unit,
        "standard_unit": unit,
        "temperature": "",
        "pressure": "",
        "phase": "",
        "source_type": "derived_harmonized",
        "source_name": PROXY_SOURCE_NAME,
        "source_id": PROXY_SOURCE_ID,
        "source_record_id": f"{PROXY_ASSESSMENT_VERSION}:{mol_id}:{property_name}",
        "method": "deterministic RDKit structural screening heuristic",
        "uncertainty": "screening_proxy",
        "quality_level": "snapshot_only",
        "assessment_version": PROXY_ASSESSMENT_VERSION,
        "time_horizon": "",
        "year": "2026",
        "notes": notes,
        "qc_status": "pass",
        "qc_flags": "",
        "canonical_feature_key": PROXY_CANONICAL_FEATURE_KEYS[property_name],
        "source_priority_rank": 9999,
        "data_quality_score_100": PROXY_DATA_QUALITY_SCORE,
        "is_proxy_or_screening": 1,
        "ml_use_status": PROXY_ML_USE_STATUS,
        "ingestion_stage_id": "05",
    }


def _has_substructure(smiles: str, smarts: str) -> bool:
    mol = Chem.MolFromSmiles(smiles)
    patt = Chem.MolFromSmarts(smarts)
    if mol is None or patt is None:
        return False
    return bool(mol.HasSubstructMatch(patt))


def _clean(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()
