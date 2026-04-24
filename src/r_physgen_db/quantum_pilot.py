"""Offline quantum pilot ingestion helpers."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


QUANTUM_SOURCE_ID = "source_r_physgen_quantum_pilot"
QUANTUM_SOURCE_NAME = "R-PhysGen-DB Quantum Pilot Offline Results"
QUANTUM_ASSESSMENT_VERSION = "pr-f-quantum-pilot-v1"
QUANTUM_ML_USE_STATUS = "long_form_quantum_pilot_not_wide_ml_target"

QUANTUM_FEATURES = {
    "quantum.homo_energy": {"property_name": "homo_ev", "unit": "eV"},
    "quantum.lumo_energy": {"property_name": "lumo_ev", "unit": "eV"},
    "quantum.homo_lumo_gap": {"property_name": "gap_ev", "unit": "eV"},
}
QUANTUM_CANONICAL_FEATURE_KEYS = set(QUANTUM_FEATURES)
QUANTUM_PROPERTY_NAMES = {item["property_name"] for item in QUANTUM_FEATURES.values()}
QUANTUM_FEATURE_KEYS = QUANTUM_CANONICAL_FEATURE_KEYS | QUANTUM_PROPERTY_NAMES
QUANTUM_QUALITY_LEVELS = {"computed_standard", "computed_high", "estimated_group_contrib", "calculated_open_source"}
QUANTUM_FORBIDDEN_WIDE_COLUMNS = QUANTUM_PROPERTY_NAMES | {f"has_{name}" for name in QUANTUM_PROPERTY_NAMES}

QUANTUM_INPUT_COLUMNS = [
    "request_id",
    "mol_id",
    "canonical_feature_key",
    "value_num",
    "unit",
    "program",
    "program_version",
    "method_family",
    "theory_level",
    "basis_set",
    "solvation_model",
    "converged",
    "imaginary_frequency_count",
    "artifact_uri",
    "artifact_sha256",
    "quality_level",
    "notes",
]

QUANTUM_JOB_COLUMNS = [
    "request_id",
    "mol_id",
    "status",
    "program",
    "program_version",
    "method_family",
    "theory_level",
    "basis_set",
    "solvation_model",
    "quality_level",
    "converged",
    "imaginary_frequency_count",
    "derived_observation_count",
    "artifact_count",
    "source_id",
    "source_name",
    "created_by_stage_id",
    "notes",
]

QUANTUM_ARTIFACT_COLUMNS = [
    "artifact_id",
    "request_id",
    "mol_id",
    "artifact_uri",
    "artifact_sha256",
    "artifact_role",
    "source_id",
    "created_by_stage_id",
    "notes",
]


@dataclass(slots=True)
class QuantumPilotBuild:
    property_rows: list[dict[str, Any]]
    quantum_job: pd.DataFrame
    quantum_artifact: pd.DataFrame
    summary: dict[str, Any]
    input_exists: bool


def build_quantum_pilot(
    input_path: Path,
    molecule_core: pd.DataFrame | None = None,
    *,
    created_by_stage_id: str = "05",
) -> QuantumPilotBuild:
    """Build quantum pilot tables and long-form observation rows from an optional CSV."""

    if not input_path.exists():
        return QuantumPilotBuild(
            property_rows=[],
            quantum_job=_empty_job_frame(),
            quantum_artifact=_empty_artifact_frame(),
            summary=quantum_pilot_summary(input_exists=False, input_path=input_path),
            input_exists=False,
        )

    raw = pd.read_csv(input_path).fillna("")
    raw = _ensure_columns(raw, QUANTUM_INPUT_COLUMNS)
    if molecule_core is not None and not molecule_core.empty and "mol_id" in molecule_core.columns:
        known_mol_ids = set(molecule_core["mol_id"].fillna("").astype(str).tolist())
    else:
        known_mol_ids = set()

    property_rows: list[dict[str, Any]] = []
    artifacts: dict[str, dict[str, Any]] = {}
    row_statuses: list[str] = []
    for row in raw.to_dict(orient="records"):
        status = _row_status(row, known_mol_ids)
        row_statuses.append(status)
        if status == "succeeded":
            property_rows.append(_observation_row(row, created_by_stage_id=created_by_stage_id))
        artifact = _artifact_row(row, created_by_stage_id=created_by_stage_id)
        if artifact is not None:
            artifacts[artifact["artifact_id"]] = artifact

    artifact_frame = _ensure_columns(pd.DataFrame(artifacts.values()), QUANTUM_ARTIFACT_COLUMNS)
    job_frame = _build_job_frame(raw, property_rows, artifact_frame, row_statuses, created_by_stage_id=created_by_stage_id)
    summary = quantum_pilot_summary(
        input_exists=True,
        input_path=input_path,
        input_row_count=len(raw),
        property_rows=property_rows,
        quantum_job=job_frame,
        quantum_artifact=artifact_frame,
    )
    return QuantumPilotBuild(
        property_rows=property_rows,
        quantum_job=job_frame,
        quantum_artifact=artifact_frame,
        summary=summary,
        input_exists=True,
    )


def quantum_pilot_summary(
    *,
    input_exists: bool,
    input_path: Path | None = None,
    input_row_count: int = 0,
    property_rows: list[dict[str, Any]] | None = None,
    quantum_job: pd.DataFrame | None = None,
    quantum_artifact: pd.DataFrame | None = None,
) -> dict[str, Any]:
    rows = property_rows or []
    feature_counts: dict[str, int] = {}
    for row in rows:
        key = _clean(row.get("canonical_feature_key"))
        feature_counts[key] = feature_counts.get(key, 0) + 1
    return {
        "input_status": "loaded" if input_exists else "not_configured",
        "input_path": str(input_path) if input_path is not None else "",
        "input_row_count": int(input_row_count),
        "quantum_job_count": int(len(quantum_job)) if quantum_job is not None else 0,
        "quantum_artifact_count": int(len(quantum_artifact)) if quantum_artifact is not None else 0,
        "quantum_observation_count": int(len(rows)),
        "quantum_molecule_count": int(len({_clean(row.get("mol_id")) for row in rows if _clean(row.get("mol_id"))})),
        "feature_counts": dict(sorted(feature_counts.items())),
    }


def _row_status(row: dict[str, Any], known_mol_ids: set[str]) -> str:
    mol_id = _clean(row.get("mol_id"))
    if known_mol_ids and mol_id not in known_mol_ids:
        return "failed_unknown_molecule"
    if not _truthy(row.get("converged")):
        return "failed_not_converged"
    if _int_value(row.get("imaginary_frequency_count")) != 0:
        return "failed_imaginary_frequency"
    if pd.isna(pd.to_numeric(pd.Series([row.get("value_num")]), errors="coerce").iloc[0]):
        return "failed_missing_value"
    return "succeeded"


def _observation_row(row: dict[str, Any], *, created_by_stage_id: str) -> dict[str, Any]:
    canonical_key = _clean(row.get("canonical_feature_key"))
    feature = QUANTUM_FEATURES.get(canonical_key, {})
    property_name = feature.get("property_name", canonical_key or "quantum_unknown")
    unit = _clean(row.get("unit")) or feature.get("unit", "eV")
    value_num = float(pd.to_numeric(pd.Series([row.get("value_num")]), errors="coerce").iloc[0])
    quality_level = _clean(row.get("quality_level")) or "computed_standard"
    method = _method_label(row)
    source_record_id = f"{_clean(row.get('request_id'))}:{canonical_key}"
    quality_score = 95 if quality_level == "computed_high" else (65 if quality_level == "estimated_group_contrib" else 80)
    return {
        "observation_id": "",
        "mol_id": _clean(row.get("mol_id")),
        "property_name": property_name,
        "value": f"{value_num:.12g}",
        "value_num": value_num,
        "unit": unit,
        "standard_unit": unit,
        "standard_value_num": value_num,
        "temperature": "298.15 K",
        "pressure": "",
        "phase": "gas",
        "source_type": "calculated_open_source",
        "source_name": QUANTUM_SOURCE_NAME,
        "source_id": QUANTUM_SOURCE_ID,
        "source_record_id": source_record_id,
        "method": method,
        "uncertainty": "",
        "quality_level": quality_level,
        "assessment_version": QUANTUM_ASSESSMENT_VERSION,
        "notes": _clean(row.get("notes")),
        "qc_status": "pass",
        "qc_flags": "",
        "canonical_feature_key": canonical_key,
        "source_priority_rank": 600,
        "data_quality_score_100": quality_score,
        "is_proxy_or_screening": 0,
        "ml_use_status": QUANTUM_ML_USE_STATUS,
        "ingestion_stage_id": created_by_stage_id,
        "normalization_rule_id": "quantum_ev_identity",
        "convergence_flag": 1,
    }


def _artifact_row(row: dict[str, Any], *, created_by_stage_id: str) -> dict[str, Any] | None:
    uri = _clean(row.get("artifact_uri"))
    digest = _clean(row.get("artifact_sha256"))
    if not uri and not digest:
        return None
    request_id = _clean(row.get("request_id"))
    artifact_id = "qart_" + hashlib.sha256(f"{request_id}|{uri}|{digest}".encode("utf-8")).hexdigest()[:16]
    return {
        "artifact_id": artifact_id,
        "request_id": request_id,
        "mol_id": _clean(row.get("mol_id")),
        "artifact_uri": uri,
        "artifact_sha256": digest,
        "artifact_role": "quantum_result_bundle",
        "source_id": QUANTUM_SOURCE_ID,
        "created_by_stage_id": created_by_stage_id,
        "notes": _clean(row.get("notes")),
    }


def _build_job_frame(
    raw: pd.DataFrame,
    property_rows: list[dict[str, Any]],
    artifact_frame: pd.DataFrame,
    row_statuses: list[str],
    *,
    created_by_stage_id: str,
) -> pd.DataFrame:
    if raw.empty:
        return _empty_job_frame()

    raw = raw.copy()
    raw["_row_status"] = row_statuses
    observation_count = {}
    for row in property_rows:
        request_id = _clean(row.get("source_record_id")).split(":", 1)[0]
        observation_count[request_id] = observation_count.get(request_id, 0) + 1
    artifact_count = artifact_frame.groupby("request_id").size().to_dict() if not artifact_frame.empty else {}

    jobs: list[dict[str, Any]] = []
    for request_id, group in raw.groupby("request_id", sort=True, dropna=False):
        first = group.iloc[0].to_dict()
        status = "succeeded" if int(observation_count.get(_clean(request_id), 0)) > 0 else "failed"
        jobs.append(
            {
                "request_id": _clean(request_id),
                "mol_id": _clean(first.get("mol_id")),
                "status": status,
                "program": _clean(first.get("program")),
                "program_version": _clean(first.get("program_version")),
                "method_family": _clean(first.get("method_family")),
                "theory_level": _clean(first.get("theory_level")),
                "basis_set": _clean(first.get("basis_set")),
                "solvation_model": _clean(first.get("solvation_model")),
                "quality_level": _clean(first.get("quality_level")) or "computed_standard",
                "converged": int(group["converged"].map(_truthy).all()),
                "imaginary_frequency_count": int(max(_int_value(value) for value in group["imaginary_frequency_count"].tolist())),
                "derived_observation_count": int(observation_count.get(_clean(request_id), 0)),
                "artifact_count": int(artifact_count.get(_clean(request_id), 0)),
                "source_id": QUANTUM_SOURCE_ID,
                "source_name": QUANTUM_SOURCE_NAME,
                "created_by_stage_id": created_by_stage_id,
                "notes": "; ".join(sorted(set(group["_row_status"].astype(str).tolist()))),
            }
        )
    return _ensure_columns(pd.DataFrame(jobs), QUANTUM_JOB_COLUMNS)


def _method_label(row: dict[str, Any]) -> str:
    pieces = [
        _clean(row.get("program")),
        _clean(row.get("theory_level")),
        _clean(row.get("basis_set")),
        _clean(row.get("solvation_model")),
    ]
    return " / ".join(piece for piece in pieces if piece) or "offline quantum calculation"


def _truthy(value: Any) -> bool:
    return _clean(value).lower() in {"1", "true", "yes", "y", "converged", "succeeded"}


def _int_value(value: Any) -> int:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return 0
    return int(numeric)


def _empty_job_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=QUANTUM_JOB_COLUMNS)


def _empty_artifact_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=QUANTUM_ARTIFACT_COLUMNS)


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in df.columns:
            df[column] = None
    return df[columns]


def _clean(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value).strip()
