"""Active learning queue table helpers."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


ACTIVE_LEARNING_SOURCE_ID = "source_r_physgen_active_learning_queue_csv"
ACTIVE_LEARNING_SOURCE_NAME = "R-PhysGen-DB Manual Active Learning Queue"

ACQUISITION_STRATEGIES = {
    "manual_triage",
    "coverage_gap",
    "uncertainty_sampling",
    "novelty_search",
    "expected_improvement",
    "constraint_gap",
}
HARD_CONSTRAINT_STATUSES = {"passed", "failed", "unknown", "not_evaluated"}
RECOMMENDED_NEXT_ACTIONS = {
    "literature_search",
    "manual_curation",
    "run_quantum",
    "run_cycle",
    "synthesize_or_purchase",
    "defer",
    "reject",
}
QUEUE_STATUSES = {"proposed", "approved", "completed", "rejected", "deferred"}
DECISION_ACTIONS = {"approve", "reject", "defer", "complete", "supersede"}
DECISION_STATUSES = {"open", "closed"}

ACTIVE_LEARNING_QUEUE_COLUMNS = [
    "queue_entry_id",
    "mol_id",
    "campaign_id",
    "model_version",
    "acquisition_strategy",
    "priority_score",
    "uncertainty_score",
    "novelty_score",
    "feasibility_score",
    "hard_constraint_status",
    "recommended_next_action",
    "payload_json",
    "status",
    "created_at",
    "updated_at",
    "expires_at",
    "source_id",
    "notes",
]

ACTIVE_LEARNING_DECISION_LOG_COLUMNS = [
    "decision_id",
    "queue_entry_id",
    "decision_action",
    "decision_status",
    "decided_by",
    "decided_at",
    "evidence_source_id",
    "notes",
]

ACTIVE_LEARNING_INPUT_COLUMNS = [
    "queue_entry_id",
    "mol_id",
    "campaign_id",
    "model_version",
    "acquisition_strategy",
    "priority_score",
    "uncertainty_score",
    "novelty_score",
    "feasibility_score",
    "hard_constraint_status",
    "recommended_next_action",
    "payload_json",
    "status",
    "created_at",
    "updated_at",
    "expires_at",
    "notes",
]

ACTIVE_LEARNING_DECISION_INPUT_COLUMNS = [
    "decision_id",
    "queue_entry_id",
    "decision_action",
    "decision_status",
    "decided_by",
    "decided_at",
    "evidence_source_id",
    "notes",
]


@dataclass(slots=True)
class ActiveLearningBuild:
    queue: pd.DataFrame
    decision_log: pd.DataFrame
    summary: dict[str, Any]
    input_exists: bool


def build_active_learning_queue(
    input_path: Path,
    molecule_core: pd.DataFrame | None = None,
    *,
    decision_log_path: Path | None = None,
) -> ActiveLearningBuild:
    """Build active learning queue tables from optional manual CSV inputs."""

    decision_log_path = decision_log_path or input_path.with_name("active_learning_decision_log.csv")
    queue_input_exists = input_path.exists()
    decision_input_exists = decision_log_path.exists()
    if not queue_input_exists and not decision_input_exists:
        queue = pd.DataFrame(columns=ACTIVE_LEARNING_QUEUE_COLUMNS)
        decision_log = pd.DataFrame(columns=ACTIVE_LEARNING_DECISION_LOG_COLUMNS)
        return ActiveLearningBuild(
            queue=queue,
            decision_log=decision_log,
            summary=active_learning_summary(
                queue,
                decision_log,
                input_exists=False,
                input_path=input_path,
                decision_log_path=decision_log_path,
            ),
            input_exists=False,
        )

    raw = _ensure_columns(pd.read_csv(input_path).fillna(""), ACTIVE_LEARNING_INPUT_COLUMNS) if queue_input_exists else pd.DataFrame(columns=ACTIVE_LEARNING_INPUT_COLUMNS)
    if molecule_core is not None and not molecule_core.empty and "mol_id" in molecule_core.columns:
        known_mol_ids = set(molecule_core["mol_id"].fillna("").astype(str).tolist())
    else:
        known_mol_ids = set()

    rows: list[dict[str, Any]] = []
    for row in raw.to_dict(orient="records"):
        normalized = _queue_row(row)
        if known_mol_ids and normalized["mol_id"] not in known_mol_ids:
            normalized["hard_constraint_status"] = "failed"
            normalized["notes"] = _join_notes(normalized["notes"], "unknown_mol_id")
        rows.append(normalized)

    queue = _ensure_columns(pd.DataFrame(rows), ACTIVE_LEARNING_QUEUE_COLUMNS)
    decision_log = _build_decision_log(decision_log_path) if decision_input_exists else pd.DataFrame(columns=ACTIVE_LEARNING_DECISION_LOG_COLUMNS)
    return ActiveLearningBuild(
        queue=queue,
        decision_log=decision_log,
        summary=active_learning_summary(
            queue,
            decision_log,
            input_exists=True,
            input_path=input_path,
            input_row_count=len(raw),
            decision_log_path=decision_log_path,
            decision_input_row_count=len(decision_log),
            queue_input_exists=queue_input_exists,
            decision_input_exists=decision_input_exists,
        ),
        input_exists=True,
    )


def active_learning_summary(
    queue: pd.DataFrame,
    decision_log: pd.DataFrame | None = None,
    *,
    input_exists: bool,
    input_path: Path | None = None,
    input_row_count: int = 0,
    decision_log_path: Path | None = None,
    decision_input_row_count: int = 0,
    queue_input_exists: bool | None = None,
    decision_input_exists: bool | None = None,
) -> dict[str, Any]:
    decision_log = decision_log if decision_log is not None else pd.DataFrame(columns=ACTIVE_LEARNING_DECISION_LOG_COLUMNS)
    action_counts = (
        queue["recommended_next_action"].fillna("").astype(str).value_counts().sort_index().to_dict()
        if not queue.empty and "recommended_next_action" in queue.columns
        else {}
    )
    status_counts = (
        queue["status"].fillna("").astype(str).value_counts().sort_index().to_dict()
        if not queue.empty and "status" in queue.columns
        else {}
    )
    campaigns = set(queue["campaign_id"].fillna("").astype(str).tolist()) if not queue.empty and "campaign_id" in queue.columns else set()
    campaigns.discard("")
    return {
        "input_status": "loaded" if input_exists else "not_configured",
        "input_path": str(input_path) if input_path is not None else "",
        "input_row_count": int(input_row_count),
        "queue_input_status": "loaded" if (queue_input_exists if queue_input_exists is not None else input_exists) else "not_configured",
        "decision_log_input_status": "loaded" if (decision_input_exists if decision_input_exists is not None else False) else "not_configured",
        "decision_log_input_path": str(decision_log_path) if decision_log_path is not None else "",
        "decision_log_input_row_count": int(decision_input_row_count),
        "queue_entry_count": int(len(queue)),
        "decision_count": int(len(decision_log)),
        "campaign_count": int(len(campaigns)),
        "recommended_next_action_counts": action_counts,
        "status_counts": status_counts,
    }


def _queue_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "queue_entry_id": _clean(row.get("queue_entry_id")),
        "mol_id": _clean(row.get("mol_id")),
        "campaign_id": _clean(row.get("campaign_id")) or "manual_seed",
        "model_version": _clean(row.get("model_version")),
        "acquisition_strategy": _clean(row.get("acquisition_strategy")) or "manual_triage",
        "priority_score": _score(row.get("priority_score"), 0.0),
        "uncertainty_score": _score(row.get("uncertainty_score"), 0.0),
        "novelty_score": _score(row.get("novelty_score"), 0.0),
        "feasibility_score": _score(row.get("feasibility_score"), 1.0),
        "hard_constraint_status": _clean(row.get("hard_constraint_status")) or "not_evaluated",
        "recommended_next_action": _clean(row.get("recommended_next_action")) or "manual_curation",
        "payload_json": _clean(row.get("payload_json")) or "{}",
        "status": _clean(row.get("status")) or "proposed",
        "created_at": _clean(row.get("created_at")),
        "updated_at": _clean(row.get("updated_at")),
        "expires_at": _clean(row.get("expires_at")),
        "source_id": ACTIVE_LEARNING_SOURCE_ID,
        "notes": _clean(row.get("notes")),
    }
    if not normalized["queue_entry_id"]:
        normalized["queue_entry_id"] = _queue_entry_id(normalized)
    return normalized


def _queue_entry_id(row: dict[str, Any]) -> str:
    signature = "|".join(
        [
            row.get("mol_id", ""),
            row.get("campaign_id", ""),
            row.get("model_version", ""),
            row.get("acquisition_strategy", ""),
            row.get("recommended_next_action", ""),
            row.get("payload_json", ""),
        ]
    )
    return "alq_" + hashlib.sha256(signature.encode("utf-8")).hexdigest()[:20]


def _build_decision_log(input_path: Path) -> pd.DataFrame:
    raw = _ensure_columns(pd.read_csv(input_path).fillna(""), ACTIVE_LEARNING_DECISION_INPUT_COLUMNS)
    rows = [_decision_row(row) for row in raw.to_dict(orient="records")]
    return _ensure_columns(pd.DataFrame(rows), ACTIVE_LEARNING_DECISION_LOG_COLUMNS)


def _decision_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "decision_id": _clean(row.get("decision_id")),
        "queue_entry_id": _clean(row.get("queue_entry_id")),
        "decision_action": _clean(row.get("decision_action")) or "defer",
        "decision_status": _clean(row.get("decision_status")) or "open",
        "decided_by": _clean(row.get("decided_by")),
        "decided_at": _clean(row.get("decided_at")),
        "evidence_source_id": _clean(row.get("evidence_source_id")),
        "notes": _clean(row.get("notes")),
    }
    if not normalized["decision_id"]:
        normalized["decision_id"] = _decision_id(normalized)
    return normalized


def _decision_id(row: dict[str, Any]) -> str:
    signature = "|".join(
        [
            row.get("queue_entry_id", ""),
            row.get("decision_action", ""),
            row.get("decision_status", ""),
            row.get("decided_at", ""),
            row.get("evidence_source_id", ""),
        ]
    )
    return "ald_" + hashlib.sha256(signature.encode("utf-8")).hexdigest()[:20]


def _score(value: Any, default: float) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return default
    return float(numeric)


def _join_notes(*parts: str) -> str:
    return "; ".join(part for part in parts if part)


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
