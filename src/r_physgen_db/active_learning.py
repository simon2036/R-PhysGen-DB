"""Active learning queue table helpers."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from r_physgen_db.quantum_pilot import ALL_QUANTUM_PROPERTY_NAMES


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

ACTIVE_LEARNING_SCORE_WEIGHTS = {
    "performance": 0.30,
    "uncertainty": 0.25,
    "novelty": 0.20,
    "feasibility": 0.15,
    "coverage_gap": 0.10,
}

ACTIVE_LEARNING_REQUIRED_PROPERTIES = [
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

DEFAULT_PRODUCTION_QUANTUM_QUEUE_ENTRIES = 2000


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

    raw_queue = pd.DataFrame(rows)
    queue = _ensure_columns(raw_queue, ACTIVE_LEARNING_QUEUE_COLUMNS)
    if "_sort_bucket" in raw_queue.columns:
        queue["_sort_bucket"] = raw_queue["_sort_bucket"].tolist()
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


def build_deterministic_active_learning_queue(
    *,
    molecule_core: pd.DataFrame,
    property_recommended: pd.DataFrame,
    seed_catalog: pd.DataFrame | None = None,
    completed_quantum_mol_ids: set[str] | None = None,
    max_entries: int = 250,
    min_quantum_entries: int | None = None,
    campaign_id: str = "v1_6_deterministic_gap_queue",
    model_version: str = "v1.6.3-draft",
    now: str = "",
) -> pd.DataFrame:
    """Generate a deterministic active-learning queue from coverage and novelty gaps."""

    if molecule_core is None or molecule_core.empty or "mol_id" not in molecule_core.columns:
        return pd.DataFrame(columns=ACTIVE_LEARNING_QUEUE_COLUMNS)
    created_at = now
    completed_quantum_mols = {_clean(mol_id) for mol_id in (completed_quantum_mol_ids or set()) if _clean(mol_id)}
    property_sets_raw = (
        property_recommended.groupby("mol_id")["property_name"].apply(set).to_dict()
        if property_recommended is not None and not property_recommended.empty
        else {}
    )
    property_sets = {
        mol_id: {property_name for property_name in properties if property_name not in ALL_QUANTUM_PROPERTY_NAMES}
        for mol_id, properties in property_sets_raw.items()
    }
    value_lookup = _property_value_lookup(property_recommended)
    candidates = _with_seed_context(molecule_core, seed_catalog)
    scaffold_counts = (
        candidates.get("scaffold_key", pd.Series("", index=candidates.index)).fillna("").astype(str).value_counts().to_dict()
        if "scaffold_key" in candidates.columns
        else {}
    )
    rows: list[dict[str, Any]] = []
    for record in candidates.fillna("").to_dict(orient="records"):
        mol_id = _clean(record.get("mol_id"))
        if not mol_id:
            continue
        model_inclusion = _clean(record.get("model_inclusion"))
        coverage_tier = _clean(record.get("coverage_tier"))
        is_promoted_gap_candidate = model_inclusion == "yes" and coverage_tier in {"A", "B", "C"}
        is_tier_d_candidate = model_inclusion == "no" and coverage_tier == "D"
        if not is_promoted_gap_candidate and not is_tier_d_candidate:
            continue
        props = property_sets.get(mol_id, set())
        components = {
            "performance": _performance_score(mol_id, value_lookup),
            "uncertainty": _coverage_gap_score(props),
            "novelty": _novelty_score(record, scaffold_counts),
            "feasibility": _feasibility_score(record, value_lookup.get((mol_id, "synthetic_accessibility"))),
            "coverage_gap": _coverage_gap_score(props),
        }
        priority = round(sum(ACTIVE_LEARNING_SCORE_WEIGHTS[key] * components[key] for key in ACTIVE_LEARNING_SCORE_WEIGHTS), 6)
        recommended_action = _next_action(props, coverage_tier=coverage_tier, model_inclusion=model_inclusion)
        strategy = _strategy(components, recommended_action)
        payload = {
            "score_weights": ACTIVE_LEARNING_SCORE_WEIGHTS,
            "score_components": components,
            "missing_properties": sorted(set(ACTIVE_LEARNING_REQUIRED_PROPERTIES) - props),
            "coverage_tier": coverage_tier,
            "seed_id": _clean(record.get("seed_id")),
            "r_number": _clean(record.get("r_number")),
            "queue_scope": "tier_d_candidate" if is_tier_d_candidate else "promoted_gap",
        }
        row = {
            "queue_entry_id": "",
            "mol_id": mol_id,
            "campaign_id": campaign_id,
            "model_version": model_version,
            "acquisition_strategy": strategy,
            "priority_score": priority,
            "uncertainty_score": round(components["uncertainty"], 6),
            "novelty_score": round(components["novelty"], 6),
            "feasibility_score": round(components["feasibility"], 6),
            "hard_constraint_status": "passed",
            "recommended_next_action": recommended_action,
            "payload_json": json.dumps(payload, sort_keys=True),
            "status": "proposed",
            "created_at": created_at,
            "updated_at": created_at,
            "expires_at": "",
            "source_id": ACTIVE_LEARNING_SOURCE_ID,
            "notes": "deterministic active-learning queue; does not mutate recommendations or model_ready",
            "_sort_bucket": 0 if is_promoted_gap_candidate else 1,
        }
        if recommended_action == "run_quantum" and mol_id in completed_quantum_mols:
            row["status"] = "completed"
            row["notes"] = _join_notes(row["notes"], "completed from quantum pilot results")
        row["queue_entry_id"] = _queue_entry_id(row)
        rows.append(row)

    raw_queue = pd.DataFrame(rows)
    queue = _ensure_columns(raw_queue, ACTIVE_LEARNING_QUEUE_COLUMNS)
    if "_sort_bucket" in raw_queue.columns:
        queue["_sort_bucket"] = raw_queue["_sort_bucket"].tolist()
    if queue.empty:
        return queue
    sorted_queue = queue.sort_values(["_sort_bucket", "priority_score", "mol_id"], ascending=[True, False, True], kind="stable")
    selected = sorted_queue.head(max_entries).copy()
    if min_quantum_entries is not None and min_quantum_entries > 0:
        selected_quantum_count = int(selected["recommended_next_action"].astype(str).eq("run_quantum").sum())
        if selected_quantum_count < min_quantum_entries:
            selected_ids = set(selected["queue_entry_id"].astype(str))
            extra_quantum = sorted_queue.loc[
                sorted_queue["recommended_next_action"].astype(str).eq("run_quantum")
                & ~sorted_queue["queue_entry_id"].astype(str).isin(selected_ids)
            ].head(min_quantum_entries - selected_quantum_count)
            selected = pd.concat([selected, extra_quantum], ignore_index=True)
    return selected.drop(columns=["_sort_bucket"], errors="ignore").reset_index(drop=True)


def active_learning_max_entries(default: int = 250) -> int:
    """Return configured active-learning queue size, falling back to ``default``."""

    return _positive_env_int("R_PHYSGEN_ACTIVE_LEARNING_MAX_ENTRIES", default)


def production_quantum_request_target(default: int = DEFAULT_PRODUCTION_QUANTUM_QUEUE_ENTRIES) -> int:
    """Return the production quantum queue target used by the build pipeline."""

    return _positive_env_int("R_PHYSGEN_QUANTUM_MAX_REQUESTS", default)


def _with_seed_context(molecule_core: pd.DataFrame, seed_catalog: pd.DataFrame | None) -> pd.DataFrame:
    """Attach seed-catalog queue context without requiring it in molecule_core."""

    out = molecule_core.copy()
    if out.empty or seed_catalog is None or seed_catalog.empty or "seed_id" not in out.columns or "seed_id" not in seed_catalog.columns:
        return out

    seed_columns = ["seed_id", "r_number", "coverage_tier", "model_inclusion", "entity_scope"]
    seed_context = seed_catalog.copy()
    for column in seed_columns:
        if column not in seed_context.columns:
            seed_context[column] = ""
    seed_context = seed_context[seed_columns].drop_duplicates(subset=["seed_id"], keep="first")
    merged = out.merge(seed_context, on="seed_id", how="left", suffixes=("", "_seed"))
    for column in ["r_number", "coverage_tier", "model_inclusion", "entity_scope"]:
        seed_column = f"{column}_seed"
        if seed_column not in merged.columns:
            continue
        if column in out.columns:
            base = merged[column].fillna("").astype(str)
            merged[column] = base.where(base.str.strip().ne(""), merged[seed_column])
        else:
            merged[column] = merged[seed_column]
        merged = merged.drop(columns=[seed_column])
    return merged


def _property_value_lookup(property_recommended: pd.DataFrame | None) -> dict[tuple[str, str], float]:
    if property_recommended is None or property_recommended.empty:
        return {}
    values: dict[tuple[str, str], float] = {}
    for record in property_recommended.to_dict(orient="records"):
        numeric = pd.to_numeric(pd.Series([record.get("value_num")]), errors="coerce").iloc[0]
        if pd.notna(numeric):
            values[(_clean(record.get("mol_id")), _clean(record.get("property_name")))] = float(numeric)
    return values


def _performance_score(mol_id: str, values: dict[tuple[str, str], float]) -> float:
    cop = values.get((mol_id, "cop_standard_cycle"))
    if cop is not None:
        return _clamp(cop / 6.0)
    return 0.5


def _coverage_gap_score(properties: set[str]) -> float:
    if not ACTIVE_LEARNING_REQUIRED_PROPERTIES:
        return 0.0
    missing = len(set(ACTIVE_LEARNING_REQUIRED_PROPERTIES) - set(properties))
    return _clamp(missing / len(ACTIVE_LEARNING_REQUIRED_PROPERTIES))


def _novelty_score(record: dict[str, Any], scaffold_counts: dict[str, int]) -> float:
    scaffold = _clean(record.get("scaffold_key"))
    if scaffold and scaffold_counts:
        return _clamp(1.0 / max(scaffold_counts.get(scaffold, 1), 1))
    heavy_atoms = pd.to_numeric(pd.Series([record.get("heavy_atom_count")]), errors="coerce").iloc[0]
    if pd.isna(heavy_atoms):
        return 0.5
    return _clamp(float(heavy_atoms) / 20.0)


def _feasibility_score(record: dict[str, Any], synthetic_accessibility: float | None) -> float:
    if synthetic_accessibility is not None:
        return _clamp(1.0 - ((synthetic_accessibility - 1.0) / 9.0))
    heavy_atoms = pd.to_numeric(pd.Series([record.get("heavy_atom_count")]), errors="coerce").iloc[0]
    if pd.isna(heavy_atoms):
        return 0.7
    return _clamp(1.0 - max(float(heavy_atoms) - 4.0, 0.0) / 30.0)


def _next_action(properties: set[str], *, coverage_tier: str = "", model_inclusion: str = "") -> str:
    if coverage_tier == "D" and model_inclusion == "no":
        return "run_quantum"
    missing = set(ACTIVE_LEARNING_REQUIRED_PROPERTIES) - set(properties)
    if {"gwp_100yr", "odp"} & missing:
        return "literature_search"
    if {"ashrae_safety", "toxicity_class"} & missing:
        return "manual_curation"
    if {"viscosity_liquid_pas", "thermal_conductivity_liquid_wmk", "cop_standard_cycle"} & missing:
        return "run_cycle"
    if not {"homo_ev", "lumo_ev", "gap_ev"} & set(properties):
        return "run_quantum"
    return "manual_curation"


def _strategy(components: dict[str, float], recommended_action: str) -> str:
    if recommended_action == "run_quantum":
        return "expected_improvement"
    if components["coverage_gap"] >= 0.5:
        return "coverage_gap"
    if components["novelty"] >= 0.75:
        return "novelty_search"
    return "uncertainty_sampling"


def _clamp(value: float) -> float:
    return float(max(0.0, min(1.0, value)))


def _positive_env_int(name: str, default: int) -> int:
    value = _clean(os.environ.get(name))
    if not value:
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


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
