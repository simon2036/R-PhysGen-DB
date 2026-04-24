"""Phase 2 interface placeholders reserved by V1."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class QuantumCalculationRequest:
    request_id: str
    mol_id: str
    isomeric_smiles: str
    charge: int
    spin_multiplicity: int
    conformer_generation_method: str
    conformer_force_field: str
    conformer_count: int
    conformer_selection_criterion: str
    method_family: str
    program: str
    program_version: str
    model_chemistry: str
    basis_set: str
    theory_level: str
    solvation_model: str
    target_canonical_features: list[str]
    artifact_root: str
    metadata_json: str


@dataclass(slots=True)
class QuantumCalculationResult:
    request_id: str
    run_id: str
    status: str
    failure_reason: str
    program_version: str
    wall_time_s: float
    converged: bool
    imaginary_frequency_count: int
    lowest_energy_conformer_id: str
    artifact_manifest_json: str
    derived_observations_json: str
    quality_level: str
    notes: str


@dataclass(slots=True)
class CycleOperatingPoint:
    evaporating_temperature_c: float
    condensing_temperature_c: float
    subcooling_k: float
    superheat_k: float
    compressor_isentropic_efficiency: float
    ambient_temperature_c: float | None = None
    gas_cooler_outlet_temperature_c: float | None = None
    high_side_pressure_mpa: float | None = None


@dataclass(slots=True)
class CycleSimulationRequest:
    request_id: str
    mol_id: str
    fluid_name: str
    mixture_id: str
    mixture_composition_json: str
    cycle_case_id: str
    operating_point: CycleOperatingPoint
    operating_point_hash: str
    cycle_model: str
    eos_source: str
    compressor_efficiency: float
    artifact_root: str
    metadata_json: str


@dataclass(slots=True)
class CycleSimulationResult:
    request_id: str
    run_id: str
    status: str
    cycle_case_id: str
    operating_point_hash: str
    convergence_flag: bool
    warning_flags_json: str
    artifact_manifest_json: str
    derived_observations_json: str
    notes: str


@dataclass(slots=True)
class ActiveLearningQueueEntry:
    queue_entry_id: str
    mol_id: str
    campaign_id: str
    model_version: str
    acquisition_strategy: str
    priority_score: float
    uncertainty_score: float
    novelty_score: float
    feasibility_score: float
    hard_constraint_status: str
    recommended_next_action: str
    payload_json: str
    status: str
    created_at: str
    updated_at: str
    expires_at: str
    source_id: str
    notes: str


@dataclass(slots=True)
class ActiveLearningDecisionLogEntry:
    decision_id: str
    queue_entry_id: str
    decision_action: str
    decision_status: str
    decided_by: str
    decided_at: str
    evidence_source_id: str
    notes: str


class Phase2InterfaceNotConfigured(RuntimeError):
    """Raised when a phase 2 interface is intentionally not configured in V1."""


def quantum_calculation(_: QuantumCalculationRequest) -> None:
    raise Phase2InterfaceNotConfigured("Quantum calculation is reserved for phase 2.")


def cycle_simulation(_: CycleSimulationRequest) -> None:
    raise Phase2InterfaceNotConfigured("External cycle simulation is reserved for phase 2.")


def active_learning_queue(_: ActiveLearningQueueEntry) -> None:
    raise Phase2InterfaceNotConfigured("Active learning queue is reserved for phase 2.")
