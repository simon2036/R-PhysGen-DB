"""Phase 2 interface placeholders reserved by V1."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class QuantumCalculationRequest:
    mol_id: str
    isomeric_smiles: str
    method: str
    basis_set: str
    metadata: dict[str, Any]


@dataclass(slots=True)
class CycleSimulationRequest:
    mol_id: str
    fluid_name: str
    operating_point: dict[str, Any]


@dataclass(slots=True)
class ActiveLearningQueueEntry:
    mol_id: str
    priority: float
    reason: str
    payload: dict[str, Any]


class Phase2InterfaceNotConfigured(RuntimeError):
    """Raised when a phase 2 interface is intentionally not configured in V1."""


def quantum_calculation(_: QuantumCalculationRequest) -> None:
    raise Phase2InterfaceNotConfigured("Quantum calculation is reserved for phase 2.")


def cycle_simulation(_: CycleSimulationRequest) -> None:
    raise Phase2InterfaceNotConfigured("External cycle simulation is reserved for phase 2.")


def active_learning_queue(_: ActiveLearningQueueEntry) -> None:
    raise Phase2InterfaceNotConfigured("Active learning queue is reserved for phase 2.")
