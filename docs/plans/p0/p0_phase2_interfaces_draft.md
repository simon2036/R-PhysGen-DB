# P0 Phase 2 Interface Draft

This draft now treats the production `schemas/*.yaml` files as the P0 contract. Phase 2 services may add execution backends, but P0 outputs must continue to flow through source manifest rows, silver extension tables, observations, recommendations, and readiness reports.

## Boundary Rules

- Do not add quantum, cycle, mixture, proxy, or active-learning fields to `property_matrix`, `model_ready`, or `model_dataset_index`.
- Every generated result must be traceable through `source_manifest`.
- Derived observations must carry `canonical_feature_key`, `quality_level`, `method`, `source_id`, and condition metadata where applicable.
- Missing Phase 2 backends are represented as empty production tables, not build failures.

## Quantum Contract

Current production schemas:

- `schemas/quantum_job.yaml`
- `schemas/quantum_artifact.yaml`

The placeholder dataclasses in `src/r_physgen_db/interfaces.py` reserve `QuantumCalculationRequest` and `QuantumCalculationResult`. Offline quantum CSV ingestion remains optional and writes empty `quantum_job` / `quantum_artifact` tables when absent.

## Cycle Contract

Current production schemas:

- `schemas/cycle_case.yaml`
- `schemas/cycle_operating_point.yaml`

`CycleOperatingPoint`, `CycleSimulationRequest`, and `CycleSimulationResult` are reserved in `src/r_physgen_db/interfaces.py`. Cycle observations must keep `cycle_case_id`, `operating_point_hash`, and `condition_set_id` references. The operating-point hash remains a stable hash of the structured operating point.

## Active Learning Contract

Current production schemas:

- `schemas/active_learning_queue.yaml`
- `schemas/active_learning_decision_log.yaml`

`active_learning_queue` supports nullable `expires_at`. The accepted vocabularies are the production vocabularies in `src/r_physgen_db/active_learning.py`: `manual_triage`, `coverage_gap`, `uncertainty_sampling`, `novelty_search`, `expected_improvement`, and `constraint_gap` for acquisition strategy; `proposed`, `approved`, `completed`, `rejected`, and `deferred` for queue status.

Optional inputs:

- `data/lake/raw/manual/active_learning_queue.csv`
- `data/lake/raw/manual/active_learning_decision_log.csv`

If both files are absent, the build writes empty queue and decision-log tables. If the decision log is present, it must reference queue entries and pass the production decision action/status vocabularies.

## Mixture Contract

Current production schemas:

- `schemas/mixture_core.yaml`
- `schemas/mixture_composition.yaml`

Mixture fractions are not imputed. `data/lake/raw/manual/mixture_fraction_curations.csv` may update existing mixture/component/basis rows only when the curation row includes source metadata. Unresolved rows, including `MIX_511A` when no traceable fraction source is present, remain unresolved.
