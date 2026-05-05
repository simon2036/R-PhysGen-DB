# Phase 2 Interfaces

V1 reserves interfaces for heavier computation and feedback loops without making them a runtime dependency.

Implemented placeholders:

- `quantum_calculation`
- `cycle_simulation`
- `active_learning_queue`

These interfaces are defined in `src/r_physgen_db/interfaces.py` and can be extended later without changing the V1 storage contract.

## Integration Rule

Any phase 2 service must write outputs back through the same `property_observation` contract and register artifacts in `source_manifest`.
