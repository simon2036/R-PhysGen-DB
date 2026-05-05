# R-PhysGen-DB

R-PhysGen-DB is a file-lake-first refrigerant data foundation for AI-assisted fifth-generation refrigerant design.

Wave 2 extends the V1 base with:

- single-component neutral small molecules
- open-source-first acquisition
- layered storage in `data/lake/raw`, `data/lake/bronze`, `data/lake/silver`, `data/lake/gold`, and `data/lake/extensions`
- Parquet as the primary persistence format
- DuckDB as the local query/index layer
- a full inventory seed catalog plus a model-ready promoted subset
- explicit `regulatory_status` and `pending_sources` tables
- NIST phase-table parsing plus EPA ODS/SNAP adapters
- explicit CoolProp alias control and transcritical `R-744` cycle handling
- `Tier A/B/C` promoted coverage waves plus `Tier D` inventory-only molecules
- explicit `entity_scope` and `model_inclusion` control in `data/lake/raw/manual/seed_catalog.csv`
- `2026-04-22` property-governance bundle alignment with canonical observation/recommended tables and a strict ML-filtered canonical slice
- explicit manual adjudication of reviewed canonical queue rows without widening the legacy projection layer
- controlled proxy-only strict acceptance for governed canonical features when no non-proxy candidate exists
- phase-2 governance quantum handoff for xTB Hessian and CREST conformer summaries, with ORCA opt/freq kept blocked on a real ORCA QC executable
- residual CoolProp cycle backend computations for the open active-learning `run_cycle` queue where the open-source backend can resolve the fluid

Reference inputs now live under `data/sources/`. The property-governance bundle at [`data/sources/property_governance/refrigerant_seed_database_20260422_property_governance_bundle.zip`](data/sources/property_governance/refrigerant_seed_database_20260422_property_governance_bundle.zip) is ingested into the extension and canonical-property layers. Historical prototypes and superseded method packages are archived under [`archive/2026-05-05/`](archive/2026-05-05/).

P0 review packages, including `R-PhysGen-DB_P0_package_v3.zip`, are archived with the 2026-05-05 manifest. Accepted P0 assets now live in `docs/plans/p0/`, `docs/reviews/`, `schemas/`, `schemas/drafts/`, `scripts/`, `src/r_physgen_db/blueprints/`, and the staged production pipeline. PR-A through PR-H have been incorporated locally: staged orchestration, condition sets, research-task readiness, structured cycle operating points, screening proxy features, offline quantum pilot ingestion, governed mixture tables, active-learning queue outputs, dataset `VERSION`, and CI contract tests are now part of the build/validate path. Remaining work is tracked in [`docs/plans/p0/p0_remaining_backlog.md`](docs/plans/p0/p0_remaining_backlog.md).

Current local baseline after the `v1.6.3-draft` phase-2 quantum handoff and residual computation pass:

- dataset version: `v1.6.3-draft`
- `seed_catalog_count`: `20707` rows (`77` refrigerants + `20630` candidates)
- `resolved_molecule_count`: `20567`
- `model_dataset_index_count`: `120`
- `property_observation`: `60752`
- `property_recommended`: `56770`
- `property_recommended_canonical_count`: `57174`
- `property_recommended_canonical_strict_count`: `2539`
- `observation_condition_set_count`: `110`
- `cycle_case_count`: `7`
- `cycle_operating_point_count`: `6`
- `mixture_core_count`: `123`
- `mixture_composition_count`: `378`
- `quantum_job_count`: `2367` (`2365` succeeded, `2` failed xTB Hessian attempts retained for audit)
- `quantum_artifact_count`: `2367`
- `quantum_observation_count`: `13822`
- phase-2 xTB Hessian: `62` attempted, `60` accepted, `2` failed due imaginary frequencies
- phase-2 CREST conformer summaries: `62` succeeded, `124` scalar observations
- Psi4 DFT expansion manifest: `150` requests now marked completed from existing `900` Psi4 scalar rows
- residual cycle backend pass: `21` attempted, `4` CoolProp successes (`16` rows), `17` still blocked on REFPROP root / backend coverage
- ORCA opt/freq: manifests generated (`3` smoke + `59` full requests), blocked because no ORCA quantum-chemistry executable is configured
- `active_learning_queue_count`: `2088` (`2000` completed `run_quantum` + `88` proposed follow-ups, including `17` remaining `run_cycle`)
- `active_learning_decision_log_count`: `0`
- PR-E proxy observations: `41134` across `20567` molecules
- `property_recommended_canonical_review_queue_count`: `0`
- open canonical conflict/source-divergence review rows: `0`
- proxy-only rows promoted into strict via policy: `396`
- canonical review decisions applied: `120` = `35` `accept_selected_source` + `85` `accept_out_of_strict`
- property-governance extension mirror: `218/218` tables aligned

## Quick Start

```powershell
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe pipelines\generate_wave2_seed_catalog.py
.venv\Scripts\python.exe pipelines\build_v1_dataset.py
.venv\Scripts\python.exe pipelines\validate_v1_dataset.py
.venv\Scripts\pytest.exe -q
```

## Key Outputs

- `data/lake/raw/manual/seed_catalog.csv`
- `data/lake/raw/manual/refrigerant_inventory.csv`
- `data/lake/raw/manual/manual_property_observations.csv`
- `data/lake/raw/manual/property_governance_20260422_unresolved_curations.csv`
- `data/lake/raw/manual/property_governance_20260422_canonical_review_decisions.csv`
- `data/lake/raw/manual/property_governance_20260422_proxy_acceptance_rules.csv`
- `data/lake/raw/manual/observations/*.csv`
- `data/lake/raw/generated/property_governance_20260422_seed_catalog.csv`
- `data/lake/raw/generated/governance_phase2_summary.json`
- `data/lake/raw/generated/governance_phase2_mapping_report.csv`
- `data/lake/raw/generated/governance_phase2_xtb_hessian_requests.csv`
- `data/lake/raw/generated/governance_phase2_crest_requests.csv`
- `data/lake/raw/generated/governance_phase2_orca_optfreq_*.csv`
- `data/lake/raw/manual/quantum_pilot_results.csv`
- `data/lake/raw/manual/cycle_backend_results.csv`
- `data/lake/raw/manual/quantum_phase2_vibrational_modes.csv`
- `data/lake/raw/manual/quantum_phase2_conformer_ensemble.csv`
- `data/lake/bronze/source_manifest.parquet`
- `data/lake/bronze/pending_sources.parquet`
- `data/lake/bronze/property_governance_20260422_substance_crosswalk.parquet`
- `data/lake/bronze/property_governance_20260422_unresolved_substances.parquet`
- `data/lake/bronze/property_governance_20260422_audit.json`
- `data/lake/silver/molecule_core.parquet`
- `data/lake/silver/molecule_alias.parquet`
- `data/lake/silver/property_observation.parquet`
- `data/lake/silver/property_observation_canonical.parquet`
- `data/lake/silver/observation_condition_set.parquet`
- `data/lake/silver/cycle_case.parquet`
- `data/lake/silver/cycle_operating_point.parquet`
- `data/lake/silver/quantum_job.parquet`
- `data/lake/silver/quantum_artifact.parquet`
- `data/lake/silver/mixture_core.parquet`
- `data/lake/silver/mixture_composition.parquet`
- `data/lake/silver/regulatory_status.parquet`
- `data/lake/gold/property_recommended.parquet`
- `data/lake/gold/property_dictionary.parquet`
- `data/lake/gold/property_canonical_map.parquet`
- `data/lake/gold/property_recommended_canonical.parquet`
- `data/lake/gold/property_recommended_canonical_strict.parquet`
- `data/lake/gold/property_recommended_canonical_review_queue.parquet`
- `data/lake/gold/molecule_master.parquet`
- `data/lake/gold/property_matrix.parquet`
- `data/lake/gold/model_dataset_index.parquet`
- `data/lake/gold/model_ready.parquet`
- `data/lake/gold/research_task_readiness_report.parquet`
- `data/lake/gold/active_learning_queue.parquet`
- `data/lake/gold/active_learning_decision_log.parquet`
- `data/lake/gold/VERSION`
- `data/lake/gold/quality_report.json`
- `data/lake/gold/validation_report.json`
- `data/lake/extensions/property_governance_20260422/`
- `data/indexes/r_physgen_v2.duckdb`

## Documentation

- [docs/overview/current_status.md](docs/overview/current_status.md)
  Current project state: completed work, latest coverage, active risks, and next priorities.
- [docs/reports/v1.6.3_data_enhancement_report.md](docs/reports/v1.6.3_data_enhancement_report.md)
  Phase-2 quantum handoff report: xTB Hessian and CREST accepted outputs, ORCA manifest-only boundary, and verification scope.
- [docs/reports/v1.6.3_residual_compute_completion_summary.md](docs/reports/v1.6.3_residual_compute_completion_summary.md)
  Residual computation completion record: Psi4 DFT manifest reconciliation, xTB Hessian retries, CoolProp cycle successes, and remaining ORCA/REFPROP blockers.
- [docs/operations/local_large_artifacts.md](docs/operations/local_large_artifacts.md)
  Local-only large artifact policy for PubChem bulk files and executor artifact directories.
- [docs/overview/wave2_implementation.md](docs/overview/wave2_implementation.md)
  Wave 2 implementation notes: coverage tiers, new tables, source adapters, validation targets, and current boundaries.
- [docs/overview/project_scope.md](docs/overview/project_scope.md)
  Project scope and inclusion boundaries: what is in-scope, what is excluded, and the overall V1/Wave 2 target definition.
- [docs/contracts/data_contract.md](docs/contracts/data_contract.md)
  Data contract reference: core tables, field expectations, layered storage rules, and schema-level conventions.
- [docs/contracts/controlled_vocabularies.md](docs/contracts/controlled_vocabularies.md)
  Controlled vocabularies and enumerations used across properties, source types, quality levels, and related fields.
- [docs/contracts/phase2_interfaces.md](docs/contracts/phase2_interfaces.md)
  Reserved Phase 2 interfaces for quantum calculations, cycle simulation, and active-learning feedback extensions.
- [docs/plans/p0/p0_scope_and_exit_criteria.md](docs/plans/p0/p0_scope_and_exit_criteria.md)
  P0 scope, exit criteria, condition migration strategy, and PR-A/B/C split.
- [docs/reviews/p0_review_response_matrix.md](docs/reviews/p0_review_response_matrix.md)
  Response matrix for the 24 P0 review findings addressed by the draft package.
- [docs/plans/p0/p0_remaining_backlog.md](docs/plans/p0/p0_remaining_backlog.md)
  Remaining P0/V1.5 follow-up work after PR-A through PR-H, including coverage enrichment, mixture fraction enrichment, active-learning nomination policy, and CI expansion.
- [docs/handoffs/p0_change_log_and_handoff.md](docs/handoffs/p0_change_log_and_handoff.md)
  Handoff index for current modification progress, unfinished work, plan/review files, implementation entry points, outputs, and verification commands.
