# Current Status

## Snapshot

Status date: `2026-05-05`
Dataset version: `v1.6.3-draft`

R-PhysGen-DB is at the v1.6.3 phase-2 quantum handoff draft plus residual computation pass and the 2026-05-01 SupplementData integration. The build preserves the v1.6.2 governance xTB/Psi4 scalar baseline, adds accepted phase-2 xTB Hessian and CREST conformer summaries, reconciles completed Psi4 DFT manifest rows, ingests the CoolProp cycle rows that can be resolved locally, records ORCA opt/freq as blocked on a real ORCA QC executable, integrates strict ASHRAE/EPA supplement observations, corrects the R-511A component/fraction curation, and keeps all quantum descriptors restricted to long-form observations plus `quantum_job`/`quantum_artifact` audit tables. No GWP/safety, mixture, xTB, CREST, Psi4, ORCA, or cycle values were fabricated.

Latest local rebuild/validation summary:

- `validation_report.errors`: `[]`
- readiness summary: `5 passed`, `0 failed`, `0 degraded`
- `seed_catalog_count`: `20707` = `77` refrigerants + `20630` candidates
- `resolved_molecule_count`: `20567`
- `property_observation`: `60840`
- `property_recommended`: `56784`
- `property_recommended_canonical`: `57188`
- `property_recommended_canonical_strict`: `2553`
- `model_dataset_index`: `120`
- `model_ready`: `120` (`train=84`, `validation=18`, `test=18`)
- `source_manifest`: `21208`
- `observation_condition_set`: `110`
- `cycle_case`: `7`
- `cycle_operating_point`: `6`
- `active_learning_queue`: `2088`
- `quantum_job`: `2367` (`2365` succeeded, `2` failed)
- `quantum_artifact`: `2367`
- `quantum_observation`: `13822`
- `data/lake/gold/VERSION`: `v1.6.3-draft`

## Production Computation State

### Active-learning, governance pregeometry, and xTB

- Generated active-learning queue: `2088` rows.
- Action mix: `run_quantum=2000`, `manual_curation=39`, `literature_search=32`, `run_cycle=17`.
- Queue status mix: `completed=2000`, `proposed=88`.
- Active-learning xTB request manifest: `2000` requests, `2000` XYZ inputs, `selection_source=active_learning_queue`, all represented as `completed` because matching xTB results are already ingested.
- Standard xTB scalar execution represented in `quantum_job`: `2038` GFN2-xTB jobs succeeded and `0` failed.
- Standard xTB scalar observations: `12228` rows = `2038` jobs × `6` features.

### Psi4/DFT phase-1 scalar scope

- Baseline/governance Psi4 execution represented in `quantum_job`: `205` B3LYP-D3BJ/def2-SVP jobs succeeded and `0` failed.
- DFT scalar observations: `1230` rows = `205` jobs × `6` features.
- The current general DFT request manifest has `150` requests, all marked `completed` from existing Psi4 results (`900` scalar rows); the retry runner resumed with no new work.
- Phase-1 DFT scalar scope remains `total_energy`, `HOMO`, `LUMO`, `gap`, `dipole`, and `polarizability`.

### Phase-2 governance quantum handoff

- Phase-2 mapped governance molecules: `62`.
- xTB Hessian requests: `62` attempted.
- xTB Hessian acceptance: `60` succeeded and produced accepted observations; `2` were retried and still reproduce `imaginary_frequency_count=1`, so they remain audit-visible failed entries in `quantum_job`/`quantum_artifact`.
- Accepted xTB Hessian scalar rows: `240` = `60` accepted jobs × `4` features (`zpe`, `lowest_real_frequency`, `thermal_enthalpy_correction`, `thermal_gibbs_correction`).
- CREST conformer summaries: `62` succeeded, producing `124` scalar observations (`conformer_count`, `conformer_energy_window`).
- ORCA opt/freq manifests: `3` smoke requests plus `59` full requests in `3` batches; ORCA was not executed because no ORCA quantum-chemistry executable is configured, and no ORCA rows were ingested.
- Local executor bundles under `data/lake/raw/manual/quantum_phase2_artifacts/` are `~16 GiB`, ignored by Git, and documented in `docs/operations/local_large_artifacts.md`.

### Governance DFT backlog batching

- Governance DFT backlog: `1350` rows = `135` substances × `10` requested high-level QM outputs.
- Current mapping: `62` mapped governance substances and `73` unmapped substances preserved in `data/lake/raw/generated/governance_dft_mapping_report.csv` and `data/lake/raw/generated/governance_phase2_mapping_report.csv`.
- v1.6.2 phase-1 execution reached `62/62` mapped governance molecules with xTB coverage and `62/62` with Psi4/B3LYP-D3BJ/def2-SVP scalar coverage.
- v1.6.3 phase-2 execution adds accepted xTB Hessian and CREST conformer descriptors for the mapped governance set without widening model tables.

### CoolProp multi-condition cycles

- Mapped supported CoolProp fluids: `114`.
- Cycle observations: `1384` resolved rows.
- Cycle cases / operating points: `7` cases and `6` operating points.
- Cycle property coverage includes `cop_standard_cycle`, `volumetric_cooling_mjm3`, `pressure_ratio`, and `discharge_temperature_c` for `96` molecules in the recommended layer.
- Residual cycle backend pass: `21` `run_cycle` queue entries attempted with backend `auto`; `4` CoolProp successes wrote `16` rows to `data/lake/raw/manual/cycle_backend_results.csv`, while `17` entries remain blocked on REFPROP/root/backend coverage.

## Current Coverage and Boundaries

- Promoted subset size remains `120` (`Tier A=32`, `Tier B=48`, `Tier C=40`).
- Effective resolved Tier D tail remains `20447` molecules; Tier D seed rows remain `model_inclusion=no`.
- Quantum descriptors remain long-form observations only and are excluded from `property_matrix`, `model_ready`, and `model_dataset_index`.
- Phase-2 xTB Hessian rows with imaginary frequencies are failed audit rows, not accepted property observations.
- ORCA opt/freq remains manifest-only until a real ORCA quantum-chemistry executable is configured and a separate execution/ingestion pass is run and verified.
- `MIX_511A` remains unresolved for composition/fraction evidence; no source-free blend estimate was added.
- GWP100 and safety coverage still have readiness warnings where exact auditable sources are absent.
- Optional CompTox remains unconfigured, so `pending_sources` stays non-zero (`47`).

## Core Output Files

- `data/lake/gold/quality_report.json`
- `data/lake/gold/validation_report.json`
- `data/lake/bronze/stage_run_manifest.parquet`
- `data/lake/raw/generated/active_learning_queue.csv`
- `data/lake/raw/generated/quantum_pilot_requests.csv`
- `data/lake/raw/generated/quantum_pilot_xyz_manifest.csv`
- `data/lake/raw/generated/quantum_dft_requests.csv`
- `data/lake/raw/generated/quantum_dft_xyz_manifest.csv`
- `data/lake/raw/generated/governance_xtb_requests_batch001.csv`
- `data/lake/raw/generated/governance_xtb_xyz_manifest_batch001.csv`
- `data/lake/raw/generated/governance_dft_singlepoint_requests_batch001.csv`
- `data/lake/raw/generated/governance_dft_singlepoint_requests_batch002.csv`
- `data/lake/raw/generated/governance_dft_singlepoint_xyz_manifest_batch001.csv`
- `data/lake/raw/generated/governance_dft_singlepoint_xyz_manifest_batch002.csv`
- `data/lake/raw/generated/governance_dft_mapping_report.csv`
- `data/lake/raw/generated/governance_dft_mapping_summary.json`
- `data/lake/raw/generated/governance_phase2_summary.json`
- `data/lake/raw/generated/governance_phase2_mapping_report.csv`
- `data/lake/raw/generated/governance_phase2_blockers.csv`
- `data/lake/raw/generated/governance_phase2_xtb_hessian_requests.csv`
- `data/lake/raw/generated/governance_phase2_xtb_hessian_xyz_manifest.csv`
- `data/lake/raw/generated/governance_phase2_crest_requests.csv`
- `data/lake/raw/generated/governance_phase2_crest_xyz_manifest.csv`
- `data/lake/raw/generated/governance_phase2_orca_optfreq_smoke_requests.csv`
- `data/lake/raw/generated/governance_phase2_orca_optfreq_smoke_xyz_manifest.csv`
- `data/lake/raw/generated/governance_phase2_orca_optfreq_batch*.csv`
- `data/lake/raw/generated/governance_phase2_orca_optfreq_xyz_manifest_batch*.csv`
- `data/lake/raw/manual/quantum_pilot_results.csv`
- `data/lake/raw/manual/cycle_backend_results.csv`
- `data/lake/raw/manual/quantum_phase2_vibrational_modes.csv`
- `data/lake/raw/manual/quantum_phase2_conformer_ensemble.csv`
- `data/lake/silver/quantum_job.parquet`
- `data/lake/silver/quantum_artifact.parquet`
- `data/lake/silver/cycle_case.parquet`
- `data/lake/silver/cycle_operating_point.parquet`
- `data/lake/gold/active_learning_queue.parquet`
- `data/lake/gold/research_task_readiness_report.parquet`
- `data/lake/gold/VERSION`
- `data/indexes/r_physgen_v2.duckdb`
