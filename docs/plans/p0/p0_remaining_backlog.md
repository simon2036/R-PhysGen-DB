# P0 Remaining Backlog

Status date: `2026-04-29`
Dataset version: `v1.6.0-draft`

This backlog reflects the current repository-local v1.6 baseline after the data enhancement pass. Build and validation pass, all five research-readiness rules are currently `passed`, and the remaining work is evidence/executor/backlog work rather than schema or pipeline unblockers.

## Completed In The Production Path

- PR-A: P0 review documents, draft schemas/configs, helper scripts, and staged-pipeline blueprint were imported from the review package.
- PR-B: `build_dataset()` runs through the production stage orchestrator while preserving the public facade and current build entrypoints.
- PR-C: `condition_set_id`, `observation_condition_set`, nullable observation extensions, condition migration reporting, and research task readiness validation are in the production build/validate path.
- PR-D: CoolProp cycle rows carry structured `cycle_case_id`, `operating_point_hash`, `condition_set_id`, and minimal extra metrics; `cycle_case` and `cycle_operating_point` are written as silver tables.
- PR-E: deterministic TFA-risk and synthetic-accessibility screening proxies are generated for resolved pure molecules, traced through source manifest, reported in quality/readiness/validation, and kept out of wide ML outputs.
- PR-F: offline quantum pilot CSV ingestion is wired into long-form observations and quantum silver tables; missing CSV still writes empty quantum tables without failing the build.
- PR-G: governance mixture extension tables are promoted into production silver `mixture_core` and `mixture_composition` tables with validation and reporting.
- PR-H: active learning queue and decision-log tables are production outputs with optional manual CSV ingestion.
- v1.6 Tier D expansion: effective inventory is `20707` seeds with `20567` resolved molecules and `20447` resolved Tier D molecules.
- v1.6 generated active-learning policy: absent a manual queue, the build writes a deterministic `250`-entry queue (`run_quantum=158`, `manual_curation=39`, `literature_search=32`, `run_cycle=21`).
- v1.6 quantum request routing/execution: `quantum_pilot_requests.csv` is selected from the highest-priority active-learning `run_quantum` entries; the current local run executed all `158` requests with xTB and ingested `948` scalar observations.
- v1.6 quantum artifact governance: xTB partial charges, WBO, stdout/stderr, JSON, optimized XYZ, and related files are retained in per-request artifacts and are not widened into ML tables.
- v1.6 validation/readiness: `validation_report.errors == []`; readiness summary is `5 passed`, `0 failed`, `0 degraded`.

## Remaining Items

1. Traceable GWP and safety enrichment
   - Status: validation/readiness pass, but promoted and Tier D coverage still contains warnings/gaps.
   - Current examples: Tier B `gwp_100yr=0.6042`, `ashrae_safety=0.5208`; Tier C `gwp_100yr=0.7000`, `ashrae_safety=0.1750`.
   - Target: add auditable public/manual sources for remaining GWP/safety values.
   - Boundary: do not promote proxies, estimates, or unsourced values into strict/model-ready data.

2. Mixture data enrichment
   - Status: production mixture tables exist and validate, but one governed blend (`MIX_511A`) still has unresolved source fractions.
   - Target: add traceable source composition for unresolved mixture fraction rows.
   - Boundary: do not impute or estimate blend composition without an auditable source.

3. Optional higher-fidelity quantum tier
   - Status: the current xTB pilot scope is executed and ingested (`158` succeeded jobs, `948` scalar observations).
   - Target: only add Psi4/DFT or higher-fidelity calculations if they are actually executed and artifacted.
   - Boundary: do not fabricate Psi4/DFT values, do not copy xTB values into DFT fields, and do not widen quantum descriptors into `property_matrix`, `model_ready`, or `model_dataset_index`.

4. Source backlog and external services
   - Status: `pending_sources=47`; optional CompTox remains unconfigured and some PubChem/NIST lookups still fail or parse no tables.
   - Target: resolve only with credentials, cached evidence, or public source snapshots that can be audited.
   - Boundary: keep failures visible in reports when authority/evidence is missing.

5. CI expansion
   - Status: targeted contract tests and build/validate commands work locally.
   - Target: add a controlled build/validate CI job once storage/runtime policy for generated Parquet/DuckDB artifacts is settled.
   - Boundary: generated Parquet/DuckDB artifacts should remain controlled explicitly and not be rewritten by lint-only jobs.

## Current Readiness State

The current v1.6 validation baseline has no validation errors. Research task readiness is no longer blocked, but warnings remain data-coverage signals for future enrichment:

- `task01_single_component_downselection`: passed, with safety coverage warning.
- `task02_core_multitask_training`: passed.
- `task03_canonical_strict_ml`: passed, with GWP/safety coverage warnings.
- `task04_phase2_cycle_seed`: passed.
- `task05_proxy_screening_seed`: passed.

## Acceptance Rule For Future PRs

Each follow-up PR must preserve existing public build/validate interfaces, keep new evidence traceable through `source_manifest`, keep `property_matrix` and `model_ready` unchanged unless explicitly scoped, and add validation checks that fail on untraceable or policy-violating rows.
