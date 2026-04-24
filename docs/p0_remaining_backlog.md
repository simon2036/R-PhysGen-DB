# P0 Remaining Backlog

Status date: `2026-04-24`

This backlog records the remaining work after PR-A through PR-H. It separates review-response items already landed in production from items that are still draft-only, reserved, or intentionally out of the current build path.

## Completed

- PR-A: P0 review documents, draft schemas/configs, helper scripts, and staged-pipeline blueprint were imported from the review package.
- PR-B: `build_dataset()` now runs through the production stage orchestrator while preserving the public facade and current build entrypoints.
- PR-C: `condition_set_id`, `observation_condition_set`, nullable observation extensions, condition migration reporting, and research task readiness validation are in the production build/validate path.
- PR-D: CoolProp cycle rows now carry structured `cycle_case_id`, `operating_point_hash`, `condition_set_id`, and minimal extra metrics; `cycle_case` and `cycle_operating_point` are written as silver tables.
- PR-E: deterministic TFA-risk and synthetic-accessibility screening proxies are generated for resolved pure molecules, traced through source manifest, reported in quality/readiness/validation, and kept out of wide ML outputs.
- PR-F: optional offline quantum pilot CSV ingestion is wired into long-form observations and quantum silver tables; missing CSV writes empty quantum tables without failing the build.
- PR-G: governance mixture extension tables are promoted into production silver `mixture_core` and `mixture_composition` tables with validation and reporting.
- PR-H: active learning queue and decision-log tables are production outputs with optional manual CSV ingestion; missing CSV writes empty gold tables.

## Remaining Items

1. Coverage-driven enrichment
   - Status: validation passes, but readiness shows coverage gaps.
   - Target: improve public GWP/ODP/safety/strict/cycle coverage enough to move failed/degraded readiness tasks toward passed.
   - Boundary: prefer traceable public/manual sources over proxy promotion for strict ML-facing use.

2. Mixture data enrichment
   - Status: production tables exist, but one governed blend (`MIX_511A`) lacks source mass percentages.
   - Target: add traceable source composition for unresolved mixture fraction rows.
   - Boundary: do not impute blend composition without an auditable source.

3. Active learning candidate generation
   - Status: queue/decision-log tables and optional CSV ingestion exist, but automatic candidate nomination is intentionally not enabled.
   - Target: define acquisition strategy, scoring policy, and campaign ownership before generating queue entries automatically.
   - Boundary: queue entries must not directly mutate recommendations or `model_ready`; new evidence must re-enter through source/observation governance.

4. CI expansion
   - Status: targeted contract tests and PR-B equivalence checker fixtures are wired into CI.
   - Target: add a controlled build/validate job once CI storage/runtime for generated Parquet artifacts is settled.
   - Boundary: generated Parquet/DuckDB artifacts should remain controlled explicitly and not be rewritten by lint-only jobs.

## Current Readiness Gap

The PR-H validation baseline has no validation errors. The remaining readiness failures are data coverage limitations, not schema or pipeline failures:

- `task01_single_component_downselection`: failed.
- `task02_core_multitask_training`: degraded.
- `task03_canonical_strict_ml`: failed.
- `task04_phase2_cycle_seed`: failed.
- `task05_proxy_screening_seed`: passed.

## Acceptance Rule For Future PRs

Each follow-up PR must preserve the existing public build/validate interfaces, keep new evidence traceable through `source_manifest`, keep `property_matrix` and `model_ready` unchanged unless explicitly scoped, and add validation checks that fail on untraceable or policy-violating rows.
