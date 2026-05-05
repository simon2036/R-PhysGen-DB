# P0 Change Log And Handoff

Status date: `2026-04-24`

This document is the handoff index for the current P0 review-response work. It records what has been implemented, what remains unfinished, where the modification plans and review artifacts live, which files are the main implementation entry points, and how the current state was verified.

## Current Progress

The local working tree has advanced from the imported P0 review package through PR-A to PR-H.

- PR-A completed: P0 documents, draft schemas/configs, stage blueprint, and helper scripts from the review package were brought into the repo.
- PR-B completed: `build_dataset()` remains the public entrypoint but now runs through the staged production orchestrator.
- PR-C completed: production condition sets, nullable observation extensions, and research-task readiness validation are connected to build/validate.
- PR-D completed: CoolProp cycle rows now carry `cycle_case_id`, `operating_point_hash`, and `condition_set_id`; `cycle_case` and `cycle_operating_point` are production silver tables.
- PR-E completed: deterministic TFA-risk and synthetic-accessibility screening proxies are generated, traced, validated, and kept out of wide ML outputs.
- PR-F completed: optional offline quantum pilot CSV ingestion is connected; missing CSV writes empty quantum silver tables and does not fail the build.
- PR-G completed: governance mixture extension tables are promoted to production `mixture_core` and `mixture_composition` silver tables.
- PR-H completed: active-learning queue and decision-log outputs are production gold tables with optional manual CSV ingestion.
- P0 gap pass completed: staged resume artifacts, Phase 2 interface placeholders, readiness filters, condition review counts, mixture curation gating, safety-label enrichment, and two-tier CI are now wired.
- Reproducibility completed for this round: `data/lake/gold/VERSION` is written and checked against `quality_report.dataset_version`; targeted CI contract tests and manual build/validate workflow dispatch are defined.

Latest verified output counts:

- `property_observation`: `15707`
- `property_recommended`: `14035`
- `observation_condition_set`: `103`
- `cycle_case`: `2`
- `cycle_operating_point`: `2`
- `quantum_job`: `0` when `data/lake/raw/manual/quantum_pilot_results.csv` is absent
- `quantum_artifact`: `0` when `data/lake/raw/manual/quantum_pilot_results.csv` is absent
- `mixture_core`: `123`
- `mixture_composition`: `378`
- `active_learning_queue`: `0` when `data/lake/raw/manual/active_learning_queue.csv` is absent
- `active_learning_decision_log`: `0` when `data/lake/raw/manual/active_learning_decision_log.csv` is absent
- `data/lake/gold/VERSION`: `v1.5.0-draft`

## Modification Plan And Review Files

Authoritative review inputs:

- `docs/reviews/R-PhysGen-DB_P0_review.md`
- `archive/2026-05-05/methods/R-PhysGen-DB_P0_package_v3.zip`
- `archive/2026-05-05/methods/R-PhysGen-DB_P0_package/`

Accepted P0 planning and response documents:

- `docs/plans/p0/p0_scope_and_exit_criteria.md`: P0 scope, exit criteria, and post-P0 priority order.
- `docs/reviews/p0_review_response_matrix.md`: response matrix for the 24 review findings.
- `docs/plans/p0/p0_pipeline_stage_refactor_plan.md`: PR-B staged-pipeline refactor plan.
- `docs/plans/p0/p0_phase2_interfaces_draft.md`: Phase 2 interface draft for quantum, cycle, and active learning.
- `docs/plans/p0/p0_validation_rules_draft.md`: draft validation and readiness rules.
- `docs/plans/p0/p0_remaining_backlog.md`: current remaining backlog after PR-A through PR-H.
- `docs/overview/current_status.md`: latest build/validate/test status and output counts.
- `docs/handoffs/p0_change_log_and_handoff.md`: this handoff index.

Draft and production schema/config locations:

- `schemas/drafts/`: retained draft schemas/configs imported from PR-A.
- `schemas/canonical_feature_registry.yaml`
- `schemas/normalization_rules.yaml`
- `schemas/observation_condition_set.yaml`
- `schemas/research_task_readiness_rules.yaml`
- `schemas/cycle_case.yaml`
- `schemas/cycle_operating_point.yaml`
- `schemas/quantum_job.yaml`
- `schemas/quantum_artifact.yaml`
- `schemas/mixture_core.yaml`
- `schemas/mixture_composition.yaml`
- `schemas/active_learning_queue.yaml`
- `schemas/active_learning_decision_log.yaml`

Helper scripts and CI:

- `scripts/backfill_condition_set.py`
- `scripts/pr_b_equivalence_check.py`
- `.github/workflows/ci.yml`

## Implementation Entry Points

Public interfaces remain stable:

- `from r_physgen_db.pipeline import build_dataset`
- `build_dataset(refresh_remote: bool = False) -> dict`
- `validate_dataset()` keeps the existing call pattern.

Main production modules added or extended:

- `src/r_physgen_db/pipeline_stages/`: staged build framework and stage functions.
- `src/r_physgen_db/condition_sets.py`: stable `condition_set_id` backfill and condition table generation.
- `src/r_physgen_db/readiness.py`: research task readiness report generation with explicit entity/model/tier filters.
- `src/r_physgen_db/cycle_conditions.py`: structured cycle operating point tables and hashes.
- `src/r_physgen_db/proxy_features.py`: deterministic screening proxy observations.
- `src/r_physgen_db/quantum_pilot.py`: optional offline quantum pilot ingestion.
- `src/r_physgen_db/mixtures.py`: governance mixture extension promotion plus traceable manual fraction-curation gating.
- `src/r_physgen_db/active_learning.py`: optional active-learning queue and decision-log CSV ingestion and empty default outputs.
- `src/r_physgen_db/validate.py`: production validation for condition sets, cycle, proxy, quantum, mixture, active learning, readiness, and dataset version.
- `src/r_physgen_db/interfaces.py`: Phase 2 dataclass placeholders aligned with P0 drafts.

Generated production outputs added during P0:

- `data/lake/bronze/stage_run_manifest.parquet`
- `data/lake/silver/observation_condition_set.parquet`
- `data/lake/silver/cycle_case.parquet`
- `data/lake/silver/cycle_operating_point.parquet`
- `data/lake/silver/quantum_job.parquet`
- `data/lake/silver/quantum_artifact.parquet`
- `data/lake/silver/mixture_core.parquet`
- `data/lake/silver/mixture_composition.parquet`
- `data/lake/gold/research_task_readiness_report.parquet`
- `data/lake/gold/active_learning_queue.parquet`
- `data/lake/gold/active_learning_decision_log.parquet`
- `data/lake/gold/VERSION`

## Remaining Work

The build and validation baseline is green, but these items remain open by design:

- Coverage-driven enrichment: readiness still reports `task01_single_component_downselection`, `task03_canonical_strict_ml`, and `task04_phase2_cycle_seed` as failed. `task02_core_multitask_training` now passes for the promoted Tier A/B/C subset after safety-label enrichment.
- Mixture fraction enrichment: `MIX_511A` has component rows but lacks source `mass_pct`; it is retained as unresolved and not imputed.
- Active learning candidate generation: tables and optional CSV ingestion exist, but automatic queue generation is not enabled until scoring policy and campaign ownership are defined.
- CI expansion: targeted PR contract tests are wired, and full build/validate is available through `workflow_dispatch`; making it a regular PR gate is deferred until generated artifact storage/runtime policy is settled.
- Data-source enrichment: public/manual GWP, ODP, safety, strict canonical, transport, and cycle coverage still need traceable sources before the remaining failed readiness tasks can move materially.

Current readiness status:

- `task01_single_component_downselection`: failed.
- `task02_core_multitask_training`: passed.
- `task03_canonical_strict_ml`: failed.
- `task04_phase2_cycle_seed`: failed.
- `task05_proxy_screening_seed`: passed.

## Verification Record

Latest commands run successfully:

```powershell
.venv\Scripts\python.exe pipelines\build_v1_dataset.py
.venv\Scripts\python.exe pipelines\validate_v1_dataset.py
.venv\Scripts\pytest.exe -q
```

Observed results:

- Build completed successfully.
- Validation completed successfully with `errors=[]`.
- Full test suite completed with `101 passed`.
- `quality_report.json` includes `mixture_summary`, `active_learning_summary`, `quantum_pilot_summary`, `condition_migration_progress`, `cycle_operating_point_summary`, `proxy_feature_summary`, and `research_task_readiness`.

Operational note:

- This repo is under Baidu Sync. Parquet files can be temporarily locked after build or validation. The stage manifest writer now retries and, if the sync client keeps the parquet locked, falls back to a run-specific JSONL manifest instead of failing the build.

## Boundary Rules For Future Work

- Do not change `build_dataset()` or `validate_dataset()` public call patterns unless explicitly scoped.
- Do not add proxy, quantum, mixture, or active-learning columns to `property_matrix`, `model_ready`, or `model_dataset_index` without a new explicit PR scope.
- Do not impute missing mixture fractions or quantum values.
- Keep every new evidence row traceable through source metadata, source IDs, and validation checks.
- Keep generated data rewrites separate from logic-only review when possible because the sync client may lock Parquet/DuckDB files.
