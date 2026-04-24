# Current Status

## Snapshot

Status date: `2026-04-24`

R-PhysGen-DB has a green build/validate/test baseline after PR-A through PR-H of the P0 review response. The governance bundle remains mirrored and fused into canonical property outputs, while the production build now also includes staged orchestration, condition sets, research-task readiness, structured cycle operating points, deterministic screening proxy features, offline quantum pilot ingestion, governed mixture tables, and active-learning queue outputs.

Latest local rebuild summary:

- curated base inventory in `data/raw/manual/seed_catalog.csv`: `5700` rows = `70` refrigerants + `5630` candidates
- effective build inventory after appending generated governance seeds: `5707` rows = `77` refrigerants + `5630` candidates
- `resolved_molecule_count`: `5598`
- `property_observation`: `15707`
- `property_observation_canonical`: `1687`
- `property_recommended`: `14035`
- `property_recommended_canonical`: `1389`
- `property_recommended_canonical_strict`: `1304`
- `property_recommended_canonical_review_queue`: `0`
- `regulatory_status`: `96`
- `pending_sources`: `47`
- `qc_issue_count`: `10`
- `observation_condition_set`: `103`
- `cycle_case`: `2`
- `cycle_operating_point`: `2`
- `mixture_core`: `123`
- `mixture_composition`: `378`
- `quantum_job`: `0` when `data/raw/manual/quantum_pilot_results.csv` is absent
- `quantum_artifact`: `0` when `data/raw/manual/quantum_pilot_results.csv` is absent
- `active_learning_queue`: `0` when `data/raw/manual/active_learning_queue.csv` is absent
- `active_learning_decision_log`: `0` when `data/raw/manual/active_learning_decision_log.csv` is absent
- `data/gold/VERSION`: `v1.5.0-draft`
- `research_task_readiness_report`: `5`
- `proxy_feature_observation`: `11196`
- split counts: `train=84`, `validation=18`, `test=18`
- extension mirror: `218/218` tables aligned; `mixture_core=123`; `mixture_component=378`
- bundle crosswalk: `matched_existing=128`, `generated_new_seed=7`, `unresolved=0`, `external_resolution_count=2`

Latest validation state:

- `pipelines/build_v1_dataset.py`: passes
- `pipelines/validate_v1_dataset.py`: passes
- `.venv\Scripts\pytest.exe -q`: `101 passed`
- current validation errors: none

Latest P0 additions:

- condition migration coverage: `15707/15707` rows with `condition_set_id`; `coverage_fraction=1.0`; `auto_backfilled=13910`; `needs_manual_review=1797`
- cycle rows: `398` resolved observations across `2` operating points
- proxy rows: `5598` `tfa_risk_proxy` + `5598` `synthetic_accessibility`
- proxy TFA distribution: `none=2801`, `low=1785`, `medium=606`, `high=393`, `unknown=13`
- readiness summary: `2 passed`, `3 failed`
- mixture summary: `123` governed blends, `378` component rows, `1` unresolved fraction group (`MIX_511A`) retained without imputation
- quantum pilot summary: `not_configured` unless optional offline CSV is supplied
- active learning summary: `not_configured` unless optional manual queue or decision-log CSV input is supplied

## Completed

### Governance bundle alignment

- The bundle `methods/refrigerant_seed_database_20260422_property_governance_bundle.zip` is mirrored into `data/extensions/property_governance_20260422/tables/*.parquet` with `218/218` tables, matching row counts, and preserved column order.
- DuckDB now registers bundle tables under `property_governance_ext.*` and normalized extension tables under `extensions.*`.
- `data/raw/manual/property_governance_20260422_unresolved_curations.csv` is applied before bundle crosswalking and is validated as a high-confidence, single-structure override input.
- `data/raw/manual/property_governance_20260422_canonical_review_decisions.csv` is applied after canonical recommendation selection and now supports both `accept_selected_source` and `accept_out_of_strict`, closing reviewed queue rows without mutating the selected canonical value.
- `data/raw/manual/property_governance_20260422_proxy_acceptance_rules.csv` is applied during strict filtering and allows explicitly approved proxy-only feature/source combinations into the ML-facing canonical strict layer.
- The two previously unresolved bundle substances, `SUB092 / R-C316 / CAS 356-18-3` and `SUB098 / R-E245ca1 / CAS 69948-24-9`, are now resolved through explicit external curation.
- The `1` top-rank conflict row and `34` source-divergence rows identified in the second-round review have now been adjudicated and removed from the open review queue.
- The `396` previously open `proxy_selected` rows are now absorbed into `property_recommended_canonical_strict` through `13` feature/source proxy-only policy rules, while preserving proxy provenance on the canonical recommendation layer.
- The `4` `environmental.ozone_depleting_flag` rows with governed `No` text values are now normalized to numeric `0.0` in the canonical overlay and enter the strict layer.
- The remaining `85` strict-rejection queue rows have been explicitly adjudicated as `accept_out_of_strict`, so the open canonical review queue is now `0` while low-quality or qualitative rows remain visible in `property_recommended_canonical`.
- Canonical property outputs are materialized as:
  - `data/silver/property_observation_canonical.parquet`
  - `data/gold/property_recommended_canonical.parquet`
  - `data/gold/property_recommended_canonical_strict.parquet`
- Legacy projection behavior stays intentionally narrow. Only the existing active canonical keys flow back into the legacy `property_name` tables.

### Inventory and promotion behavior

- `data/raw/manual/seed_catalog.csv` remains the authoritative curated base catalog.
- `data/raw/generated/property_governance_20260422_seed_catalog.csv` appends `7` build-time `Tier D` refrigerant rows sourced from the governance bundle.
- Those `7` generated bundle seeds remain `model_inclusion=no`, so they do not automatically enter `model_dataset_index` or `model_ready`.
- The promoted model subset remains the same `Tier A/B/C` block with `120` rows.

### Existing expansion paths retained

- The PubChem bulk candidate intake remains active and still feeds the first-pass `Tier D` candidate pool.
- The Excel 202603 supplement remains active through `data/raw/manual/observations/excel_202603_observations.csv` and its generated staging/candidate artifacts.
- Validation now covers schema integrity, inventory completeness, quality gates, canonical strict-filtering rules, and property-governance extension mirror checks in one report.

### P0 review-response production path

- Stage orchestration writes `data/bronze/stage_run_manifest.parquet` and preserves `build_dataset()` as the public entrypoint.
- `data/silver/observation_condition_set.parquet` is generated and linked from every `property_observation` row.
- Cycle outputs write `data/silver/cycle_case.parquet` and `data/silver/cycle_operating_point.parquet`.
- Proxy screening rows are generated from local structure heuristics, traced through `source_r_physgen_proxy_heuristics`, recommended in long form, and blocked from wide ML outputs.
- Quantum pilot rows are loaded only from optional `data/raw/manual/quantum_pilot_results.csv`; without that file the build writes empty quantum silver tables and reports `not_configured`.
- Mixture outputs write `data/silver/mixture_core.parquet` and `data/silver/mixture_composition.parquet` from the governance extension mirror; missing component fractions are recorded rather than imputed.
- Active learning outputs write `data/gold/active_learning_queue.parquet` and `data/gold/active_learning_decision_log.parquet`; without optional manual queue/decision-log CSV input they are empty and do not affect recommendations.
- `data/gold/VERSION` is written and checked against `quality_report.dataset_version`.
- `data/gold/research_task_readiness_report.parquet` and `validation_report.json` now carry task-level readiness status.

## Current Coverage

### Promoted subset

- `Tier A`: `32`
- `Tier B`: `48`
- `Tier C`: `40`
- promoted model subset size: `120`

### Effective inventory-only tail

- effective `Tier D` rows: `5587`
- `Tier D` candidate rows: `5575`
- `Tier D` refrigerant rows: `12`
- `Tier D` molecules currently resolved into `molecule_core`: `5478`

### Canonical layer

- canonical recommended feature keys: `30`
- strict ML-ready canonical feature keys: `24`
- proxy/screening-selected rows still visible in `property_recommended_canonical`: `466`
- proxy-only selected rows: `466`
- top-rank conflict provenance flags still visible in `property_recommended_canonical`: `1`
- lower-priority source-divergence provenance flags still visible in `property_recommended_canonical`: `35`
- open top-rank conflict rows in `property_recommended_canonical_review_queue`: `0`
- open source-divergence rows in `property_recommended_canonical_review_queue`: `0`
- open proxy-selected rows in `property_recommended_canonical_review_queue`: `0`
- canonical review decision rows applied: `120`
- `accept_selected_source` review decisions applied: `35`
- `accept_out_of_strict` review decisions applied: `85`
- proxy-only policy rules applied: `13`
- strict rows accepted through proxy-only policy: `396`
- canonical review queue rows: `0`
- bundle unresolved substances after curation: `0`

### Key label coverage on the promoted subset

- `Tier A`
  - `gwp_100yr`: `0.9688`
  - `odp`: `1.0000`
  - `ashrae_safety`: `0.9063`
  - `toxicity_class`: `0.9063`
- `Tier B`
  - `gwp_100yr`: `0.5625`
  - `odp`: `0.5833`
  - `ashrae_safety`: `0.5208`
  - `toxicity_class`: `0.5000`
- `Tier C`
  - `gwp_100yr`: `0.6750`
  - `odp`: `0.4250`
  - `ashrae_safety`: `0.1750`
  - `toxicity_class`: `0.1750`

## Known Issues

### Upstream source limitations

- Several NIST phase pages still return `No tables found`, including `R-1234yf`, `R-1234ze(E)`, `R-1234ze(Z)`, `R-1233zd(E)`, `R-1336mzz(E)`, and `Novec 649`.
- Some NIST snapshots parse but yield `0 parsed observations`, including `deuterium`, `methyl linolenate`, and `neon`.
- Some CoolProp fluids still fail the current standard-cycle setup because the configured operating point falls below supported limits, including `D4`, `methyl palmitate`, `methyl stearate`, and `p-xylene`.

### Coverage limitations

- `Tier B` environmental and safety labels are still incomplete.
- `Tier C` and `Tier D` safety coverage remain sparse.
- Bulk PubChem `Tier D` rows are still intentionally structure-first in the initial pass, so most of them do not yet carry environmental or safety labels.
- `pending_sources` remains non-zero because the optional CompTox line is not configured with an API key.
- The general canonical recommendation layer still contains proxy-only rows by design, and it preserves conflict/source-divergence provenance flags even after manual closure decisions and proxy-only strict acceptance; ML-facing work should use `property_recommended_canonical_strict`, while low-quality or qualitative rows intentionally kept out of strict are documented in `data/raw/manual/property_governance_20260422_canonical_review_decisions.csv`.

## Next Priorities

1. Coverage-driven enrichment for GWP/ODP/safety/strict/cycle readiness gaps.
2. Traceable mixture composition enrichment for unresolved fraction rows such as `MIX_511A`.
3. Active learning automatic nomination policy after campaign/scoring ownership is defined.
4. Promote the new manual build/validate workflow into a regular gate after artifact storage/runtime policy is settled.

## Core Output Files

- `data/gold/quality_report.json`
- `data/gold/validation_report.json`
- `data/bronze/property_governance_20260422_audit.json`
- `data/bronze/property_governance_20260422_substance_crosswalk.parquet`
- `data/raw/generated/property_governance_20260422_seed_catalog.csv`
- `data/raw/manual/property_governance_20260422_unresolved_curations.csv`
- `data/raw/manual/property_governance_20260422_canonical_review_decisions.csv`
- `data/raw/manual/property_governance_20260422_proxy_acceptance_rules.csv`
- `data/silver/property_observation_canonical.parquet`
- `data/silver/mixture_core.parquet`
- `data/silver/mixture_composition.parquet`
- `data/gold/property_recommended_canonical.parquet`
- `data/gold/property_recommended_canonical_strict.parquet`
- `data/gold/property_recommended_canonical_review_queue.parquet`
- `data/gold/active_learning_queue.parquet`
- `data/gold/active_learning_decision_log.parquet`
- `data/gold/VERSION`
- `data/extensions/property_governance_20260422/`
- `data/index/r_physgen_v2.duckdb`
