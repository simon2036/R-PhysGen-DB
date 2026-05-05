# Data Contract

## Layering

- `raw`
  Original downloaded or manually curated artifacts, including the inventory catalog and manual observation batches.
- `bronze`
  Source inventory, crosswalks, fetch status, and human-readable audits.
- `silver`
  Normalized entities and long-form observations.
- `gold`
  Recommended labels, dictionaries, rules, reports, and model-facing tables.
- `extensions`
  Fidelity-preserving mirrors of externally governed table bundles plus small normalized convenience tables derived from them.

## Schema Evolution

Dataset schema and contract changes are governed by [`dataset_migration_spec.md`](dataset_migration_spec.md).

Key rules:

- Parquet files are the authoritative generated table artifacts.
- DuckDB files under `data/indexes/` are rebuildable query indexes, not the primary storage layer.
- Every data-contract, output-schema, dataset-version, or validation-rule change must have a migration record in `docs/migrations/dataset/`.
- Traditional database migrations are reserved for a future service database such as PostgreSQL; they do not apply to the current Parquet/DuckDB dataset layer.

## Inventory Catalog

`data/lake/raw/manual/seed_catalog.csv` is the authoritative curated inventory catalog.

Key fields:

- `seed_id`
- `r_number`
- `family`
- `coverage_tier`
- `selection_role`
- `entity_scope`
- `model_inclusion`
- `source_bundle`
- `coolprop_support_expected`
- `regulatory_priority`

Build-time supplements:

- `data/lake/raw/generated/property_governance_20260422_seed_catalog.csv`
  Generated `seed_catalog`-compatible rows for bundle-only refrigerants discovered in the `2026-04-22` property-governance bundle. This file is appended during dataset builds; it does not replace the curated base catalog.

Interpretation:

- `Tier A/B/C` rows with `model_inclusion=yes` are the promoted subset.
- `Tier D` rows with `model_inclusion=no` remain inventory-only until promoted.
- `source_bundle=pubchem_bulk` is reserved for generated `Tier D` candidate rows sourced from the PubChem FTP bulk intake.
- `source_bundle=excel_202603_structured` is reserved for generated `Tier D` candidate rows sourced from the curated Excel 202603 workbook intake.
- `source_bundle=property_governance_20260422` is reserved for generated inventory-only refrigerant rows derived from the governed bundle crosswalk.

Rules:

- Generated governance-bundle seeds stay `model_inclusion=no` unless explicitly promoted through a later catalog decision.
- The curated base inventory in `data/lake/raw/manual/seed_catalog.csv` remains authoritative for direct manual edits; generated supplements are append-only build inputs.

## Property Governance Bundle Artifacts

The governed bundle intake writes the following contract-level artifacts:

- `data/sources/property_governance/refrigerant_seed_database_20260422_property_governance_bundle.zip`
  Source bundle snapshot for the `2026-04-22` governance intake.
- `data/lake/extensions/property_governance_20260422/extension_manifest.parquet`
  Manifest of mirrored bundle tables with row counts.
- `data/lake/extensions/property_governance_20260422/tables/*.parquet`
  Fidelity-preserving mirror of every bundle table.
- `data/lake/extensions/property_governance_20260422/mixture_core.parquet`
  Normalized mixture-level table derived from the mirrored bundle.
- `data/lake/extensions/property_governance_20260422/mixture_component.parquet`
  Normalized mixture-component bridge derived from the mirrored bundle.
- `data/lake/bronze/property_governance_20260422_substance_crosswalk.parquet`
  Per-substance crosswalk from bundle identities into existing or generated inventory rows.
- `data/lake/bronze/property_governance_20260422_unresolved_substances.parquet`
  Residual unresolved bundle substances after crosswalking and accepted manual curations.
- `data/lake/bronze/property_governance_20260422_audit.json`
  Human-readable audit summary with table counts, crosswalk counts, canonical metrics, review-decision counts, and mirror status.
- `data/lake/gold/property_recommended_canonical_review_queue.parquet`
  Action-oriented review queue for canonical rows that still need manual adjudication or strict-coverage recovery.

Rules:

- The extension manifest must remain row-aligned with the bundle row manifest for every mirrored table.
- `data/lake/extensions/property_governance_20260422/tables/*.parquet` is the fidelity layer; normalized extension outputs do not replace it.
- `data/lake/bronze/property_governance_20260422_unresolved_substances.parquet` may be empty, but it must still represent the post-curation unresolved state rather than the raw pre-curation state.

## Unresolved Bundle Curation Input

`data/lake/raw/manual/property_governance_20260422_unresolved_curations.csv` is a controlled manual/external override input for unresolved governance-bundle substances.

Required columns:

- `substance_id`
- `refrigerant_number`
- `cas_number`
- `canonical_smiles`
- `isomeric_smiles`
- `inchi`
- `inchikey`
- `resolution_source`
- `resolution_source_url`
- `resolution_confidence`
- `notes`

Rules:

- Only `resolution_confidence=high` rows may be applied.
- Each accepted row must identify a single structure that is consistent across CAS, name, and structure evidence.
- Low-confidence, conflicting, or ambiguous rows must be rejected rather than silently overwriting unresolved bundle records.
- This curation input is for identity resolution only; it is not an observation source.

## Canonical Review Decision Input

`data/lake/raw/manual/property_governance_20260422_canonical_review_decisions.csv` is a controlled manual adjudication input for already-selected canonical recommendations that should be removed from the open review queue without changing the selected canonical recommendation.

Required columns:

- `mol_id`
- `canonical_feature_key`
- `review_reason`
- `decision_action`
- `expected_selected_source_id`
- `expected_selected_value`
- `resolution_basis`
- `resolution_source_url`
- `notes`

Rules:

- `decision_action` must currently be one of `accept_selected_source` or `accept_out_of_strict`.
- `accept_selected_source` may currently be used only for `review_reason` values `top_rank_conflict` or `source_divergence`.
- `accept_out_of_strict` may currently be used only for `review_reason` values `below_minimum_quality`, `non_numeric_selected_value`, or `not_ml_relevant`.
- `expected_selected_source_id` and `expected_selected_value` must match the current selected canonical recommendation exactly enough to detect stale decisions after upstream changes.
- Matching decision rows close the corresponding rows out of `property_recommended_canonical_review_queue` but do not rewrite `property_recommended_canonical`.
- `accept_out_of_strict` closes the queue row while leaving that recommendation outside `property_recommended_canonical_strict`.
- Conflict/source-divergence provenance flags and low-quality/qualitative canonical rows remain visible in `property_recommended_canonical`; the review-decision file only changes open-queue state and audit metrics.

## Proxy Acceptance Rule Input

`data/lake/raw/manual/property_governance_20260422_proxy_acceptance_rules.csv` is a controlled manual policy input for proxy-only canonical recommendations that may enter the strict ML-facing subset when no non-proxy candidate exists.

Required columns:

- `proxy_policy_id`
- `canonical_feature_key`
- `selected_source_id`
- `allow_in_strict_if_proxy_only`
- `rationale`
- `notes`

Rules:

- `allow_in_strict_if_proxy_only` must currently be `1`.
- Each rule must match a current canonical recommendation by `canonical_feature_key` and `selected_source_id`.
- Matching rows must be proxy-only selections with no non-proxy alternative remaining after canonical ranking.
- This policy input does not rewrite `property_recommended_canonical`; it only changes strict acceptance and whether a proxy-only row remains open in `property_recommended_canonical_review_queue`.
- Audit outputs must report the active proxy-policy count and the number of strict rows accepted through proxy-only policy.

## Excel 202603 Intake Artifacts

The curated workbook intake writes four contract-level artifacts:

- `data/lake/raw/manual/observations/excel_202603_observations.csv`
  Workbook-derived supplement observations merged automatically through `data/lake/raw/manual/observations/*.csv`. This file may contain both exact-matched existing molecules and workbook-generated candidate seeds.
- `data/lake/raw/generated/excel_202603_tierd_candidates.csv`
  Generated `seed_catalog`-compatible `Tier D` candidate supplement for workbook-only structured rows.
- `data/lake/raw/generated/excel_202603_name_only_staging.csv`
  Name-only staging table from the workbook; it is explicitly excluded from `seed_catalog` until a unique structure or external identifier is resolved.
- `docs/excel_202603_brief_report.md`
  Human-readable brief report summarizing workbook usability, supplement coverage, new-dimension decisions, and expansion/staging counts.

Rules:

- `data/lake/raw/manual/observations/excel_202603_observations.csv` may populate `boiling_point_c`, `critical_temp_c`, `critical_pressure_mpa`, `acentric_factor`, `vaporization_enthalpy_kjmol`, `odp`, and `critical_compressibility_factor`.
- The same workbook observation batch may backfill generated `source_bundle=excel_202603_structured` candidates after they enter `seed_catalog`, so the Excel intake supports both existing-molecule supplementation and new-candidate backfill.
- The workbook may additionally backfill generated Excel candidates through exact alias matches on resolved `molecule_alias` entries, but only for `boiling_point_c`, `critical_temp_c`, `critical_pressure_mpa`, and `acentric_factor`, and only when the matched value is unique for that candidate-property pair.
- Workbook `ODP` values outside the conservative `[0, 1]` import range are treated as outliers and excluded from the main observation tables.
- Bare `GWP` values from the workbook are report-only until a time horizon is confirmed.
- Workbook-only name staging is an analysis artifact, not an inventory source.

## Bulk Candidate Artifacts

Bulk PubChem acquisition writes five contract-level artifacts:

- `data/lake/bronze/coarse_filter_summary.parquet`
  Full `CID-Mass.gz` coarse-filter aggregate cube grouped by `coarse_filter_reason`, `element_pattern`, `carbon_bucket`, and `mass_bucket`.
- `data/lake/bronze/coarse_filter_summary.json`
  Compact human-readable rollup for the same coarse-filter pass.
- `data/lake/bronze/pubchem_candidate_pool.parquet`
  Filtered, RDKit-standardized candidate pool with structural annotations, de-duplication flags, and `volatility_status`.
- `data/lake/bronze/pubchem_candidate_filter_audit.parquet`
  Per-CID audit log for the materialized bulk-screening universe.
- `data/lake/raw/generated/pubchem_tierd_candidates.csv`
  Generated `seed_catalog`-compatible supplement used by `pipelines/generate_wave2_seed_catalog.py` to append a capped batch of `Tier D` rows.

Rules:

- The bulk candidate pool is a `Tier D`-only entry path.
- `build_v1_dataset.py` resolves `source_bundle=pubchem_bulk` rows from `pubchem_candidate_pool.parquet` and must not fall back to live per-record PubChem API requests for those rows.
- Existing curated refrigerant rows remain authoritative for completeness validation.
- Large local-only raw FTP bundles and regenerated bulk artifacts that should not be pushed to normal Git history are documented in `docs/operations/local_large_artifacts.md`.

Audit semantics:

- `coarse_filter_summary.parquet` is the project-default replacement for a full coarse-failure ledger across PubChem mass rows.
- `coarse_filter_summary.json` is a digest of the parquet summary rather than a second copy of all grouped rows.
- `pubchem_candidate_filter_audit.parquet` is not a full PubChem negative ledger; it covers the coarse-survivor universe and records why a near-candidate was excluded later.

## Core Tables

### `source_manifest`

Tracks source artifacts and generated source sessions.

Key fields:

- `source_id`
- `source_type`
- `source_name`
- `license`
- `retrieved_at`
- `checksum_sha256`
- `local_path`
- `parser_version`
- `upstream_url`
- `status`

### `molecule_core`

One row per standardized molecular entity.

Key fields:

- `mol_id`
- `seed_id`
- `canonical_smiles`
- `isomeric_smiles`
- `inchi`
- `inchikey`
- `inchikey_first_block`
- `formula`
- `molecular_weight`
- `charge`
- `heavy_atom_count`
- `stereo_flag`
- `ez_isomer`
- `family`
- `entity_scope`
- `model_inclusion`

For `source_bundle=pubchem_bulk`, the molecule identity is resolved from the local bulk candidate pool using the generated `seed_id=tierd_pubchem_<cid>` mapping.

### `molecule_alias`

Stores human and external identifiers as a crosswalk.

Key fields:

- `mol_id`
- `alias_type`
- `alias_value`
- `is_primary`
- `source_name`

### `property_observation`

Long table of all observed, curated, or calculated values.

Required fields:

- `observation_id`
- `mol_id`
- `property_name`
- `value`
- `value_num`
- `unit`
- `temperature`
- `pressure`
- `phase`
- `source_type`
- `source_name`
- `source_id`
- `method`
- `uncertainty`
- `quality_level`

Optional governance-alignment fields:

- `canonical_feature_key`
- `standard_unit`
- `bundle_record_id`
- `source_priority_rank`
- `data_quality_score_100`
- `is_proxy_or_screening`
- `ml_use_status`

Excel 202603 supplement rows enter here with `source_type=derived_harmonized` and keep workbook sheet provenance in `source_name` / `notes`.

### `property_observation_canonical`

Long canonical-property overlay derived from the governance bundle and linked to internal `mol_id` values.

Key fields:

- `observation_id`
- `mol_id`
- `canonical_property_id`
- `canonical_feature_key`
- `value`
- `unit`
- `source_id`
- `source_name`
- `standard_unit`
- `source_priority_rank`
- `data_quality_score_100`
- `is_proxy_or_screening`
- `bundle_record_id`

### `property_recommended`

One selected recommended value per `mol_id` and `property_name`, plus conflict metadata.

### `property_recommended_canonical`

One selected recommended value per `mol_id` and `canonical_feature_key` for the canonical-property layer.

Key fields:

- `mol_id`
- `canonical_feature_key`
- `canonical_property_id`
- `value`
- `unit`
- `selected_source_id`
- `selected_source_name`
- `source_priority_rank`
- `source_divergence_flag`
- `source_count`
- `conflict_flag`
- `conflict_detail`

Rules:

- `conflict_flag` captures unresolved disagreement inside the active top-rank candidate set.
- `source_divergence_flag` captures disagreement introduced by lower-priority candidate sources.
- These provenance flags may remain true even after matching rows are closed out of the open review queue via `property_governance_20260422_canonical_review_decisions.csv`.

### `property_recommended_canonical_strict`

ML-oriented strict subset of `property_recommended_canonical`, kept in long form.

Key fields:

- `mol_id`
- `canonical_feature_key`
- `canonical_property_id`
- `value`
- `value_num`
- `unit`
- `selected_source_id`
- `selected_source_name`
- `source_priority_rank`
- `data_quality_score_100`
- `is_proxy_or_screening`
- `source_count`
- `conflict_flag`
- `readiness_rule_id`
- `use_as_ml_feature`
- `use_as_ml_target`
- `minimum_quality_score`
- `exclude_if_proxy_or_screening`
- `strict_accept`
- `strict_accept_basis`
- `proxy_policy_id`
- `proxy_policy_rationale`

Rules:

- Rows must have numeric `value_num`.
- Rows must satisfy the matching readiness rule's minimum quality threshold.
- Rows must be excluded when the readiness rule forbids proxy/screening values and the selected recommendation is proxy/screening, unless an explicit matching proxy-only policy rule allows the row into strict output.
- At least one of `use_as_ml_feature` or `use_as_ml_target` must be enabled.
- `strict_accept_basis=standard` means the row passed the default strict filter with no proxy override.
- `strict_accept_basis=proxy_only_policy` means the row would otherwise be excluded as proxy/screening, but was admitted because it is proxy-only and matched an explicit policy rule.

### `property_recommended_canonical_review_queue`

Action queue for canonical recommendations that still need review after selection and strict filtering.

Key fields:

- `mol_id`
- `canonical_feature_key`
- `selected_source_id`
- `is_proxy_or_screening`
- `proxy_only_flag`
- `source_divergence_flag`
- `conflict_flag`
- `strict_accept`
- `strict_rejection_reason`
- `review_reason`
- `review_triggers`
- `review_priority`

Rules:

- Every row must be triggered by at least one of: top-rank conflict, lower-priority source divergence, or strict-filter rejection.
- `review_reason` is the primary driver for triage; `review_triggers` preserves the full trigger set.
- Matching rows listed in `data/lake/raw/manual/property_governance_20260422_canonical_review_decisions.csv` are excluded from this table, so it represents only open review work and may legitimately be empty after all current items are adjudicated.
- Matching rows admitted into `property_recommended_canonical_strict` through `data/lake/raw/manual/property_governance_20260422_proxy_acceptance_rules.csv` are also excluded from this table, so proxy-only policy acceptance closes those queue items without changing the selected canonical recommendation.
- This table is for manual follow-up and audit prioritization. It does not replace either the general canonical recommendation table or the strict ML-facing subset.

### `property_dictionary`

Canonical property dictionary for the governance layer.

### `property_canonical_map`

Mapping from raw governance-bundle table/property labels into canonical property ids and feature keys.

### `unit_conversion_rules`

Canonical unit-normalization rules used by the governance layer.

### `property_source_priority_rules`

Source-priority ranking rules used for canonical-property recommendation selection.

### `property_modeling_readiness_rules`

Per-feature ML-readiness rules used to build `property_recommended_canonical_strict`.

### `property_governance_issues`

Issue registry for governance-layer quality or mapping problems.

### `structure_features`

RDKit-derived descriptors, counts, fingerprints, scaffold, and SELFIES.

### `model_dataset_index`

Split assignment, label masks, confidence, and scaffold grouping for the promoted subset only.

### `model_ready`

The final model-facing join. This table is intentionally filtered to `model_inclusion=yes`.

Bulk-only `Tier D` candidates and generated governance-bundle refrigerants are expected to resolve into inventory tables such as `molecule_core` and `molecule_alias`, but they are excluded from `model_dataset_index` and `model_ready` until promoted.

## Manual Observation Inputs

The pipeline merges and deduplicates:

- `data/lake/raw/manual/manual_property_observations.csv`
- `data/lake/raw/manual/observations/*.csv`

Each physical file is registered in `source_manifest` and mapped into `property_observation.source_id`.

`data/lake/raw/manual/property_governance_20260422_unresolved_curations.csv` is not part of this observation merge. It is handled earlier as an identity-resolution override input for the governance bundle crosswalk.

## Recommended Source Priority

1. `manual_curated_reference`
2. `public_database`
3. `calculated_open_source`
4. `public_web_snapshot`
5. `placeholder`

## Canonical Units

- temperature: `degC`
- pressure: `MPa`
- density: `kg/m3`
- enthalpy: `kJ/mol`
- GWP/ODP: `dimensionless`
- COP: `dimensionless`
- volumetric cooling: `MJ/m3`

The added `critical_compressibility_factor` (`Zc`) also uses `dimensionless`.
