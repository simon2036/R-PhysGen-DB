# Archive Manifest — 2026-05-05

This manifest records repository-organization moves made during the 2026-05-05 cleanup. Large ignored local artifacts remain local-only and are documented in `docs/operations/local_large_artifacts.md` and `data/README.md`.

| Old path | New path | Reason |
| --- | --- | --- |
| `data/raw` | `data/lake/raw` | Move layer into the explicit data/lake hierarchy |
| `data/bronze` | `data/lake/bronze` | Move layer into the explicit data/lake hierarchy |
| `data/silver` | `data/lake/silver` | Move layer into the explicit data/lake hierarchy |
| `data/gold` | `data/lake/gold` | Move layer into the explicit data/lake hierarchy |
| `data/extensions` | `data/lake/extensions` | Move layer into the explicit data/lake hierarchy |
| `data/index` | `data/indexes` | Rename rebuildable query-index directory to data/indexes |
| `methods/制冷剂数据库202603.xlsx` | `data/sources/excel/制冷剂数据库202603.xlsx` | Categorize authoritative workbook reference input |
| `methods/refrigerant_seed_database_20260422_property_governance_bundle.zip` | `data/sources/property_governance/refrigerant_seed_database_20260422_property_governance_bundle.zip` | Categorize property-governance source bundle |
| `SupplementData` | `data/sources/supplemental/SupplementData` | Categorize supplemental curation source packs |
| `runs` | `data/artifacts/local/runs` | Move local run logs/artifacts under ignored local artifact policy |
| `R-PhysGen-DB.html` | `deploy/static/R-PhysGen-DB.html` | Move current static frontend out of repository root |
| `R-PhysGen-DB_frontend_review_and_revision_suggestions.md` | `docs/reviews/R-PhysGen-DB_frontend_review_and_revision_suggestions.md` | Categorize frontend review notes |
| `r-physgen-db-frontend.zip` | `archive/2026-05-05/frontend-bundles/r-physgen-db-frontend.zip` | Archive superseded frontend bundle |
| `前端claudeclaudedesignclaudedesignv1.zip` | `archive/2026-05-05/frontend-bundles/前端claudeclaudedesignclaudedesignv1.zip` | Archive superseded frontend bundle |
| `前端v2据库.zip` | `archive/2026-05-05/frontend-bundles/前端v2据库.zip` | Archive superseded frontend bundle |
| `前端v3库.zip` | `archive/2026-05-05/frontend-bundles/前端v3库.zip` | Archive superseded frontend bundle |
| `methods/R-PhysGen-DB_P0_review.md` | `docs/reviews/R-PhysGen-DB_P0_review.md` | Categorize P0 review notes |
| `methods` | `archive/2026-05-05/methods` | Archive historical P0 packages, prototypes, and legacy method notes |
| `周报` | `archive/2026-05-05/weekly-reports/周报` | Archive weekly report docx bundle |
| `docs/controlled_vocabularies.md` | `docs/contracts/controlled_vocabularies.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/data_contract.md` | `docs/contracts/data_contract.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/dataset_migration_spec.md` | `docs/contracts/dataset_migration_spec.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/phase2_interfaces.md` | `docs/contracts/phase2_interfaces.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/dataset_migrations` | `docs/migrations/dataset` | Categorize documentation under the reorganized docs taxonomy |
| `docs/current_status.md` | `docs/overview/current_status.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/project_scope.md` | `docs/overview/project_scope.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/wave2_implementation.md` | `docs/overview/wave2_implementation.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/excel_202603_brief_report.md` | `docs/reports/excel_202603_brief_report.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/R-PhysGen-DB_Data_Enhancement_and_Computation_Update_Plan.docx` | `docs/reports/R-PhysGen-DB_Data_Enhancement_and_Computation_Update_Plan.docx` | Categorize documentation under the reorganized docs taxonomy |
| `docs/v1.6.0_data_enhancement_report.md` | `docs/reports/v1.6.0_data_enhancement_report.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/v1.6.1_data_enhancement_report.md` | `docs/reports/v1.6.1_data_enhancement_report.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/v1.6.2_data_enhancement_report.md` | `docs/reports/v1.6.2_data_enhancement_report.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/v1.6.3_data_enhancement_report.md` | `docs/reports/v1.6.3_data_enhancement_report.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/v1.6.3_residual_compute_completion_summary.md` | `docs/reports/v1.6.3_residual_compute_completion_summary.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/v1.6.3_supplement_data_migration.md` | `docs/reports/v1.6.3_supplement_data_migration.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/local_large_artifacts.md` | `docs/operations/local_large_artifacts.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/p0_scope_and_exit_criteria.md` | `docs/plans/p0/p0_scope_and_exit_criteria.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/p0_phase2_interfaces_draft.md` | `docs/plans/p0/p0_phase2_interfaces_draft.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/p0_pipeline_stage_refactor_plan.md` | `docs/plans/p0/p0_pipeline_stage_refactor_plan.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/p0_validation_rules_draft.md` | `docs/plans/p0/p0_validation_rules_draft.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/p0_remaining_backlog.md` | `docs/plans/p0/p0_remaining_backlog.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/p0_review_response_matrix.md` | `docs/reviews/p0_review_response_matrix.md` | Categorize documentation under the reorganized docs taxonomy |
| `docs/p0_change_log_and_handoff.md` | `docs/handoffs/p0_change_log_and_handoff.md` | Categorize documentation under the reorganized docs taxonomy |

## Notable archived contents

- `methods/R-PhysGen-DB_P0_package_v3.zip` is now under `archive/2026-05-05/methods/R-PhysGen-DB_P0_package_v3.zip`.
- `methods/R-PhysGen-DB_P0_package.zip` is now under `archive/2026-05-05/methods/R-PhysGen-DB_P0_package.zip`.
- `methods/refrigerant_data_project.zip` is now under `archive/2026-05-05/methods/refrigerant_data_project.zip`.
- `周报/R-PhysGen-DB_第一阶段数据库汇总20260423.docx` is now under `archive/2026-05-05/weekly-reports/周报/R-PhysGen-DB_第一阶段数据库汇总20260423.docx`.
