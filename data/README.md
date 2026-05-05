# Data Layout

- `lake/raw`, `lake/bronze`, `lake/silver`, `lake/gold`, `lake/extensions` — canonical layered file-lake outputs.
- `sources/` — categorized source inputs such as the Excel 202603 workbook, property-governance bundle, and supplemental curation packs.
- `indexes/` — rebuildable DuckDB query indexes.
- `artifacts/local/` — ignored workstation-only logs, scratch trees, executor bundles, and other large local artifacts.

Legacy paths such as `data/raw` and `data/index` are intentionally not duplicated. Code resolves the new layout first and keeps a temporary fallback for pre-cleanup checkouts.
