# Wave 2 Implementation

## Scope

Wave 2 now separates inventory breadth from model readiness:

- `Tier A`: 32 anchor refrigerants with high-completeness thermodynamic, environmental, and safety labels.
- `Tier B`: 48 promoted known refrigerants / industrial candidates.
- `Tier C`: 40 promoted long-tail candidates.
- `Tier D`: inventory-only molecules that are in scope but not yet promoted into the model subset.

The current local catalog contains `132` seed rows. The promoted model subset remains `120` molecules.

## Inventory Model

`data/lake/raw/manual/seed_catalog.csv` now acts as the authoritative inventory catalog instead of a fixed-size Wave 2 seed list. It includes:

- `entity_scope`
  - `refrigerant`
  - `candidate`
- `model_inclusion`
  - `yes`
  - `no`
- `selection_role`
  - promoted rows continue to use `anchor`, `expansion`, or `candidate`
  - inventory-only rows use `inventory`

Interpretation:

- `Tier A/B/C` with `model_inclusion=yes` feed `model_dataset_index` and `model_ready`
- `Tier D` with `model_inclusion=no` still resolve into the entity and observation tables, but are excluded from model-facing outputs

## Source Adapters

- `PubChem PUG REST`
  Primary structure, identifier, and synonym anchor.
- `NIST Chemistry WebBook`
  Phase-change page snapshots are fetched with `Mask=4` and parsed for boiling point, critical values when available, and enthalpy of vaporization.
- `EPA ODS`
  Parsed for atmospheric lifetime, ODP, and AR4/AR5 GWP values.
- `EPA SNAP`
  Parsed across multiple refrigeration and air-conditioning end-use pages and mapped into both `regulatory_status` and selected label observations.
- `EPA Technology Transitions GWP Reference Table`
  Parsed for explicit refrigerant matches and candidate-only grouped upper-bound mappings.
- `CoolProp`
  Supplies open thermodynamic and cycle labels. `R-744` uses a dedicated transcritical path. Unsupported fluids are not guessed implicitly; only explicit aliases in `data/lake/raw/manual/coolprop_aliases.yaml` are used.

## Manual Inputs

- `data/lake/raw/manual/refrigerant_inventory.csv`
  Curated refrigerant additions beyond the existing CoolProp-backed list.
- `data/lake/raw/manual/manual_property_observations.csv`
  Legacy manual observation input.
- `data/lake/raw/manual/observations/*.csv`
  Additional manual observation batches merged with the legacy file and deduplicated before ingestion.

## Validation Targets

The current validation enforces:

- at least `120` resolved molecules overall
- full refrigerant inventory resolution
- `Tier A` coverage thresholds for `boiling_point_c`, `critical_temp_c`, `critical_pressure_mpa`, `odp`, `gwp_100yr`, and `ashrae_safety`
- `Tier B` core thermodynamic coverage thresholds
- no dangling source tracing
- no scaffold split leakage
- preserved separation for `R-1234ze(E)` and `R-1234ze(Z)`
- `model_ready` consistency with `model_dataset_index`

The validation report also emits non-blocking convergence metrics for label gaps by `entity_scope` and `coverage_tier`.

## Known Boundaries

- EPA SNAP page availability is intermittently affected by remote SSL failures during live refreshes. Cached successful snapshots are retained and reused.
- Some NIST phase pages do not expose machine-readable tables, so specific compounds may still report `No tables found`.
- Candidate-only grouped mappings are intentionally restricted from filling explicit refrigerant inventory rows.
- CompTox is deliberately non-blocking. Without `COMPTOX_API_KEY` or `EPA_COMPTOX_API_KEY`, records are written to `pending_sources` instead of failing the build.
