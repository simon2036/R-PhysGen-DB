from __future__ import annotations

import pandas as pd
import pytest

from r_physgen_db.constants import PROJECT_ROOT
from r_physgen_db.mixtures import (
    MIXTURE_COMPONENT_CURATION_SOURCE_ID,
    MIXTURE_FRACTION_CURATION_SOURCE_ID,
    apply_mixture_component_curations,
    apply_mixture_fraction_curations,
    build_mixture_tables,
    load_mixture_component_curations,
    load_mixture_fraction_curations,
)
from r_physgen_db.validate import _validate_mixtures


def test_governance_mixture_extension_maps_to_production_tables() -> None:
    root = PROJECT_ROOT / "data" / "extensions" / "property_governance_20260422"
    molecule_core = pd.read_parquet(PROJECT_ROOT / "data" / "silver" / "molecule_core.parquet")

    build = build_mixture_tables(
        pd.read_parquet(root / "mixture_core.parquet"),
        pd.read_parquet(root / "mixture_component.parquet"),
        molecule_core,
    )

    assert len(build.mixture_core) == 123
    assert len(build.mixture_composition) == 378
    assert set(build.mixture_composition["composition_basis"]) == {"mass_fraction"}
    assert build.mixture_composition["fraction_value"].dropna().between(0, 1).all()
    assert build.summary["dangling_component_count"] == 0
    assert build.summary["fraction_sum_error_count"] == 0
    assert build.summary["fraction_sum_unresolved_count"] == 1


def test_mixture_fraction_sum_validation_detects_errors() -> None:
    mixture_core = pd.DataFrame(
        [{"mixture_id": "mix_a", "mixture_name": "Mix A", "ashrae_blend_designation": "Mix A", "source_id": "s", "source_name": "s"}]
    )
    mixture_composition = pd.DataFrame(
        [
            {
                "mixture_id": "mix_a",
                "mixture_name": "Mix A",
                "component_mol_id": "mol_a",
                "component_role": "component",
                "composition_basis": "mass_fraction",
                "fraction_value": 0.75,
                "source_id": "s",
                "source_name": "s",
            },
            {
                "mixture_id": "mix_a",
                "mixture_name": "Mix A",
                "component_mol_id": "mol_missing",
                "component_role": "component",
                "composition_basis": "mass_fraction",
                "fraction_value": 0.10,
                "source_id": "s",
                "source_name": "s",
            },
        ]
    )
    results = {"integration_checks": [], "errors": []}

    _validate_mixtures(
        results,
        mixture_core,
        mixture_composition,
        pd.DataFrame([{"mol_id": "mol_a"}]),
        pd.DataFrame(columns=["mol_id"]),
        pd.DataFrame(columns=["mol_id"]),
        pd.DataFrame(columns=["mol_id"]),
    )

    assert any("Dangling mixture component_mol_id" in error for error in results["errors"])
    assert any("do not sum to 1" in error for error in results["errors"])


def test_mixture_tables_do_not_leak_into_wide_outputs() -> None:
    results = {"integration_checks": [], "errors": []}

    _validate_mixtures(
        results,
        pd.DataFrame([{"mixture_id": "mix_a"}]),
        pd.DataFrame(
            [
                {
                    "mixture_id": "mix_a",
                    "component_mol_id": "mol_a",
                    "composition_basis": "mass_fraction",
                    "fraction_value": 1.0,
                }
            ]
        ),
        pd.DataFrame([{"mol_id": "mol_a"}]),
        pd.DataFrame(columns=["mol_id", "mixture_id"]),
        pd.DataFrame(columns=["mol_id"]),
        pd.DataFrame(columns=["mol_id"]),
    )

    assert any("Mixture columns leaked into wide ML outputs" in error for error in results["errors"])


def test_mixture_fraction_curations_require_traceable_source_metadata() -> None:
    composition = pd.DataFrame(
        [
            {
                "mixture_id": "MIX_511A",
                "mixture_name": "R-511A",
                "component_mol_id": "mol_a",
                "component_role": "component",
                "composition_basis": "mass_fraction",
                "fraction_value": None,
                "source_id": "source_property_governance_mixture_component",
                "source_name": "Property Governance Normalized Mixture Component",
                "notes": "source_fraction_missing",
            }
        ]
    )

    with pytest.raises(ValueError, match="requires source_id and source_name"):
        apply_mixture_fraction_curations(
            composition,
            pd.DataFrame(
                [
                    {
                        "mixture_id": "MIX_511A",
                        "component_mol_id": "mol_a",
                        "composition_basis": "mass_fraction",
                        "fraction_value": 1.0,
                    }
                ]
            ),
        )

    curated = apply_mixture_fraction_curations(
        composition,
        pd.DataFrame(
            [
                {
                    "mixture_id": "MIX_511A",
                    "component_mol_id": "mol_a",
                    "composition_basis": "mass_fraction",
                    "fraction_value": 1.0,
                    "source_id": MIXTURE_FRACTION_CURATION_SOURCE_ID,
                    "source_name": "Manual Mixture Fraction Curations",
                    "source_url": "https://example.test/source",
                }
            ]
        ),
    )

    assert curated.iloc[0]["fraction_value"] == 1.0
    assert curated.iloc[0]["source_id"] == MIXTURE_FRACTION_CURATION_SOURCE_ID
    assert "fraction_curated_from_manual_source" in curated.iloc[0]["notes"]


def test_mixture_component_curations_require_traceable_source_metadata() -> None:
    composition = pd.DataFrame(
        [
            {
                "mixture_id": "MIX_511A",
                "mixture_name": "R-511A",
                "component_mol_id": "mol_ethane",
                "component_role": "component",
                "composition_basis": "mass_fraction",
                "fraction_value": None,
                "source_id": "source_property_governance_mixture_component",
                "source_name": "Property Governance Normalized Mixture Component",
                "notes": "source_fraction_missing",
            }
        ]
    )

    with pytest.raises(ValueError, match="requires source_id and source_name"):
        apply_mixture_component_curations(
            composition,
            pd.DataFrame(
                [
                    {
                        "mixture_id": "MIX_511A",
                        "current_component_mol_id": "mol_ethane",
                        "replacement_component_mol_id": "mol_dimethyl_ether",
                    }
                ]
            ),
        )

    curated = apply_mixture_component_curations(
        composition,
        pd.DataFrame(
            [
                {
                    "mixture_id": "MIX_511A",
                    "current_component_mol_id": "mol_ethane",
                    "replacement_component_mol_id": "mol_dimethyl_ether",
                    "source_id": MIXTURE_COMPONENT_CURATION_SOURCE_ID,
                    "source_name": "Manual Mixture Component Curations",
                    "source_url": "https://example.test/source",
                }
            ]
        ),
    )

    assert curated.iloc[0]["component_mol_id"] == "mol_dimethyl_ether"
    assert curated.iloc[0]["source_id"] == MIXTURE_COMPONENT_CURATION_SOURCE_ID
    assert "component_curated_from_manual_source" in curated.iloc[0]["notes"]


def test_mix511a_component_and_fraction_curations_replace_ethane_with_dimethyl_ether() -> None:
    root = PROJECT_ROOT / "data" / "extensions" / "property_governance_20260422"
    molecule_core = pd.read_parquet(PROJECT_ROOT / "data" / "silver" / "molecule_core.parquet")

    component_curations = load_mixture_component_curations(PROJECT_ROOT / "data" / "raw" / "manual" / "mixture_component_curations.csv")
    fraction_curations = load_mixture_fraction_curations(PROJECT_ROOT / "data" / "raw" / "manual" / "mixture_fraction_curations.csv")
    build = build_mixture_tables(
        pd.read_parquet(root / "mixture_core.parquet"),
        pd.read_parquet(root / "mixture_component.parquet"),
        molecule_core,
        component_curations=component_curations,
        fraction_curations=fraction_curations,
    )

    mix511a = build.mixture_composition[build.mixture_composition["mixture_id"] == "MIX_511A"]
    fractions = dict(zip(mix511a["component_mol_id"], pd.to_numeric(mix511a["fraction_value"], errors="coerce"), strict=True))

    assert "mol_otmsdbzupauedd-uhfffaoysa-n" not in fractions
    assert fractions == {
        "mol_atuoywhbwrkthz-uhfffaoysa-n": 0.95,
        "mol_lcglnkutagevqw-uhfffaoysa-n": 0.05,
    }
    assert pytest.approx(sum(fractions.values()), rel=0, abs=1e-9) == 1.0
    assert set(mix511a["source_id"]) == {MIXTURE_FRACTION_CURATION_SOURCE_ID}
    assert build.summary["fraction_sum_error_count"] == 0
