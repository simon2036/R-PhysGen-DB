from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

from r_physgen_db import pipeline


ROOT = Path(__file__).resolve().parents[1]


def _load_seed_generator_module():
    path = ROOT / "pipelines" / "generate_wave2_seed_catalog.py"
    spec = importlib.util.spec_from_file_location("generate_wave2_seed_catalog", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_seed_catalog_build_rows_supports_inventory_tail() -> None:
    module = _load_seed_generator_module()

    rows = module.build_rows()

    assert len(rows) > 120
    assert all("entity_scope" in row for row in rows)
    assert all("model_inclusion" in row for row in rows)
    assert sum(1 for row in rows if row["coverage_tier"] == "A") == 32
    assert sum(1 for row in rows if row["coverage_tier"] == "B") == 48
    assert sum(1 for row in rows if row["coverage_tier"] == "C") == 40
    assert any(row["coverage_tier"] == "D" for row in rows)
    assert all(row["model_inclusion"] == "yes" for row in rows if row["coverage_tier"] in {"A", "B", "C"})
    assert all(row["model_inclusion"] == "no" for row in rows if row["coverage_tier"] == "D")
    assert all(row["selection_role"] == "inventory" for row in rows if row["coverage_tier"] == "D")
    assert any(row["r_number"] == "R-114a" for row in rows)


def test_load_manual_observations_merges_legacy_and_directory_without_duplicates(tmp_path: Path) -> None:
    raw_manual = tmp_path / "data" / "raw" / "manual"
    raw_manual.mkdir(parents=True)
    observations_dir = raw_manual / "observations"
    observations_dir.mkdir()

    header = (
        "seed_id,r_number,property_name,value,value_num,unit,temperature,pressure,phase,source_type,"
        "source_name,source_url,method,uncertainty,quality_level,assessment_version,time_horizon,year,notes\n"
    )
    row = (
        "seed_x,R-999,gwp_100yr,10,10,dimensionless,,,,manual_curated_reference,Example,https://example.com,"
        "manual,,manual_curated_reference,AR6,100,2026,test row\n"
    )
    (raw_manual / "manual_property_observations.csv").write_text(header + row, encoding="utf-8")
    (observations_dir / "extra.csv").write_text(header + row, encoding="utf-8")

    loaded = pipeline._load_manual_observations(
        {
            "manual_observations": raw_manual / "manual_property_observations.csv",
            "manual_observations_dir": observations_dir,
        }
    )

    assert len(loaded) == 1
    assert loaded.iloc[0]["r_number"] == "R-999"


def test_review_only_inequality_file_is_manifested_but_not_ingested(tmp_path: Path) -> None:
    raw_manual = tmp_path / "data" / "raw" / "manual"
    raw_manual.mkdir(parents=True)
    observations_dir = raw_manual / "observations"
    observations_dir.mkdir()
    review_only_dir = raw_manual / "review_only"
    review_only_dir.mkdir()

    header = (
        "seed_id,r_number,property_name,value,value_num,unit,temperature,pressure,phase,source_type,"
        "source_name,source_url,method,uncertainty,quality_level,assessment_version,time_horizon,year,notes\n"
    )
    row = (
        "seed_x,R-999,gwp_100yr,10,10,dimensionless,,,,manual_curated_reference,Example,https://example.com,"
        "manual,,manual_curated_reference,AR6,100,2026,test row\n"
    )
    review_header = (
        "candidate_key,seed_id,r_number,property_name,reported_value,unit,source_name,source_url,"
        "do_not_merge_reason,recommended_next_step,notes\n"
    )
    review_row = (
        "candidate,seed_x,R-999,gwp_100yr,<<1,dimensionless,Example,https://example.com,"
        "source reports inequality/bound rather than exact numeric value,add bound semantics,review only\n"
    )
    (raw_manual / "manual_property_observations.csv").write_text(header + row, encoding="utf-8")
    review_path = review_only_dir / "review_only_inequality_observations_round2_20260501.csv"
    review_path.write_text(review_header + review_row, encoding="utf-8")

    paths = {
        "seed_catalog": raw_manual / "seed_catalog.csv",
        "refrigerant_inventory": raw_manual / "refrigerant_inventory.csv",
        "manual_observations": raw_manual / "manual_property_observations.csv",
        "manual_observations_dir": observations_dir,
        "coolprop_aliases": raw_manual / "coolprop_aliases.yaml",
        "raw_generated_pubchem_tierd_candidates": tmp_path / "data" / "raw" / "generated" / "pubchem_tierd_candidates.csv",
        "bronze_pubchem_candidate_pool": tmp_path / "data" / "bronze" / "pubchem_candidate_pool.parquet",
        "bronze_pubchem_candidate_filter_audit": tmp_path / "data" / "bronze" / "pubchem_candidate_filter_audit.parquet",
        "raw_review_only_inequality_observations": review_path,
    }

    loaded = pipeline._load_manual_observations(paths)
    source_ids = {row["source_id"] for row in pipeline._register_manual_sources(paths)}

    assert len(loaded) == 1
    assert set(loaded["r_number"]) == {"R-999"}
    assert "source_manual_review_only_inequality_observations_20260501" in source_ids


def test_epa_grouped_candidate_mapping_skips_refrigerant_inventory_rows() -> None:
    gwp_df = pd.DataFrame(
        [
            {
                "substance_name": "Hydrocarbons (C5-C20)",
                "gwp_text": "1.3-3.7",
                "gwp_100yr": None,
                "gwp_range_min": 1.3,
                "gwp_range_max": 3.7,
                "reference": "Calculated",
                "is_range": True,
            },
        ]
    )
    molecule_context = pd.DataFrame(
        [
            {
                "mol_id": "mol_candidate",
                "seed_id": "seed_candidate",
                "family": "Candidate",
                "formula": "C5H12",
                "pubchem_query": "109-66-0",
                "coverage_tier": "D",
                "selection_role": "inventory",
                "entity_scope": "candidate",
            },
            {
                "mol_id": "mol_refrigerant",
                "seed_id": "seed_refrigerant",
                "family": "Natural",
                "formula": "C5H12",
                "pubchem_query": "109-66-0",
                "coverage_tier": "D",
                "selection_role": "inventory",
                "entity_scope": "refrigerant",
            },
        ]
    )

    rows = pipeline._epa_gwp_reference_property_rows(
        gwp_df=gwp_df,
        alias_lookup={},
        molecule_context=molecule_context,
        source_id="source_epa_gwp",
    )

    assert {row["mol_id"] for row in rows} == {"mol_candidate"}


def test_model_outputs_only_include_model_ready_inventory() -> None:
    structure_features = pd.DataFrame(
        [
            {"mol_id": "mol_keep", "scaffold_key": "scaf_a"},
            {"mol_id": "mol_drop", "scaffold_key": "scaf_b"},
        ]
    )
    property_recommended = pd.DataFrame(
        [
            {
                "mol_id": "mol_keep",
                "property_name": "gwp_100yr",
                "value": "1",
                "value_num": 1.0,
                "selected_quality_level": "manual_curated_reference",
            },
            {
                "mol_id": "mol_drop",
                "property_name": "gwp_100yr",
                "value": "2",
                "value_num": 2.0,
                "selected_quality_level": "manual_curated_reference",
            },
        ]
    )
    molecule_core = pd.DataFrame(
        [
            {"mol_id": "mol_keep", "model_inclusion": "yes"},
            {"mol_id": "mol_drop", "model_inclusion": "no"},
        ]
    )
    molecule_master = pd.DataFrame(
        [
            {"mol_id": "mol_keep", "scaffold_key": "scaf_a", "canonical_smiles": "C", "isomeric_smiles": "C", "selfies": "[C]"},
            {"mol_id": "mol_drop", "scaffold_key": "scaf_b", "canonical_smiles": "CC", "isomeric_smiles": "CC", "selfies": "[C][C]"},
        ]
    )
    property_matrix = pd.DataFrame(
        [
            {"mol_id": "mol_keep", "gwp_100yr": 1.0},
            {"mol_id": "mol_drop", "gwp_100yr": 2.0},
        ]
    )

    model_index = pipeline._build_model_dataset_index(structure_features, property_recommended, molecule_core)
    model_ready = pipeline._build_model_ready(molecule_master, property_matrix, model_index)

    assert model_index["mol_id"].tolist() == ["mol_keep"]
    assert model_ready["mol_id"].tolist() == ["mol_keep"]


def test_quality_report_includes_inventory_metrics() -> None:
    seed_catalog = pd.DataFrame(
        [
            {"seed_id": "seed_keep", "coverage_tier": "A", "entity_scope": "refrigerant"},
            {"seed_id": "seed_gap", "coverage_tier": "D", "entity_scope": "refrigerant"},
            {"seed_id": "seed_candidate", "coverage_tier": "D", "entity_scope": "candidate"},
        ]
    )
    molecule_core = pd.DataFrame(
        [
            {"mol_id": "mol_keep", "seed_id": "seed_keep"},
            {"mol_id": "mol_candidate", "seed_id": "seed_candidate"},
        ]
    )
    property_observation = pd.DataFrame([{"mol_id": "mol_keep"}])
    property_recommended = pd.DataFrame(
        [
            {"mol_id": "mol_keep", "property_name": "gwp_100yr", "selected_source_id": "source_manual"},
        ]
    )
    model_ready = pd.DataFrame([{"mol_id": "mol_keep", "split": "train"}])
    resolution_df = pd.DataFrame(
        [
            {"seed_id": "seed_gap", "stage": "pubchem", "status": "failed", "detail": "not found"},
        ]
    )

    report = pipeline._build_quality_report(
        seed_catalog=seed_catalog,
        molecule_core=molecule_core,
        property_observation=property_observation,
        property_recommended=property_recommended,
        model_ready=model_ready,
        qc_issues=pd.DataFrame(columns=["mol_id", "issue_type", "detail"]),
        resolution_df=resolution_df,
        regulatory_status=pd.DataFrame(columns=["mol_id"]),
        pending_sources=pd.DataFrame(columns=["pending_id"]),
    )

    assert report["refrigerant_count"] == 2
    assert report["candidate_count"] == 1
    assert report["unresolved_refrigerants"] == [{"seed_id": "seed_gap", "stage": "pubchem", "detail": "not found"}]
    assert report["inventory_property_gaps"]["refrigerant"]["D"]["gwp_100yr"]["missing_count"] == 1


def test_select_recommended_prefers_non_proxy_rows_even_when_proxy_rank_is_higher() -> None:
    df = pd.DataFrame(
        [
            {
                "mol_id": "mol_a",
                "property_name": "gwp_100yr",
                "value": "1200",
                "value_num": 1200.0,
                "unit": "dimensionless",
                "source_type": "manual_curated_reference",
                "source_name": "Proxy",
                "source_id": "source_proxy",
                "quality_level": "manual_curated_reference",
                "source_priority_rank": 1,
                "data_quality_score_100": 99,
                "is_proxy_or_screening": 1,
            },
            {
                "mol_id": "mol_a",
                "property_name": "gwp_100yr",
                "value": "550",
                "value_num": 550.0,
                "unit": "dimensionless",
                "source_type": "derived_harmonized",
                "source_name": "Authoritative",
                "source_id": "source_auth",
                "quality_level": "derived_harmonized",
                "source_priority_rank": 3,
                "data_quality_score_100": 90,
                "is_proxy_or_screening": 0,
            },
        ]
    )

    selected = pipeline._select_recommended(df)

    assert selected.iloc[0]["selected_source_id"] == "source_auth"
    assert selected.iloc[0]["value_num"] == 550.0
