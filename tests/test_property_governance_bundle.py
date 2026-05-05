from __future__ import annotations

import hashlib
import io
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from r_physgen_db.chemistry import standardize_smiles
from r_physgen_db.sources.property_governance_bundle import (
    _coerce_governed_value_num,
    build_canonical_recommended_review_queue,
    integrate_property_governance_bundle,
    load_property_governance_canonical_review_decisions,
    load_property_governance_proxy_acceptance_rules,
    select_canonical_recommended,
    select_canonical_recommended_strict,
)


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _write_bundle(path: Path, tables: dict[str, pd.DataFrame]) -> None:
    inner_bytes = io.BytesIO()
    with zipfile.ZipFile(inner_bytes, "w", compression=zipfile.ZIP_DEFLATED) as inner:
        for table_name, frame in tables.items():
            inner.writestr(f"{table_name}.csv", frame.to_csv(index=False).encode("utf-8"))
    inner_payload = inner_bytes.getvalue()

    summary_text = "# synthetic bundle\n"
    qa_text = "qa ok\n"
    row_manifest = pd.DataFrame(
        [{"table_name": table_name, "row_count": len(frame)} for table_name, frame in tables.items()]
    )
    file_manifest = pd.DataFrame(
        [
            {
                "file_name": "refrigerant_seed_database_20260422_property_governance_csv_tables.zip",
                "relative_path": "refrigerant_seed_database_20260422_property_governance_csv_tables.zip",
                "size_bytes": len(inner_payload),
                "sha256": _sha256_bytes(inner_payload),
            },
            {
                "file_name": "property_governance_update_summary_20260422.md",
                "relative_path": "property_governance_update_summary_20260422.md",
                "size_bytes": len(summary_text.encode("utf-8")),
                "sha256": _sha256_bytes(summary_text.encode("utf-8")),
            },
            {
                "file_name": "workbook_QA_property_governance_20260422.txt",
                "relative_path": "workbook_QA_property_governance_20260422.txt",
                "size_bytes": len(qa_text.encode("utf-8")),
                "sha256": _sha256_bytes(qa_text.encode("utf-8")),
            },
            {
                "file_name": "manifest_row_counts_20260422_property_governance.csv",
                "relative_path": "manifest_row_counts_20260422_property_governance.csv",
                "size_bytes": len(row_manifest.to_csv(index=False).encode("utf-8")),
                "sha256": _sha256_bytes(row_manifest.to_csv(index=False).encode("utf-8")),
            },
            {
                "file_name": "file_manifest_20260422_property_governance.csv",
                "relative_path": "file_manifest_20260422_property_governance.csv",
                "size_bytes": 0,
                "sha256": "",
            },
        ]
    )
    file_manifest_csv = file_manifest.to_csv(index=False).encode("utf-8")

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as outer:
        outer.writestr("refrigerant_seed_database_20260422_property_governance_csv_tables.zip", inner_payload)
        outer.writestr("property_governance_update_summary_20260422.md", summary_text.encode("utf-8"))
        outer.writestr("workbook_QA_property_governance_20260422.txt", qa_text.encode("utf-8"))
        outer.writestr("manifest_row_counts_20260422_property_governance.csv", row_manifest.to_csv(index=False).encode("utf-8"))
        outer.writestr("file_manifest_20260422_property_governance.csv", file_manifest_csv)


def _write_unresolved_curations(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        rows,
        columns=[
            "substance_id",
            "refrigerant_number",
            "cas_number",
            "canonical_smiles",
            "isomeric_smiles",
            "inchi",
            "inchikey",
            "resolution_source",
            "resolution_source_url",
            "resolution_confidence",
            "notes",
        ],
    ).to_csv(path, index=False)


def _write_canonical_review_decisions(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        rows,
        columns=[
            "mol_id",
            "canonical_feature_key",
            "review_reason",
            "decision_action",
            "expected_selected_source_id",
            "expected_selected_value",
            "resolution_basis",
            "resolution_source_url",
            "notes",
        ],
    ).to_csv(path, index=False)


def _write_proxy_acceptance_rules(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        rows,
        columns=[
            "proxy_policy_id",
            "canonical_feature_key",
            "selected_source_id",
            "allow_in_strict_if_proxy_only",
            "rationale",
            "notes",
        ],
    ).to_csv(path, index=False)


def _minimal_unresolved_bundle_tables() -> dict[str, pd.DataFrame]:
    return {
        "tbl_substances": pd.DataFrame(
            [
                {
                    "substance_id": "SUB092",
                    "refrigerant_number": "R-C316",
                    "family": "PFC",
                    "common_name": "R-C316",
                    "chemical_name": "1,2-dichloro-1,2,3,3,4,4-hexafluorocyclobutane",
                    "chemical_formula": "C4Cl2F6",
                    "cas_number": "356-18-3",
                    "safety_group": "",
                    "primary_source_id": "SRC_AUTH",
                    "notes": "",
                }
            ]
        ),
        "tbl_molecular_info": pd.DataFrame(
            [
                {
                    "molecule_info_id": "MI092",
                    "substance_id": "SUB092",
                    "refrigerant_number": "R-C316",
                    "scope_status": "ASHRAE_pure_approved",
                    "ashrae_category": "Test",
                    "family": "PFC",
                    "chemical_name": "1,2-dichloro-1,2,3,3,4,4-hexafluorocyclobutane",
                    "common_name": "R-C316",
                    "molecular_formula": "C4Cl2F6",
                    "cas_number": "356-18-3",
                    "molecular_weight_g_mol": 232.94,
                    "smiles": "",
                    "canonical_smiles": "",
                    "isomeric_smiles": "",
                    "inchi": "",
                    "inchikey": "",
                }
            ]
        ),
        "tbl_sources": pd.DataFrame([{"source_id": "SRC_AUTH", "source_type": "official", "title": "Authoritative Source"}]),
        "tbl_property_dictionary_v1": pd.DataFrame(
            [
                {
                    "canonical_property_id": "PROP_GWP100",
                    "canonical_property_group": "environmental",
                    "canonical_property_name": "gwp_100yr",
                    "canonical_feature_key": "environmental.gwp_100yr",
                    "standard_unit": "dimensionless",
                }
            ]
        ),
        "tbl_property_canonical_map_v1": pd.DataFrame(
            [
                {
                    "canonical_map_id": "MAP001",
                    "raw_table_name": "tbl_pure_properties",
                    "raw_property_group": "environmental",
                    "raw_property_name": "GWP100",
                    "canonical_property_id": "PROP_GWP100",
                    "canonical_feature_key": "environmental.gwp_100yr",
                }
            ]
        ),
        "tbl_unit_conversion_rules_v1": pd.DataFrame(
            [
                {
                    "unit_conversion_rule_id": "UNIT001",
                    "from_unit": "dimensionless",
                    "to_standard_unit": "dimensionless",
                    "scale_factor": 1.0,
                    "offset": 0.0,
                    "conversion_formula": "identity",
                    "dimensionality": "scalar",
                    "notes": "",
                    "dictionary_version": "test",
                    "created_date": "2026-04-22",
                }
            ]
        ),
        "tbl_property_source_priority_rules_v1": pd.DataFrame(
            [{"priority_rule_id": "SP001", "property_scope": "environmental", "source_priority_rank": 2, "source_class": "official"}]
        ),
        "tbl_property_modeling_readiness_rules_v1": pd.DataFrame(
            [
                {
                    "readiness_rule_id": "ML001",
                    "canonical_property_id": "PROP_GWP100",
                    "canonical_feature_key": "environmental.gwp_100yr",
                    "use_as_ml_feature": 1,
                    "use_as_ml_target": 1,
                    "minimum_quality_score": 70,
                    "exclude_if_proxy_or_screening": 1,
                    "preferred_standard_unit": "dimensionless",
                    "normalization_recommendation": "identity",
                    "missing_value_strategy": "drop",
                    "notes": "strict test rule",
                }
            ]
        ),
        "tbl_property_governance_issues_v1": pd.DataFrame(
            [{"issue_id": "ISS001", "issue_type": "missing_structure", "severity": "high", "affected_record_count": 1, "recommended_action": "curate"}]
        ),
        "tbl_pure_properties_canonical_overlay_v1": pd.DataFrame(
            [
                {
                    "overlay_id": "OV092",
                    "record_id": "R092",
                    "substance_id": "SUB092",
                    "canonical_property_id": "PROP_GWP100",
                    "canonical_feature_key": "environmental.gwp_100yr",
                    "canonical_property_group": "environmental",
                    "canonical_property_name": "gwp_100yr",
                    "standard_unit": "dimensionless",
                    "standard_value_numeric": 7200.0,
                    "standard_value_text": "",
                    "source_id": "SRC_AUTH",
                    "source_priority_rank": 2,
                    "data_quality_score_100": 92,
                    "is_proxy_or_screening": 0,
                    "ml_use_status": "recommended_numeric_candidate",
                    "notes": "resolved via curation",
                }
            ]
        ),
        "tbl_mixtures": pd.DataFrame(columns=["mixture_id", "refrigerant_number", "notes"]),
        "tbl_mixture_components": pd.DataFrame(columns=["mixture_id", "component_refrigerant", "mass_pct"]),
    }


def test_integrate_property_governance_bundle_builds_crosswalk_and_canonical_layers(tmp_path: Path) -> None:
    existing = standardize_smiles("CC")
    novel = standardize_smiles("C=C")

    tables = {
        "tbl_substances": pd.DataFrame(
            [
                {
                    "substance_id": "SUB001",
                    "refrigerant_number": "R-Existing",
                    "family": "HFC",
                    "common_name": "Existing Fluid",
                    "chemical_name": "ethane",
                    "chemical_formula": "C2H6",
                    "cas_number": "100-00-0",
                    "safety_group": "A1",
                    "primary_source_id": "SRC_AUTH",
                    "notes": "",
                },
                {
                    "substance_id": "SUB002",
                    "refrigerant_number": "R-New",
                    "family": "HFO",
                    "common_name": "Novel Fluid",
                    "chemical_name": "ethene",
                    "chemical_formula": "C2H4",
                    "cas_number": "200-00-0",
                    "safety_group": "A2L",
                    "primary_source_id": "SRC_AUTH",
                    "notes": "",
                },
            ]
        ),
        "tbl_molecular_info": pd.DataFrame(
            [
                {
                    "molecule_info_id": "MI001",
                    "substance_id": "SUB001",
                    "refrigerant_number": "R-Existing",
                    "scope_status": "ASHRAE_pure_approved",
                    "ashrae_category": "Test",
                    "family": "HFC",
                    "chemical_name": "ethane",
                    "common_name": "Existing Fluid",
                    "molecular_formula": existing["formula"],
                    "cas_number": "100-00-0",
                    "molecular_weight_g_mol": existing["molecular_weight"],
                    "smiles": existing["canonical_smiles"],
                    "canonical_smiles": existing["canonical_smiles"],
                    "isomeric_smiles": existing["isomeric_smiles"],
                    "inchi": existing["inchi"],
                    "inchikey": existing["inchikey"],
                },
                {
                    "molecule_info_id": "MI002",
                    "substance_id": "SUB002",
                    "refrigerant_number": "R-New",
                    "scope_status": "ASHRAE_pure_approved",
                    "ashrae_category": "Test",
                    "family": "HFO",
                    "chemical_name": "ethene",
                    "common_name": "Novel Fluid",
                    "molecular_formula": novel["formula"],
                    "cas_number": "200-00-0",
                    "molecular_weight_g_mol": novel["molecular_weight"],
                    "smiles": novel["canonical_smiles"],
                    "canonical_smiles": novel["canonical_smiles"],
                    "isomeric_smiles": novel["isomeric_smiles"],
                    "inchi": novel["inchi"],
                    "inchikey": novel["inchikey"],
                },
            ]
        ),
        "tbl_sources": pd.DataFrame(
            [
                {"source_id": "SRC_AUTH", "source_type": "official", "title": "Authoritative Source"},
                {"source_id": "SRC_PROXY", "source_type": "screening", "title": "Proxy Source"},
            ]
        ),
        "tbl_property_dictionary_v1": pd.DataFrame(
            [
                {
                    "canonical_property_id": "PROP_GWP100",
                    "canonical_property_group": "environmental",
                    "canonical_property_name": "gwp_100yr",
                    "canonical_feature_key": "environmental.gwp_100yr",
                    "standard_unit": "dimensionless",
                },
                {
                    "canonical_property_id": "PROP_TB",
                    "canonical_property_group": "thermodynamic",
                    "canonical_property_name": "normal_boiling_temperature",
                    "canonical_feature_key": "thermodynamic.normal_boiling_temperature",
                    "standard_unit": "K",
                },
                {
                    "canonical_property_id": "PROP_SAFETY",
                    "canonical_property_group": "safety",
                    "canonical_property_name": "safety_group",
                    "canonical_feature_key": "safety.safety_group",
                    "standard_unit": "dimensionless",
                },
            ]
        ),
        "tbl_property_canonical_map_v1": pd.DataFrame(
            [
                {
                    "canonical_map_id": "MAP001",
                    "raw_table_name": "tbl_pure_properties",
                    "raw_property_group": "environmental",
                    "raw_property_name": "GWP100",
                    "canonical_property_id": "PROP_GWP100",
                    "canonical_feature_key": "environmental.gwp_100yr",
                }
            ]
        ),
        "tbl_unit_conversion_rules_v1": pd.DataFrame(
            [
                {
                    "unit_conversion_rule_id": "UNIT001",
                    "from_unit": "degC",
                    "to_standard_unit": "K",
                    "scale_factor": 1.0,
                    "offset": 273.15,
                    "conversion_formula": "K = degC + 273.15",
                    "dimensionality": "temperature",
                    "notes": "",
                    "dictionary_version": "test",
                    "created_date": "2026-04-22",
                }
            ]
        ),
        "tbl_property_source_priority_rules_v1": pd.DataFrame(
            [{"priority_rule_id": "SP001", "property_scope": "environmental", "source_priority_rank": 1, "source_class": "official"}]
        ),
        "tbl_property_modeling_readiness_rules_v1": pd.DataFrame(
            [
                {
                    "readiness_rule_id": "ML001",
                    "canonical_property_id": "PROP_GWP100",
                    "canonical_feature_key": "environmental.gwp_100yr",
                    "use_as_ml_feature": 1,
                    "use_as_ml_target": 1,
                    "minimum_quality_score": 70,
                    "exclude_if_proxy_or_screening": 1,
                    "preferred_standard_unit": "dimensionless",
                    "normalization_recommendation": "identity",
                    "missing_value_strategy": "drop",
                    "notes": "strict test rule",
                }
            ]
        ),
        "tbl_property_governance_issues_v1": pd.DataFrame(
            [{"issue_id": "ISS001", "issue_type": "proxy", "severity": "high", "affected_record_count": 1, "recommended_action": "skip"}]
        ),
        "tbl_pure_properties_canonical_overlay_v1": pd.DataFrame(
            [
                {
                    "overlay_id": "OV001",
                    "record_id": "R001",
                    "substance_id": "SUB001",
                    "canonical_property_id": "PROP_GWP100",
                    "canonical_feature_key": "environmental.gwp_100yr",
                    "canonical_property_group": "environmental",
                    "canonical_property_name": "gwp_100yr",
                    "standard_unit": "dimensionless",
                    "standard_value_numeric": 1200.0,
                    "standard_value_text": "",
                    "source_id": "SRC_PROXY",
                    "source_priority_rank": 1,
                    "data_quality_score_100": 99,
                    "is_proxy_or_screening": 1,
                    "ml_use_status": "proxy_only",
                    "notes": "proxy row",
                },
                {
                    "overlay_id": "OV002",
                    "record_id": "R002",
                    "substance_id": "SUB001",
                    "canonical_property_id": "PROP_GWP100",
                    "canonical_feature_key": "environmental.gwp_100yr",
                    "canonical_property_group": "environmental",
                    "canonical_property_name": "gwp_100yr",
                    "standard_unit": "dimensionless",
                    "standard_value_numeric": 550.0,
                    "standard_value_text": "",
                    "source_id": "SRC_AUTH",
                    "source_priority_rank": 3,
                    "data_quality_score_100": 90,
                    "is_proxy_or_screening": 0,
                    "ml_use_status": "recommended_numeric_candidate",
                    "notes": "authoritative row",
                },
                {
                    "overlay_id": "OV003",
                    "record_id": "R003",
                    "substance_id": "SUB002",
                    "canonical_property_id": "PROP_TB",
                    "canonical_feature_key": "thermodynamic.normal_boiling_temperature",
                    "canonical_property_group": "thermodynamic",
                    "canonical_property_name": "normal_boiling_temperature",
                    "standard_unit": "K",
                    "standard_value_numeric": 250.0,
                    "standard_value_text": "",
                    "source_id": "SRC_AUTH",
                    "source_priority_rank": 3,
                    "data_quality_score_100": 88,
                    "is_proxy_or_screening": 0,
                    "ml_use_status": "recommended_numeric_candidate",
                    "notes": "tb row",
                },
                {
                    "overlay_id": "OV004",
                    "record_id": "R004",
                    "substance_id": "SUB002",
                    "canonical_property_id": "PROP_SAFETY",
                    "canonical_feature_key": "safety.safety_group",
                    "canonical_property_group": "safety",
                    "canonical_property_name": "safety_group",
                    "standard_unit": "dimensionless",
                    "standard_value_numeric": "",
                    "standard_value_text": "A2L",
                    "source_id": "SRC_AUTH",
                    "source_priority_rank": 3,
                    "data_quality_score_100": 88,
                    "is_proxy_or_screening": 0,
                    "ml_use_status": "recommended_numeric_candidate",
                    "notes": "safety row",
                },
            ]
        ),
        "tbl_mixtures": pd.DataFrame(
            [{"mixture_id": "MIX001", "refrigerant_number": "R-444A", "mixture_type": "binary", "safety_group": "A2L", "primary_source_id": "SRC_AUTH", "notes": ""}]
        ),
        "tbl_mixture_components": pd.DataFrame(
            [
                {"mixture_id": "MIX001", "sequence_no": 1, "component_refrigerant": "R-Existing", "mass_pct": 20.0, "source_id": "SRC_AUTH", "source_detail": ""},
                {"mixture_id": "MIX001", "sequence_no": 2, "component_refrigerant": "R-New", "mass_pct": 80.0, "source_id": "SRC_AUTH", "source_detail": ""},
            ]
        ),
    }

    bundle_path = tmp_path / "synthetic_bundle.zip"
    _write_bundle(bundle_path, tables)

    result = integrate_property_governance_bundle(
        bundle_path=bundle_path,
        output_root=tmp_path,
        seed_catalog=pd.DataFrame(
            [
                {
                    "seed_id": "seed_existing",
                    "r_number": "R-Existing",
                    "family": "HFC",
                    "query_name": "100-00-0",
                    "coverage_tier": "B",
                    "selection_role": "candidate",
                    "entity_scope": "refrigerant",
                    "model_inclusion": "yes",
                }
            ]
        ),
        molecule_core=pd.DataFrame(
            [
                {
                    "mol_id": f"mol_{existing['inchikey'].lower()}",
                    "seed_id": "seed_existing",
                    "family": "HFC",
                    "canonical_smiles": existing["canonical_smiles"],
                    "isomeric_smiles": existing["isomeric_smiles"],
                    "inchi": existing["inchi"],
                    "inchikey": existing["inchikey"],
                    "formula": existing["formula"],
                }
            ]
        ),
        alias_df=pd.DataFrame(
            [
                {"mol_id": f"mol_{existing['inchikey'].lower()}", "alias_type": "cas", "alias_value": "100-00-0", "is_primary": False, "source_name": "test"},
                {"mol_id": f"mol_{existing['inchikey'].lower()}", "alias_type": "r_number", "alias_value": "R-Existing", "is_primary": True, "source_name": "test"},
            ]
        ),
        parser_version="test-version",
        retrieved_at="2026-04-22T00:00:00+00:00",
    )

    assert result["bundle_present"] is True
    assert len(result["generated_seed_rows"]) == 1
    assert result["generated_seed_rows"].iloc[0]["r_number"] == "R-New"
    assert len(result["generated_molecule_rows"]) == 1
    assert len(result["legacy_property_rows"]) >= 2
    assert result["crosswalk"]["match_status"].value_counts().to_dict() == {
        "generated_new_seed": 1,
        "matched_existing": 1,
    }

    canonical_recommended = result["canonical_recommended"]
    selected_gwp = canonical_recommended.loc[
        canonical_recommended["canonical_feature_key"] == "environmental.gwp_100yr"
    ].iloc[0]
    assert selected_gwp["selected_source_id"] == "SRC_AUTH"
    assert bool(selected_gwp["is_proxy_or_screening"]) is False
    assert bool(selected_gwp["conflict_flag"]) is False
    assert bool(selected_gwp["source_divergence_flag"]) is True
    assert bool(selected_gwp["proxy_only_flag"]) is False
    canonical_recommended_strict = result["canonical_recommended_strict"]
    assert canonical_recommended_strict["canonical_feature_key"].tolist() == ["environmental.gwp_100yr"]
    assert bool(canonical_recommended_strict.iloc[0]["strict_accept"]) is True
    review_queue = result["canonical_review_queue"]
    queue_row = review_queue.loc[review_queue["canonical_feature_key"] == "environmental.gwp_100yr"].iloc[0]
    assert queue_row["review_reason"] == "source_divergence"
    assert queue_row["review_priority"] == "high"
    assert bool(queue_row["strict_accept"]) is True

    legacy_rows = pd.DataFrame(result["legacy_property_rows"])
    tb_row = legacy_rows.loc[legacy_rows["property_name"] == "boiling_point_c"].iloc[0]
    assert round(tb_row["value_num"], 2) == -23.15
    safety_row = legacy_rows.loc[legacy_rows["property_name"] == "ashrae_safety"].iloc[0]
    assert safety_row["value"] == "A2L"

    extension_manifest = result["extension_manifest"]
    source_manifest = pd.DataFrame(result["source_manifest_rows"])
    unit_conversion_rules = pd.read_parquet(tmp_path / "data" / "lake" / "gold" / "unit_conversion_rules.parquet")
    assert set(extension_manifest["table_name"]) == set(tables.keys())
    assert {"SRC_AUTH", "SRC_PROXY"}.issubset(set(source_manifest["source_id"]))
    assert unit_conversion_rules.loc[0, "standard_unit"] == "K"
    assert (tmp_path / "data" / "lake" / "silver" / "property_observation_canonical.parquet").exists()
    assert (tmp_path / "data" / "lake" / "gold" / "property_recommended_canonical.parquet").exists()
    assert (tmp_path / "data" / "lake" / "gold" / "property_recommended_canonical_strict.parquet").exists()
    assert (tmp_path / "data" / "lake" / "gold" / "property_recommended_canonical_review_queue.parquet").exists()


def test_integrate_property_governance_bundle_applies_high_confidence_curations(tmp_path: Path) -> None:
    tables = _minimal_unresolved_bundle_tables()
    bundle_path = tmp_path / "synthetic_bundle.zip"
    _write_bundle(bundle_path, tables)

    curation_path = tmp_path / "data" / "lake" / "raw" / "manual" / "property_governance_20260422_unresolved_curations.csv"
    _write_unresolved_curations(
        curation_path,
        [
            {
                "substance_id": "SUB092",
                "refrigerant_number": "R-C316",
                "cas_number": "356-18-3",
                "canonical_smiles": "FC1(F)C(F)(F)C(F)(Cl)C1(F)Cl",
                "isomeric_smiles": "FC1(F)C(F)(F)C(F)(Cl)C1(F)Cl",
                "inchi": "InChI=1S/C4Cl2F6/c5-1(7)2(6,8)4(11,12)3(1,9)10",
                "inchikey": "LMHAGAHDHRQIMB-UHFFFAOYSA-N",
                "resolution_source": "PubChem CID 9643 cross-checked against NIST refrigerant reference",
                "resolution_source_url": "https://pubchem.ncbi.nlm.nih.gov/compound/9643",
                "resolution_confidence": "high",
                "notes": "Resolved from authoritative external identifiers.",
            }
        ],
    )

    result = integrate_property_governance_bundle(
        bundle_path=bundle_path,
        output_root=tmp_path,
        seed_catalog=pd.DataFrame(columns=["seed_id", "r_number", "family"]),
        molecule_core=pd.DataFrame(columns=["mol_id", "seed_id", "family", "canonical_smiles", "isomeric_smiles", "inchi", "inchikey", "formula"]),
        alias_df=pd.DataFrame(columns=["mol_id", "alias_type", "alias_value", "is_primary", "source_name"]),
        parser_version="test-version",
        retrieved_at="2026-04-22T00:00:00+00:00",
        unresolved_curation_path=curation_path,
    )

    crosswalk_row = result["crosswalk"].iloc[0]
    assert crosswalk_row["match_status"] == "generated_new_seed"
    assert bool(crosswalk_row["external_resolution_applied"]) is True
    assert len(result["canonical_observation"]) == 1
    assert len(result["unresolved"]) == 0
    assert result["audit"]["crosswalk"]["external_resolution_count"] == 1


def test_integrate_property_governance_bundle_rejects_non_high_confidence_curations(tmp_path: Path) -> None:
    tables = _minimal_unresolved_bundle_tables()
    bundle_path = tmp_path / "synthetic_bundle.zip"
    _write_bundle(bundle_path, tables)

    curation_path = tmp_path / "data" / "lake" / "raw" / "manual" / "property_governance_20260422_unresolved_curations.csv"
    _write_unresolved_curations(
        curation_path,
        [
            {
                "substance_id": "SUB092",
                "refrigerant_number": "R-C316",
                "cas_number": "356-18-3",
                "canonical_smiles": "FC1(F)C(F)(F)C(F)(Cl)C1(F)Cl",
                "isomeric_smiles": "FC1(F)C(F)(F)C(F)(Cl)C1(F)Cl",
                "inchi": "InChI=1S/C4Cl2F6/c5-1(7)2(6,8)4(11,12)3(1,9)10",
                "inchikey": "LMHAGAHDHRQIMB-UHFFFAOYSA-N",
                "resolution_source": "PubChem CID 9643",
                "resolution_source_url": "https://pubchem.ncbi.nlm.nih.gov/compound/9643",
                "resolution_confidence": "medium",
                "notes": "Should fail because confidence is not high.",
            }
        ],
    )

    with pytest.raises(ValueError, match="resolution_confidence=high"):
        integrate_property_governance_bundle(
            bundle_path=bundle_path,
            output_root=tmp_path,
            seed_catalog=pd.DataFrame(columns=["seed_id", "r_number", "family"]),
            molecule_core=pd.DataFrame(columns=["mol_id", "seed_id", "family", "canonical_smiles", "isomeric_smiles", "inchi", "inchikey", "formula"]),
            alias_df=pd.DataFrame(columns=["mol_id", "alias_type", "alias_value", "is_primary", "source_name"]),
            parser_version="test-version",
            retrieved_at="2026-04-22T00:00:00+00:00",
            unresolved_curation_path=curation_path,
        )


def test_integrate_property_governance_bundle_rejects_conflicting_curations(tmp_path: Path) -> None:
    tables = _minimal_unresolved_bundle_tables()
    bundle_path = tmp_path / "synthetic_bundle.zip"
    _write_bundle(bundle_path, tables)

    curation_path = tmp_path / "data" / "lake" / "raw" / "manual" / "property_governance_20260422_unresolved_curations.csv"
    _write_unresolved_curations(
        curation_path,
        [
            {
                "substance_id": "SUB092",
                "refrigerant_number": "R-C316",
                "cas_number": "356-18-3",
                "canonical_smiles": "FC1(F)C(F)(F)C(F)(Cl)C1(F)Cl",
                "isomeric_smiles": "FC1(F)C(F)(F)C(F)(Cl)C1(F)Cl",
                "inchi": "InChI=1S/C4Cl2F6/c5-1(7)2(6,8)4(11,12)3(1,9)10",
                "inchikey": "WRONGINCHIKEY-UHFFFAOYSA-N",
                "resolution_source": "PubChem CID 9643",
                "resolution_source_url": "https://pubchem.ncbi.nlm.nih.gov/compound/9643",
                "resolution_confidence": "high",
                "notes": "Should fail because structure and InChIKey disagree.",
            }
        ],
    )

    with pytest.raises(ValueError, match="InChIKey mismatch"):
        integrate_property_governance_bundle(
            bundle_path=bundle_path,
            output_root=tmp_path,
            seed_catalog=pd.DataFrame(columns=["seed_id", "r_number", "family"]),
            molecule_core=pd.DataFrame(columns=["mol_id", "seed_id", "family", "canonical_smiles", "isomeric_smiles", "inchi", "inchikey", "formula"]),
            alias_df=pd.DataFrame(columns=["mol_id", "alias_type", "alias_value", "is_primary", "source_name"]),
            parser_version="test-version",
            retrieved_at="2026-04-22T00:00:00+00:00",
            unresolved_curation_path=curation_path,
        )


def test_select_canonical_recommended_only_flags_top_rank_conflicts() -> None:
    canonical_recommended = select_canonical_recommended(
        pd.DataFrame(
            [
                {
                    "mol_id": "mol_a",
                    "canonical_feature_key": "environmental.gwp_100yr",
                    "canonical_property_id": "PROP_ENV_GWP100",
                    "canonical_property_group": "environmental",
                    "canonical_property_name": "gwp_100yr",
                    "value": "100",
                    "value_num": 100.0,
                    "unit": "dimensionless",
                    "source_id": "SRC_TOP",
                    "source_name": "Top Source",
                    "quality_level": "manual_curated_reference",
                    "source_priority_rank": 1,
                    "data_quality_score_100": 95,
                    "is_proxy_or_screening": 0,
                    "ml_use_status": "recommended_numeric_candidate",
                },
                {
                    "mol_id": "mol_a",
                    "canonical_feature_key": "environmental.gwp_100yr",
                    "canonical_property_id": "PROP_ENV_GWP100",
                    "canonical_property_group": "environmental",
                    "canonical_property_name": "gwp_100yr",
                    "value": "130",
                    "value_num": 130.0,
                    "unit": "dimensionless",
                    "source_id": "SRC_LOWER",
                    "source_name": "Lower Source",
                    "quality_level": "primary_public_reference",
                    "source_priority_rank": 2,
                    "data_quality_score_100": 90,
                    "is_proxy_or_screening": 0,
                    "ml_use_status": "recommended_numeric_candidate",
                },
            ]
        )
    )

    row = canonical_recommended.iloc[0]
    assert bool(row["conflict_flag"]) is False
    assert bool(row["source_divergence_flag"]) is True


def test_build_canonical_review_queue_marks_proxy_only_selected_rows() -> None:
    canonical_recommended = pd.DataFrame(
        [
            {
                "mol_id": "mol_proxy",
                "canonical_feature_key": "thermodynamic.critical_density",
                "canonical_property_id": "PROP_CRIT_DENS",
                "canonical_property_group": "thermodynamic",
                "canonical_property_name": "critical_density",
                "value": "480",
                "value_num": 480.0,
                "unit": "kg/m3",
                "selected_source_id": "SRC_PROXY",
                "selected_source_name": "Proxy Source",
                "selected_quality_level": "manual_curated_reference",
                "source_priority_rank": 1,
                "data_quality_score_100": 95,
                "is_proxy_or_screening": True,
                "ml_use_status": "proxy_only",
                "proxy_only_flag": True,
                "nonproxy_candidate_count": 0,
                "top_rank_source_count": 1,
                "source_divergence_flag": False,
                "source_divergence_detail": "",
                "source_count": 1,
                "conflict_flag": False,
                "conflict_detail": "",
            }
        ]
    )
    readiness = pd.DataFrame(
        [
            {
                "readiness_rule_id": "RULE001",
                "canonical_property_id": "PROP_CRIT_DENS",
                "canonical_feature_key": "thermodynamic.critical_density",
                "use_as_ml_feature": 1,
                "use_as_ml_target": 0,
                "minimum_quality_score": 80,
                "exclude_if_proxy_or_screening": 1,
                "preferred_standard_unit": "kg/m3",
                "normalization_recommendation": "identity",
                "missing_value_strategy": "drop",
                "notes": "Proxy rows should be reviewed.",
            }
        ]
    )

    queue = build_canonical_recommended_review_queue(
        canonical_recommended=canonical_recommended,
        readiness_rules=readiness,
    )

    row = queue.iloc[0]
    assert row["review_reason"] == "proxy_selected"
    assert row["review_priority"] == "medium"
    assert row["strict_rejection_reason"] == "proxy_selected"
    assert bool(row["strict_accept"]) is False


def test_proxy_acceptance_rules_promote_proxy_only_rows_into_strict(tmp_path: Path) -> None:
    canonical_recommended = pd.DataFrame(
        [
            {
                "mol_id": "mol_proxy",
                "canonical_feature_key": "molecular_descriptor.acentric_factor",
                "canonical_property_id": "PROP_MOL_ACENTRIC",
                "canonical_property_group": "molecular_descriptor",
                "canonical_property_name": "acentric_factor",
                "value": "0.252535",
                "value_num": 0.252535,
                "unit": "dimensionless",
                "selected_source_id": "SRC023",
                "selected_source_name": "CoolProp 7.2.0 High-Level API / HEOS backend",
                "selected_quality_level": "primary_public_reference",
                "source_priority_rank": 2,
                "data_quality_score_100": 80,
                "is_proxy_or_screening": True,
                "ml_use_status": "recommended_numeric_candidate",
                "proxy_only_flag": True,
                "nonproxy_candidate_count": 0,
                "top_rank_source_count": 1,
                "source_divergence_flag": False,
                "source_divergence_detail": "",
                "source_count": 1,
                "conflict_flag": False,
                "conflict_detail": "",
            }
        ]
    )
    readiness = pd.DataFrame(
        [
            {
                "readiness_rule_id": "RULE001",
                "canonical_property_id": "PROP_MOL_ACENTRIC",
                "canonical_feature_key": "molecular_descriptor.acentric_factor",
                "use_as_ml_feature": 1,
                "use_as_ml_target": 0,
                "minimum_quality_score": 70,
                "exclude_if_proxy_or_screening": 1,
                "preferred_standard_unit": "dimensionless",
                "normalization_recommendation": "identity",
                "missing_value_strategy": "drop",
                "notes": "Proxy-only CoolProp values may be admitted through policy.",
            }
        ]
    )
    rule_path = tmp_path / "data" / "lake" / "raw" / "manual" / "property_governance_20260422_proxy_acceptance_rules.csv"
    _write_proxy_acceptance_rules(
        rule_path,
        [
            {
                "proxy_policy_id": "POLICY_COOLPROP_ACENTRIC",
                "canonical_feature_key": "molecular_descriptor.acentric_factor",
                "selected_source_id": "SRC023",
                "allow_in_strict_if_proxy_only": 1,
                "rationale": "Allow proxy-only CoolProp acentric factors when no non-proxy candidate exists.",
                "notes": "",
            }
        ],
    )

    rules = load_property_governance_proxy_acceptance_rules(
        rule_path=rule_path,
        canonical_recommended=canonical_recommended,
    )
    strict = select_canonical_recommended_strict(
        canonical_recommended=canonical_recommended,
        readiness_rules=readiness,
        proxy_acceptance_rules=rules,
    )
    queue = build_canonical_recommended_review_queue(
        canonical_recommended=canonical_recommended,
        readiness_rules=readiness,
        proxy_acceptance_rules=rules,
    )

    assert len(strict) == 1
    strict_row = strict.iloc[0]
    assert bool(strict_row["strict_accept"]) is True
    assert strict_row["strict_accept_basis"] == "proxy_only_policy"
    assert strict_row["proxy_policy_id"] == "POLICY_COOLPROP_ACENTRIC"
    assert queue.empty


def test_proxy_acceptance_rules_reject_non_proxy_only_targets(tmp_path: Path) -> None:
    canonical_recommended = pd.DataFrame(
        [
            {
                "mol_id": "mol_bad",
                "canonical_feature_key": "thermodynamic.critical_temperature",
                "canonical_property_id": "PROP_CRIT_T",
                "canonical_property_group": "thermodynamic",
                "canonical_property_name": "critical_temperature",
                "value": "309.52",
                "value_num": 309.52,
                "unit": "K",
                "selected_source_id": "SRC023",
                "selected_source_name": "CoolProp 7.2.0 High-Level API / HEOS backend",
                "selected_quality_level": "primary_public_reference",
                "source_priority_rank": 2,
                "data_quality_score_100": 80,
                "is_proxy_or_screening": True,
                "ml_use_status": "recommended_numeric_candidate",
                "proxy_only_flag": False,
                "nonproxy_candidate_count": 1,
                "top_rank_source_count": 1,
                "source_divergence_flag": False,
                "source_divergence_detail": "",
                "source_count": 2,
                "conflict_flag": False,
                "conflict_detail": "",
            }
        ]
    )
    rule_path = tmp_path / "data" / "lake" / "raw" / "manual" / "property_governance_20260422_proxy_acceptance_rules.csv"
    _write_proxy_acceptance_rules(
        rule_path,
        [
            {
                "proxy_policy_id": "POLICY_BAD",
                "canonical_feature_key": "thermodynamic.critical_temperature",
                "selected_source_id": "SRC023",
                "allow_in_strict_if_proxy_only": 1,
                "rationale": "Should fail because a non-proxy candidate still exists.",
                "notes": "",
            }
        ],
    )

    with pytest.raises(ValueError, match="non-proxy candidates"):
        load_property_governance_proxy_acceptance_rules(
            rule_path=rule_path,
            canonical_recommended=canonical_recommended,
        )


def test_load_canonical_review_decisions_close_resolved_review_rows(tmp_path: Path) -> None:
    canonical_recommended = pd.DataFrame(
        [
            {
                "mol_id": "mol_review",
                "canonical_feature_key": "environmental.gwp_100yr",
                "canonical_property_id": "PROP_ENV_GWP100",
                "canonical_property_group": "environmental",
                "canonical_property_name": "gwp_100yr",
                "value": "1500",
                "value_num": 1500.0,
                "unit": "dimensionless",
                "selected_source_id": "SRC059",
                "selected_source_name": "IPCC AR6",
                "selected_quality_level": "manual_curated_reference",
                "source_priority_rank": 1,
                "data_quality_score_100": 95,
                "is_proxy_or_screening": False,
                "ml_use_status": "recommended_numeric_candidate",
                "proxy_only_flag": False,
                "nonproxy_candidate_count": 2,
                "top_rank_source_count": 1,
                "source_divergence_flag": True,
                "source_divergence_detail": "numeric spread 1330.0..1500.0",
                "source_count": 2,
                "conflict_flag": False,
                "conflict_detail": "",
            }
        ]
    )
    readiness = pd.DataFrame(
        [
            {
                "readiness_rule_id": "RULE001",
                "canonical_property_id": "PROP_ENV_GWP100",
                "canonical_feature_key": "environmental.gwp_100yr",
                "use_as_ml_feature": 1,
                "use_as_ml_target": 1,
                "minimum_quality_score": 80,
                "exclude_if_proxy_or_screening": 1,
                "preferred_standard_unit": "dimensionless",
                "normalization_recommendation": "identity",
                "missing_value_strategy": "drop",
                "notes": "Divergence row still uses the selected source.",
            }
        ]
    )
    decision_path = tmp_path / "data" / "lake" / "raw" / "manual" / "property_governance_20260422_canonical_review_decisions.csv"
    _write_canonical_review_decisions(
        decision_path,
        [
            {
                "mol_id": "mol_review",
                "canonical_feature_key": "environmental.gwp_100yr",
                "review_reason": "source_divergence",
                "decision_action": "accept_selected_source",
                "expected_selected_source_id": "SRC059",
                "expected_selected_value": "1500",
                "resolution_basis": "Accept the higher-priority governed source.",
                "resolution_source_url": "https://example.test/src059",
                "notes": "",
            }
        ],
    )

    decisions = load_property_governance_canonical_review_decisions(
        decision_path=decision_path,
        canonical_recommended=canonical_recommended,
    )
    queue = build_canonical_recommended_review_queue(
        canonical_recommended=canonical_recommended,
        readiness_rules=readiness,
        review_decisions=decisions,
    )

    assert len(decisions) == 1
    assert queue.empty


def test_load_canonical_review_decisions_rejects_stale_selected_source(tmp_path: Path) -> None:
    canonical_recommended = pd.DataFrame(
        [
            {
                "mol_id": "mol_review",
                "canonical_feature_key": "environmental.atmospheric_lifetime",
                "canonical_property_id": "PROP_ENV_LIFE",
                "canonical_property_group": "environmental",
                "canonical_property_name": "atmospheric_lifetime",
                "value": "1.5",
                "value_num": 1.5,
                "unit": "year",
                "selected_source_id": "SRC005",
                "selected_source_name": "Ozone Assessment",
                "selected_quality_level": "manual_curated_reference",
                "source_priority_rank": 1,
                "data_quality_score_100": 95,
                "is_proxy_or_screening": False,
                "ml_use_status": "recommended_numeric_candidate",
                "proxy_only_flag": False,
                "nonproxy_candidate_count": 2,
                "top_rank_source_count": 2,
                "source_divergence_flag": True,
                "source_divergence_detail": "numeric spread 1.5..1.6",
                "source_count": 2,
                "conflict_flag": True,
                "conflict_detail": "numeric spread 1.5..1.6",
            }
        ]
    )
    decision_path = tmp_path / "data" / "lake" / "raw" / "manual" / "property_governance_20260422_canonical_review_decisions.csv"
    _write_canonical_review_decisions(
        decision_path,
        [
            {
                "mol_id": "mol_review",
                "canonical_feature_key": "environmental.atmospheric_lifetime",
                "review_reason": "top_rank_conflict",
                "decision_action": "accept_selected_source",
                "expected_selected_source_id": "SRC999",
                "expected_selected_value": "1.5",
                "resolution_basis": "This should fail because the selected source changed.",
                "resolution_source_url": "",
                "notes": "",
            }
        ],
    )

    with pytest.raises(ValueError, match="selected source mismatch"):
        load_property_governance_canonical_review_decisions(
            decision_path=decision_path,
            canonical_recommended=canonical_recommended,
        )


def test_load_canonical_review_decisions_close_below_minimum_rows_out_of_strict(tmp_path: Path) -> None:
    canonical_recommended = pd.DataFrame(
        [
            {
                "mol_id": "mol_low_quality",
                "canonical_feature_key": "thermodynamic.critical_density",
                "canonical_property_id": "PROP_CRIT_DENS",
                "canonical_property_group": "thermodynamic",
                "canonical_property_name": "critical_density",
                "value": "574.297488917131",
                "value_num": 574.297488917131,
                "unit": "kg/m3",
                "selected_source_id": "SRC052",
                "selected_source_name": "P2 Backlog Clearance",
                "selected_quality_level": "screening",
                "source_priority_rank": 1,
                "data_quality_score_100": 35,
                "is_proxy_or_screening": True,
                "ml_use_status": "screening_proxy",
                "proxy_only_flag": True,
                "nonproxy_candidate_count": 0,
                "top_rank_source_count": 1,
                "source_divergence_flag": False,
                "source_divergence_detail": "",
                "source_count": 1,
                "conflict_flag": False,
                "conflict_detail": "",
            }
        ]
    )
    readiness = pd.DataFrame(
        [
            {
                "readiness_rule_id": "RULE_CRIT_DENS",
                "canonical_property_id": "PROP_CRIT_DENS",
                "canonical_feature_key": "thermodynamic.critical_density",
                "use_as_ml_feature": 1,
                "use_as_ml_target": 1,
                "minimum_quality_score": 70,
                "exclude_if_proxy_or_screening": 1,
                "preferred_standard_unit": "kg/m3",
                "normalization_recommendation": "zscore",
                "missing_value_strategy": "impute",
                "notes": "Synthetic low-quality queue row.",
            }
        ]
    )
    decision_path = tmp_path / "data" / "lake" / "raw" / "manual" / "property_governance_20260422_canonical_review_decisions.csv"
    _write_canonical_review_decisions(
        decision_path,
        [
            {
                "mol_id": "mol_low_quality",
                "canonical_feature_key": "thermodynamic.critical_density",
                "review_reason": "below_minimum_quality",
                "decision_action": "accept_out_of_strict",
                "expected_selected_source_id": "SRC052",
                "expected_selected_value": "574.297488917131",
                "resolution_basis": "Reviewed and intentionally retained outside strict output.",
                "resolution_source_url": "",
                "notes": "",
            }
        ],
    )

    decisions = load_property_governance_canonical_review_decisions(
        decision_path=decision_path,
        canonical_recommended=canonical_recommended,
    )
    queue = build_canonical_recommended_review_queue(
        canonical_recommended=canonical_recommended,
        readiness_rules=readiness,
        review_decisions=decisions,
    )

    assert len(decisions) == 1
    assert decisions.iloc[0]["decision_action"] == "accept_out_of_strict"
    assert queue.empty


def test_coerce_governed_value_num_maps_ozone_flag_booleans() -> None:
    assert _coerce_governed_value_num(
        canonical_feature_key="environmental.ozone_depleting_flag",
        value="No",
        value_num=None,
    ) == 0.0
    assert _coerce_governed_value_num(
        canonical_feature_key="environmental.ozone_depleting_flag",
        value="Yes",
        value_num=None,
    ) == 1.0
    assert _coerce_governed_value_num(
        canonical_feature_key="environmental.atmospheric_lifetime",
        value="few days",
        value_num=None,
    ) is None
