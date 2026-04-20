from __future__ import annotations

import csv
import gzip
import importlib.util
from pathlib import Path

import pandas as pd

from r_physgen_db import pipeline
from r_physgen_db.chemistry import standardize_smiles
from r_physgen_db.sources import pubchem_bulk as pubchem_bulk_module
from r_physgen_db.sources.pubchem_bulk import (
    build_pubchem_candidate_pool,
    build_pubchem_candidate_pool_streaming,
    export_tierd_seed_rows,
)


ROOT = Path(__file__).resolve().parents[1]


def _load_seed_generator_module():
    path = ROOT / "pipelines" / "generate_wave2_seed_catalog.py"
    spec = importlib.util.spec_from_file_location("generate_wave2_seed_catalog", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _write_gzip_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        handle.write("\n".join(lines))
        handle.write("\n")


def _extras_record(cid: str, smiles: str) -> dict[str, str]:
    standardized = standardize_smiles(smiles)
    return {
        "cid": cid,
        "smiles": standardized["isomeric_smiles"],
        "inchi": standardized["inchi"],
        "inchikey": standardized["inchikey"],
        "formula": standardized["formula"],
        "mass": f"{standardized['molecular_weight']:.6f}",
    }


def test_download_pubchem_extras_resumes_partial_files_and_skips_existing(monkeypatch, tmp_path: Path) -> None:
    class DummyResponse:
        def __init__(self, status_code: int, payload: bytes) -> None:
            self.status_code = status_code
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, chunk_size: int):
            for start in range(0, len(self._payload), chunk_size):
                yield self._payload[start : start + chunk_size]

    class DummySession:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}
            self.calls: list[dict[str, object]] = []

        def get(self, url: str, stream: bool, timeout: int, headers: dict[str, str] | None = None):
            header_map = headers or {}
            self.calls.append({"url": url, "headers": dict(header_map), "timeout": timeout, "stream": stream})
            if header_map.get("Range") == "bytes=3-":
                return DummyResponse(206, b"def")
            raise AssertionError(f"unexpected request headers: {header_map}")

    monkeypatch.setattr(pubchem_bulk_module, "DEFAULT_EXTRAS_FILES", {"smiles": "CID-SMILES.gz"})

    target_dir = tmp_path / "extras"
    target_dir.mkdir(parents=True, exist_ok=True)
    partial_path = target_dir / "CID-SMILES.gz.part"
    partial_path.write_bytes(b"abc")

    session = DummySession()
    paths = pubchem_bulk_module.download_pubchem_extras(
        target_dir,
        session=session,
        file_sleep_seconds=0.0,
        retry_sleep_seconds=0.0,
    )

    output_path = paths["smiles"]
    assert output_path.read_bytes() == b"abcdef"
    assert partial_path.exists() is False
    assert session.calls[0]["headers"] == {"Range": "bytes=3-"}

    second_session = DummySession()
    second_paths = pubchem_bulk_module.download_pubchem_extras(
        target_dir,
        session=second_session,
        file_sleep_seconds=0.0,
        retry_sleep_seconds=0.0,
    )
    assert second_paths["smiles"] == output_path
    assert second_session.calls == []


def test_build_pubchem_candidate_pool_filters_and_annotates_records(tmp_path: Path) -> None:
    records = {
        "1001": _extras_record("1001", "F/C=C/F"),
        "1002": _extras_record("1002", "C[N+](C)(C)C"),
        "1003": _extras_record("1003", "CCCCCCCC"),
        "1004": _extras_record("1004", "CC.O"),
        "1005": _extras_record("1005", "C[Si](C)C"),
        "1006": _extras_record("1006", "C=C(C(F)(F)F)F"),
    }

    smiles_path = tmp_path / "CID-SMILES.gz"
    inchikey_path = tmp_path / "CID-InChI-Key.gz"
    mass_path = tmp_path / "CID-Mass.gz"
    component_path = tmp_path / "CID-Component.gz"
    synonym_path = tmp_path / "CID-Synonym-filtered.gz"

    _write_gzip_lines(smiles_path, [f"{cid}\t{record['smiles']}" for cid, record in records.items()])
    _write_gzip_lines(inchikey_path, [f"{cid}\t{record['inchi']}\t{record['inchikey']}" for cid, record in records.items()])
    _write_gzip_lines(mass_path, [f"{cid}\t{record['formula']}\t{record['mass']}\t{record['mass']}" for cid, record in records.items()])
    _write_gzip_lines(
        component_path,
        [
            "1001\t1001",
            "1002\t1002",
            "1003\t1003",
            "1004\t1004\t9999",
            "1005\t1005",
            "1006\t1006",
        ],
    )
    _write_gzip_lines(
        synonym_path,
        [
            "1001\t(E)-1,2-difluoroethene",
            "1001\t1630-78-0",
            "1006\t2,3,3,3-Tetrafluoroprop-1-ene",
            "1006\t754-12-1",
        ],
    )

    existing = pd.DataFrame(
        [
            {
                "mol_id": "mol_existing",
                "inchikey": records["1006"]["inchikey"],
                "inchikey_first_block": records["1006"]["inchikey"].split("-")[0],
            }
        ]
    )

    candidate_pool, audit = build_pubchem_candidate_pool(
        smiles_path=smiles_path,
        inchikey_path=inchikey_path,
        mass_path=mass_path,
        component_path=component_path,
        synonym_path=synonym_path,
        existing_molecule_core=existing,
    )

    assert candidate_pool["cid"].tolist() == ["1001", "1006"]
    passed = candidate_pool.set_index("cid")
    assert bool(passed.loc["1001", "passed_hard_filters"]) is True
    assert passed.loc["1001", "total_atom_count"] <= 18
    assert bool(passed.loc["1001", "has_c_c_double_bond"]) is True
    assert passed.loc["1001", "volatility_status"] == "unknown"
    assert bool(passed.loc["1006", "existing_full_inchikey_match"]) is True
    assert bool(passed.loc["1006", "existing_first_block_match"]) is True

    failures = audit.set_index("cid")
    assert bool(failures.loc["1002", "passed_hard_filters"]) is False
    assert "non_neutral" in failures.loc["1002", "failure_reasons"]
    assert "total_atom_count_gt_18" in failures.loc["1003", "failure_reasons"]
    assert "multi_component" in failures.loc["1004", "failure_reasons"]
    assert "disallowed_elements" in failures.loc["1005", "failure_reasons"]


def test_build_pubchem_candidate_pool_streaming_prefilters_with_duckdb(tmp_path: Path) -> None:
    records = {
        "1001": _extras_record("1001", "F/C=C/F"),
        "1003": _extras_record("1003", "CCCCCCCC"),
        "1004": _extras_record("1004", "CC.O"),
        "1005": _extras_record("1005", "C[Si](C)C"),
        "1006": _extras_record("1006", "C=C(C(F)(F)F)F"),
    }

    smiles_path = tmp_path / "CID-SMILES.gz"
    inchikey_path = tmp_path / "CID-InChI-Key.gz"
    mass_path = tmp_path / "CID-Mass.gz"
    component_path = tmp_path / "CID-Component.gz"
    synonym_path = tmp_path / "CID-Synonym-filtered.gz"

    _write_gzip_lines(smiles_path, [f"{cid}\t{record['smiles']}" for cid, record in records.items()])
    _write_gzip_lines(inchikey_path, [f"{cid}\t{record['inchi']}\t{record['inchikey']}" for cid, record in records.items()])
    _write_gzip_lines(mass_path, [f"{cid}\t{record['formula']}\t{record['mass']}\t{record['mass']}" for cid, record in records.items()])
    _write_gzip_lines(
        component_path,
        [
            "1001\t1001",
            "1003\t1003",
            "1004\t1004\t9999",
            "1005\t1005",
            "1006\t1006",
        ],
    )
    _write_gzip_lines(
        synonym_path,
        [
            "1001\t(E)-1,2-difluoroethene",
            "1001\t1630-78-0",
            "1006\t2,3,3,3-Tetrafluoroprop-1-ene",
            "1006\t754-12-1",
        ],
    )

    existing = pd.DataFrame(
        [
            {
                "mol_id": "mol_existing",
                "inchikey": records["1006"]["inchikey"],
                "inchikey_first_block": records["1006"]["inchikey"].split("-")[0],
            }
        ]
    )

    candidate_pool, audit = build_pubchem_candidate_pool_streaming(
        smiles_path=smiles_path,
        inchikey_path=inchikey_path,
        mass_path=mass_path,
        component_path=component_path,
        synonym_path=synonym_path,
        existing_molecule_core=existing,
    )

    assert candidate_pool["cid"].tolist() == ["1001", "1006"]
    passed = candidate_pool.set_index("cid")
    assert bool(passed.loc["1006", "existing_full_inchikey_match"]) is True
    assert bool(passed.loc["1006", "existing_first_block_match"]) is True
    assert passed.loc["1001", "primary_name"] == "(E)-1,2-difluoroethene"

    failures = audit.set_index("cid")
    assert "1003" not in failures.index
    assert "1005" not in failures.index
    assert bool(failures.loc["1004", "passed_hard_filters"]) is False
    assert failures.loc["1004", "failure_reasons"] == "multi_component"


def test_export_tierd_seed_rows_uses_inventory_defaults_and_skips_full_duplicates() -> None:
    candidate_pool = pd.DataFrame(
        [
            {
                "cid": "2001",
                "title": "Candidate 2001",
                "primary_name": "Candidate 2001",
                "canonical_smiles": "C=C(F)F",
                "isomeric_smiles": "F/C=C/F",
                "formula": "C2H2F2",
                "molecular_weight": 64.03,
                "existing_full_inchikey_match": False,
                "existing_first_block_match": False,
                "has_halogen": True,
                "has_c_c_double_bond": True,
                "ring_count": 0,
                "total_atom_count": 6,
                "carbon_count": 2,
            },
            {
                "cid": "2002",
                "title": "Duplicate Candidate",
                "primary_name": "Duplicate Candidate",
                "canonical_smiles": "C=C(C(F)(F)F)F",
                "isomeric_smiles": "C=C(C(F)(F)F)F",
                "formula": "C3H2F4",
                "molecular_weight": 114.04,
                "existing_full_inchikey_match": True,
                "existing_first_block_match": True,
                "has_halogen": True,
                "has_c_c_double_bond": True,
                "ring_count": 0,
                "total_atom_count": 9,
                "carbon_count": 3,
            },
            {
                "cid": "2003",
                "title": "Isomer Candidate",
                "primary_name": "Isomer Candidate",
                "canonical_smiles": "FC(F)=CF",
                "isomeric_smiles": "F/C(F)=C/F",
                "formula": "C2HF3",
                "molecular_weight": 82.02,
                "existing_full_inchikey_match": False,
                "existing_first_block_match": True,
                "has_halogen": True,
                "has_c_c_double_bond": True,
                "ring_count": 0,
                "total_atom_count": 6,
                "carbon_count": 2,
            },
        ]
    )

    exported = export_tierd_seed_rows(candidate_pool, limit=10)

    assert [row["seed_id"] for row in exported] == ["tierd_pubchem_2001", "tierd_pubchem_2003"]
    assert all(row["pubchem_query_type"] == "cid" for row in exported)
    assert all(row["selection_role"] == "inventory" for row in exported)
    assert all(row["coverage_tier"] == "D" for row in exported)
    assert all(row["entity_scope"] == "candidate" for row in exported)
    assert all(row["model_inclusion"] == "no" for row in exported)
    assert all(row["source_bundle"] == "pubchem_bulk" for row in exported)
    assert all(row["coolprop_support_expected"] == "no" for row in exported)
    assert all(row["regulatory_priority"] == "low" for row in exported)


def test_generate_wave2_seed_catalog_merges_generated_pubchem_tierd_candidates(tmp_path: Path, monkeypatch) -> None:
    module = _load_seed_generator_module()

    generated_path = tmp_path / "pubchem_tierd_candidates.csv"
    with generated_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=module.FIELDNAMES)
        writer.writeheader()
        writer.writerow(
            {
                "seed_id": "tierd_pubchem_999001",
                "r_number": "",
                "family": "Candidate",
                "query_name": "999001",
                "pubchem_query_type": "cid",
                "nist_query": "",
                "nist_query_type": "name",
                "coolprop_fluid": "",
                "priority_tier": "4",
                "selection_role": "inventory",
                "coverage_tier": "D",
                "source_bundle": "pubchem_bulk",
                "coolprop_support_expected": "no",
                "regulatory_priority": "low",
                "entity_scope": "candidate",
                "model_inclusion": "no",
                "notes": "generated from PubChem bulk pool",
            }
        )

    monkeypatch.setattr(module, "GENERATED_PUBCHEM_TIERD_CANDIDATES", generated_path)

    rows = module.build_rows()

    matching = [row for row in rows if row["seed_id"] == "tierd_pubchem_999001"]
    assert len(matching) == 1
    assert matching[0]["source_bundle"] == "pubchem_bulk"
    assert matching[0]["coverage_tier"] == "D"


def test_build_dataset_uses_bulk_candidate_pool_without_live_pubchem(tmp_path: Path, monkeypatch) -> None:
    record = _extras_record("3001", "F/C=C/F")

    data_root = tmp_path / "data"
    raw_manual = data_root / "raw" / "manual"
    raw_generated = data_root / "raw" / "generated"
    bronze = data_root / "bronze"
    silver = data_root / "silver"
    gold = data_root / "gold"
    index_dir = data_root / "index"
    raw_pubchem = data_root / "raw" / "pubchem"
    raw_nist = data_root / "raw" / "nist_webbook"
    raw_epa = data_root / "raw" / "epa"
    raw_coolprop = data_root / "raw" / "coolprop"

    for path in [raw_manual, raw_generated, bronze, silver, gold, index_dir, raw_pubchem, raw_nist, raw_epa, raw_coolprop]:
        path.mkdir(parents=True, exist_ok=True)

    candidate_pool = pd.DataFrame(
        [
            {
                "cid": "3001",
                "primary_name": "(E)-1,2-difluoroethene",
                "title": "(E)-1,2-difluoroethene",
                "synonyms": ["1630-78-0", "(E)-1,2-difluoroethene"],
                "canonical_smiles": standardize_smiles("F/C=C/F")["canonical_smiles"],
                "isomeric_smiles": record["smiles"],
                "inchi": record["inchi"],
                "inchikey": record["inchikey"],
                "formula": record["formula"],
                "molecular_weight": float(record["mass"]),
                "total_atom_count": 6,
                "heavy_atom_count": 4,
                "passed_hard_filters": True,
                "existing_full_inchikey_match": False,
                "existing_first_block_match": False,
                "volatility_status": "unknown",
            }
        ]
    )
    candidate_pool_path = bronze / "pubchem_candidate_pool.parquet"
    candidate_pool.to_parquet(candidate_pool_path, index=False)

    exported_rows = export_tierd_seed_rows(candidate_pool, limit=10)
    generated_csv = raw_generated / "pubchem_tierd_candidates.csv"
    module = _load_seed_generator_module()
    with generated_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=module.FIELDNAMES)
        writer.writeheader()
        writer.writerows(exported_rows)

    seed_catalog_path = raw_manual / "seed_catalog.csv"
    with seed_catalog_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=module.FIELDNAMES)
        writer.writeheader()
        writer.writerows(exported_rows)

    (raw_manual / "refrigerant_inventory.csv").write_text(
        "seed_id,r_number,family,query_name,pubchem_query_type,nist_query,nist_query_type,coolprop_fluid,notes\n",
        encoding="utf-8",
    )
    (raw_manual / "manual_property_observations.csv").write_text(
        "seed_id,r_number,property_name,value,value_num,unit,temperature,pressure,phase,source_type,source_name,source_url,method,uncertainty,quality_level,assessment_version,time_horizon,year,notes\n",
        encoding="utf-8",
    )
    (raw_manual / "coolprop_aliases.yaml").write_text("mappings: {}\n", encoding="utf-8")

    path_map = {
        "raw_root": data_root / "raw",
        "raw_pubchem": raw_pubchem,
        "raw_nist": raw_nist,
        "raw_epa": raw_epa,
        "raw_coolprop_meta": raw_coolprop / "session_metadata.json",
        "seed_catalog": seed_catalog_path,
        "refrigerant_inventory": raw_manual / "refrigerant_inventory.csv",
        "manual_observations": raw_manual / "manual_property_observations.csv",
        "manual_observations_dir": raw_manual / "observations",
        "coolprop_aliases": raw_manual / "coolprop_aliases.yaml",
        "raw_generated_pubchem_tierd_candidates": generated_csv,
        "bronze_source_manifest": bronze / "source_manifest.parquet",
        "bronze_pending_sources": bronze / "pending_sources.parquet",
        "bronze_seed_resolution": bronze / "seed_resolution.parquet",
        "bronze_pubchem_candidate_pool": candidate_pool_path,
        "bronze_pubchem_candidate_filter_audit": bronze / "pubchem_candidate_filter_audit.parquet",
        "silver_molecule_core": silver / "molecule_core.parquet",
        "silver_molecule_alias": silver / "molecule_alias.parquet",
        "silver_property_observation": silver / "property_observation.parquet",
        "silver_regulatory_status": silver / "regulatory_status.parquet",
        "silver_qc_issues": silver / "qc_issues.parquet",
        "gold_property_recommended": gold / "property_recommended.parquet",
        "gold_structure_features": gold / "structure_features.parquet",
        "gold_molecule_master": gold / "molecule_master.parquet",
        "gold_property_matrix": gold / "property_matrix.parquet",
        "gold_model_index": gold / "model_dataset_index.parquet",
        "gold_model_ready": gold / "model_ready.parquet",
        "gold_quality_report": gold / "quality_report.json",
        "duckdb_path": index_dir / "r_physgen_v2.duckdb",
    }

    class DummyCoolPropSource:
        version = "test"

        def session_metadata(self) -> dict[str, str]:
            return {"version": "test"}

        def generate_observations(self, mol_id: str, fluid: str, source_id: str) -> list[dict[str, str]]:
            raise AssertionError("CoolProp should not be called for this bulk candidate test")

    class DummyCompToxClient:
        enabled = False

        def pending_record(self, seed_id: str, r_number: str, mol_id: str) -> dict[str, str]:
            return {
                "seed_id": seed_id,
                "r_number": r_number,
                "mol_id": mol_id,
                "requested_source": "CompTox",
                "status": "pending",
                "detail": "",
                "required_env_var": "COMPTOX_API_KEY",
            }

    monkeypatch.setattr(pipeline, "_paths", lambda: path_map)
    monkeypatch.setattr(pipeline, "_fetch_global_sources", lambda **kwargs: {
        "epa_gwp_reference_df": pd.DataFrame(),
        "epa_gwp_reference_source_id": "source_epa_technology_transitions_gwp",
        "epa_ods_df": pd.DataFrame(),
        "epa_ods_source_id": "source_epa_ods",
        "epa_snap_frames": [],
    })
    monkeypatch.setattr(pipeline, "CoolPropSource", DummyCoolPropSource)
    monkeypatch.setattr(pipeline, "CompToxClient", DummyCompToxClient)
    monkeypatch.setattr(pipeline.PubChemClient, "resolve_compound", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("live PubChem should not be called")))
    monkeypatch.setattr(pipeline.PubChemClient, "fetch_synonyms", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("live PubChem should not be called")))

    report = pipeline.build_dataset(refresh_remote=False)
    molecule_core = pd.read_parquet(path_map["silver_molecule_core"])

    assert report["seed_catalog_count"] == 1
    assert molecule_core["seed_id"].tolist() == ["tierd_pubchem_3001"]
    assert molecule_core["pubchem_cid"].tolist() == ["3001"]
