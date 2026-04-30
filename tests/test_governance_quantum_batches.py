from __future__ import annotations

from pathlib import Path

import pandas as pd

from r_physgen_db.governance_quantum_batches import (
    build_governance_mapping,
    governance_dft_request_id,
    governance_xtb_request_id,
    materialize_governance_quantum_batches,
)
from r_physgen_db.quantum_pilot import QUANTUM_CANONICAL_FEATURE_KEYS


def test_governance_mapping_report_preserves_unmapped_substances() -> None:
    queue = _queue([("SUB001", "R-1"), ("SUB002", "R-2"), ("SUB003", "R-3")])
    seed_catalog = pd.DataFrame(
        [
            {"seed_id": "seed_mapped", "r_number": "R-1", "coverage_tier": "A", "model_inclusion": "yes"},
            {"seed_id": "seed_without_molecule", "r_number": "R-2", "coverage_tier": "A", "model_inclusion": "yes"},
        ]
    )
    molecule_core = pd.DataFrame(
        [
            {
                "mol_id": "mol_mapped",
                "seed_id": "seed_mapped",
                "canonical_smiles": "CF",
                "isomeric_smiles": "CF",
            }
        ]
    )

    mapping = build_governance_mapping(queue, seed_catalog, molecule_core)

    assert len(mapping) == 3
    assert mapping["mapping_status"].value_counts().to_dict() == {"unmapped": 2, "mapped": 1}
    assert mapping.set_index("substance_id").loc["SUB001", "mol_id"] == "mol_mapped"
    assert mapping.set_index("substance_id").loc["SUB002", "mapping_reason"] == "no_molecule_core_match"
    assert mapping.set_index("substance_id").loc["SUB003", "mapping_reason"] == "no_seed_catalog_match"
    assert mapping.set_index("substance_id").loc["SUB001", "requested_property_count"] == 2
    assert mapping.set_index("substance_id").loc["SUB001", "requested_properties"] == "dft_homo_energy;dft_lumo_energy"


def test_xtb_pregeometry_batches_only_missing_completed_xtb(tmp_path: Path) -> None:
    queue = _queue([("SUB001", "R-1"), ("SUB002", "R-2"), ("SUB003", "R-3")])
    seed_catalog, molecule_core = _mapped_catalog([("seed_a", "R-1", "mol_a", "CF"), ("seed_b", "R-2", "mol_b", "CCl"), ("seed_c", "R-3", "mol_c", "CC")])
    quantum_job = pd.DataFrame(
        [
            {
                "request_id": governance_xtb_request_id("mol_a", "CF"),
                "mol_id": "mol_a",
                "status": "succeeded",
                "program": "xtb",
                "theory_level": "GFN2-xTB",
                "basis_set": "",
                "derived_observation_count": 6,
            }
        ]
    )

    summary = materialize_governance_quantum_batches(
        mode="xtb-pregeometry",
        queue=queue,
        seed_catalog=seed_catalog,
        molecule_core=molecule_core,
        quantum_job=quantum_job,
        output_dir=tmp_path / "generated",
        xyz_dir=tmp_path / "governance_xtb_xyz",
        batch_size=1,
        tools_available=True,
    )

    assert summary["mapped_substance_count"] == 3
    assert summary["completed_xtb_molecule_count"] == 1
    assert summary["pending_xtb_request_count"] == 2
    first_batch = pd.read_csv(tmp_path / "generated" / "governance_xtb_requests_batch001.csv")
    second_batch = pd.read_csv(tmp_path / "generated" / "governance_xtb_requests_batch002.csv")
    assert first_batch["mol_id"].tolist() == ["mol_b"]
    assert second_batch["mol_id"].tolist() == ["mol_c"]
    assert first_batch.iloc[0]["request_id"] == governance_xtb_request_id("mol_b", "CCl")
    assert first_batch.iloc[0]["status"] == "ready_for_executor"
    xyz_manifest = pd.read_csv(tmp_path / "generated" / "governance_xtb_xyz_manifest_batch001.csv")
    assert xyz_manifest.iloc[0]["xyz_status"] == "generated"
    assert Path(xyz_manifest.iloc[0]["xyz_path"]).exists()
    mapping_report = pd.read_csv(tmp_path / "generated" / "governance_dft_mapping_report.csv").fillna("")
    statuses = mapping_report.set_index("mol_id")["xtb_enqueue_status"].to_dict()
    assert statuses["mol_a"] == "already_completed"
    assert statuses["mol_b"] == "queued"
    assert statuses["mol_c"] == "queued"


def test_psi4_singlepoint_skips_completed_and_requires_xtb_geometry(tmp_path: Path) -> None:
    queue = _queue([("SUB001", "R-1"), ("SUB002", "R-2"), ("SUB003", "R-3")])
    seed_catalog, molecule_core = _mapped_catalog([("seed_a", "R-1", "mol_a", "CF"), ("seed_b", "R-2", "mol_b", "CCl"), ("seed_c", "R-3", "mol_c", "CC")])
    xtb_artifact_dir = tmp_path / "artifacts" / governance_xtb_request_id("mol_b", "CCl")
    xtb_artifact_dir.mkdir(parents=True)
    (xtb_artifact_dir / "xtbopt.xyz").write_text("1\nxtb optimized mol_b\nC 9.0 0.0 0.0\n", encoding="utf-8")
    xtb_artifact_path = xtb_artifact_dir / f"{governance_xtb_request_id('mol_b', 'CCl')}_xtb_artifact.tar.gz"
    xtb_artifact_path.write_text("placeholder", encoding="utf-8")
    quantum_job = pd.DataFrame(
        [
            {
                "request_id": governance_dft_request_id("mol_a", "CF"),
                "mol_id": "mol_a",
                "status": "succeeded",
                "program": "psi4",
                "theory_level": "B3LYP-D3BJ",
                "basis_set": "def2-SVP",
                "derived_observation_count": 6,
            }
        ]
    )
    quantum_results = pd.DataFrame(
        [
            {
                "request_id": governance_xtb_request_id("mol_b", "CCl"),
                "mol_id": "mol_b",
                "canonical_feature_key": feature_key,
                "value_num": -1.0,
                "program": "xtb",
                "theory_level": "GFN2-xTB",
                "basis_set": "",
                "converged": 1,
                "artifact_uri": str(xtb_artifact_path),
            }
            for feature_key in QUANTUM_CANONICAL_FEATURE_KEYS
        ]
    )

    summary = materialize_governance_quantum_batches(
        mode="psi4-singlepoint",
        queue=queue,
        seed_catalog=seed_catalog,
        molecule_core=molecule_core,
        quantum_job=quantum_job,
        quantum_results=quantum_results,
        output_dir=tmp_path / "generated",
        xyz_dir=tmp_path / "governance_dft_singlepoint_xyz",
        batch_size=50,
        tools_available=True,
    )

    assert summary["completed_psi4_dft_molecule_count"] == 1
    assert summary["pending_dft_request_count"] == 1
    assert summary["dft_blocked_missing_xtb_geometry_count"] == 1
    requests = pd.read_csv(tmp_path / "generated" / "governance_dft_singlepoint_requests_batch001.csv")
    assert requests["mol_id"].tolist() == ["mol_b"]
    assert requests.iloc[0]["request_id"] == governance_dft_request_id("mol_b", "CCl")
    xyz_manifest = pd.read_csv(tmp_path / "generated" / "governance_dft_singlepoint_xyz_manifest_batch001.csv")
    copied_xyz = Path(xyz_manifest.iloc[0]["xyz_path"])
    assert "xtb optimized mol_b" in copied_xyz.read_text(encoding="utf-8")
    assert "copied from completed xTB optimized geometry" in xyz_manifest.iloc[0]["notes"]
    mapping_report = pd.read_csv(tmp_path / "generated" / "governance_dft_mapping_report.csv").fillna("")
    statuses = mapping_report.set_index("mol_id")["dft_enqueue_status"].to_dict()
    assert statuses["mol_a"] == "already_completed"
    assert statuses["mol_b"] == "queued"
    assert statuses["mol_c"] == "blocked_missing_xtb_geometry"


def _queue(substances: list[tuple[str, str]]) -> pd.DataFrame:
    rows = []
    for substance_id, refrigerant_number in substances:
        for idx, requested_property in enumerate(["dft_homo_energy", "dft_lumo_energy"], start=1):
            rows.append(
                {
                    "queue_id": f"Q{len(rows) + 1:03d}",
                    "substance_id": substance_id,
                    "refrigerant_number": refrigerant_number,
                    "requested_property": requested_property,
                    "requested_output_unit": "eV",
                    "priority": "P2",
                    "current_status": "queued_not_computed_in_current_overlay",
                    "notes": f"fixture {idx}",
                }
            )
    return pd.DataFrame(rows)


def _mapped_catalog(rows: list[tuple[str, str, str, str]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    seed_catalog = pd.DataFrame(
        [
            {"seed_id": seed_id, "r_number": refrigerant_number, "coverage_tier": "A", "model_inclusion": "yes"}
            for seed_id, refrigerant_number, _, _ in rows
        ]
    )
    molecule_core = pd.DataFrame(
        [
            {"seed_id": seed_id, "mol_id": mol_id, "canonical_smiles": smiles, "isomeric_smiles": smiles}
            for seed_id, _, mol_id, smiles in rows
        ]
    )
    return seed_catalog, molecule_core
