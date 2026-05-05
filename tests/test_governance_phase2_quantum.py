from __future__ import annotations

import json
import stat
import sys
from pathlib import Path

import pandas as pd
import pytest

from r_physgen_db.phase2_quantum import (
    CREST_DETAIL_COLUMNS,
    PHASE2_CONFORMER_FEATURE_KEYS,
    PHASE2_XTB_HESSIAN_FEATURE_KEYS,
    governance_phase2_request_id,
    materialize_governance_phase2_batches,
    parse_crest_conformer_ensemble,
    parse_xtb_hessian_output,
    run_crest_conformer_phase2,
    run_xtb_phase2_hessian,
)
from r_physgen_db.quantum_pilot import QUANTUM_CANONICAL_FEATURE_KEYS


def test_phase2_generator_preserves_mapped_and_unmapped_and_writes_staged_batches(tmp_path: Path) -> None:
    queue = _queue([("SUB001", "R-1"), ("SUB002", "R-2"), ("SUB003", "R-3")])
    seed_catalog, molecule_core = _mapped_catalog(
        [
            ("seed_a", "R-1", "mol_small", "CF", 2),
            ("seed_b", "R-2", "mol_flexible", "CCCCCC", 6),
        ]
    )
    quantum_results = _completed_xtb_rows(tmp_path, [("mol_small", "CF"), ("mol_flexible", "CCCCCC")])

    summary = materialize_governance_phase2_batches(
        queue=queue,
        seed_catalog=seed_catalog,
        molecule_core=molecule_core,
        quantum_results=quantum_results,
        output_dir=tmp_path / "generated",
        xyz_dir=tmp_path / "phase2_xyz",
        crest_heavy_atom_min=6,
        orca_smoke_size=1,
        batch_size_orca=1,
        mapped_only=True,
        xtb_available=True,
        crest_available=True,
        orca_available=True,
    )

    assert summary["mapped_substance_count"] == 2
    assert summary["unmapped_substance_count"] == 1
    assert summary["xtb_hessian_request_count"] == 2
    assert summary["crest_request_count"] == 2
    assert summary["crest_external_request_count"] == 1
    assert summary["crest_singleton_request_count"] == 1
    assert summary["orca_smoke_request_count"] == 1
    assert summary["orca_full_request_count"] == 1

    mapping_report = pd.read_csv(tmp_path / "generated" / "governance_phase2_mapping_report.csv").fillna("")
    assert mapping_report["mapping_status"].value_counts().to_dict() == {"mapped": 2, "unmapped": 1}
    blocker_report = pd.read_csv(tmp_path / "generated" / "governance_phase2_blockers.csv").fillna("")
    assert set(blocker_report["blocker_type"]) >= {"unmapped_governance_substance", "postprocessor_unavailable"}
    assert "nbo_resp_charges" in set(blocker_report["target_output"])
    assert "standard_enthalpy_of_formation" in set(blocker_report["target_output"])

    crest_requests = pd.read_csv(tmp_path / "generated" / "governance_phase2_crest_requests.csv").fillna("")
    assert crest_requests.set_index("mol_id").loc["mol_small", "execution_kind"] == "singleton"
    assert crest_requests.set_index("mol_id").loc["mol_flexible", "execution_kind"] == "crest"

    smoke = pd.read_csv(tmp_path / "generated" / "governance_phase2_orca_optfreq_smoke_requests.csv")
    batch001 = pd.read_csv(tmp_path / "generated" / "governance_phase2_orca_optfreq_batch001.csv")
    assert len(smoke) == 1
    assert len(batch001) == 1
    assert set(smoke["request_id"]).isdisjoint(set(batch001["request_id"]))


def test_phase2_generator_does_not_duplicate_completed_hessian_requests(tmp_path: Path) -> None:
    queue = _queue([("SUB001", "R-1"), ("SUB002", "R-2")])
    seed_catalog, molecule_core = _mapped_catalog(
        [
            ("seed_a", "R-1", "mol_done", "CF", 2),
            ("seed_b", "R-2", "mol_todo", "CC", 2),
        ]
    )
    quantum_results = _completed_xtb_rows(tmp_path, [("mol_done", "CF"), ("mol_todo", "CC")])
    done_request_id = governance_phase2_request_id("mol_done", "CF", program="xtb", task="hessian")
    completed_hessian = [
        {
            "request_id": done_request_id,
            "mol_id": "mol_done",
            "canonical_feature_key": feature_key,
            "value_num": 0.1,
            "unit": "Eh" if feature_key == "quantum.zpe" else "cm^-1",
            "program": "xtb",
            "program_version": "fake",
            "method_family": "semiempirical",
            "theory_level": "GFN2-xTB+hessian",
            "basis_set": "",
            "solvation_model": "gas_phase",
            "converged": 1,
            "imaginary_frequency_count": 0,
            "artifact_uri": "manual://done",
            "artifact_sha256": "a" * 64,
            "quality_level": "calculated_open_source",
            "notes": "done",
        }
        for feature_key in PHASE2_XTB_HESSIAN_FEATURE_KEYS
    ]
    quantum_results = pd.concat([quantum_results, pd.DataFrame(completed_hessian)], ignore_index=True)

    summary = materialize_governance_phase2_batches(
        queue=queue,
        seed_catalog=seed_catalog,
        molecule_core=molecule_core,
        quantum_results=quantum_results,
        output_dir=tmp_path / "generated",
        xyz_dir=tmp_path / "phase2_xyz",
        mapped_only=True,
        xtb_available=True,
        crest_available=True,
        orca_available=True,
    )

    requests = pd.read_csv(tmp_path / "generated" / "governance_phase2_xtb_hessian_requests.csv").fillna("")
    assert summary["xtb_hessian_request_count"] == 1
    assert requests["mol_id"].tolist() == ["mol_todo"]
    assert done_request_id not in set(requests["request_id"])


def test_xtb_hessian_parser_extracts_zpe_frequency_and_ir_modes(tmp_path: Path) -> None:
    stdout = tmp_path / "xtb.stdout"
    stdout.write_text(
        """
       zero point energy          0.012345 Eh
       G(RRHO) contrib.           0.045678 Eh
       H(T)-H(0)                  0.034567 Eh

       mode  frequency/cm-1   IR intensity/km mol-1
          1       -12.50          0.00
          2       123.40          4.50
          3       456.70          1.25
""",
        encoding="utf-8",
    )

    parsed = parse_xtb_hessian_output(stdout)

    assert parsed.scalars["quantum.zpe"] == pytest.approx(0.012345)
    assert parsed.scalars["quantum.lowest_real_frequency"] == pytest.approx(123.40)
    assert parsed.scalars["quantum.thermal_gibbs_correction"] == pytest.approx(0.045678)
    assert parsed.scalars["quantum.thermal_enthalpy_correction"] == pytest.approx(0.034567)
    assert parsed.imaginary_frequency_count == 1
    assert parsed.modes[0]["frequency_cm_inv"] == pytest.approx(-12.50)
    assert parsed.modes[1]["ir_intensity_km_mol"] == pytest.approx(4.50)


def test_crest_parser_extracts_relative_energies_and_boltzmann_weights(tmp_path: Path) -> None:
    energies = tmp_path / "crest.energies"
    energies.write_text(
        """
  Erel/kcal   Etot/Eh      weight
     0.000    -100.0000    0.700
     1.250    -99.9980     0.300
""",
        encoding="utf-8",
    )

    parsed = parse_crest_conformer_ensemble(energies)

    assert parsed.scalars["quantum.conformer_count"] == pytest.approx(2.0)
    assert parsed.scalars["quantum.conformer_energy_window"] == pytest.approx(1.25)
    assert [row["conformer_index"] for row in parsed.conformers] == [1, 2]
    assert parsed.conformers[0]["boltzmann_weight"] == pytest.approx(0.7)


def test_xtb_hessian_runner_writes_rows_artifacts_and_details_with_fake_binary(tmp_path: Path) -> None:
    requests_path, xyz_manifest_path = _phase2_manifest(tmp_path, program="xtb", task="hessian", mol_id="mol_a", smiles="CF")
    output_path = tmp_path / "quantum_pilot_results.csv"
    artifact_dir = tmp_path / "artifacts"
    detail_path = tmp_path / "vibrational_modes.csv"
    fake_xtb = tmp_path / "fake_xtb.py"
    fake_xtb.write_text(
        f"""#!{sys.executable}
from pathlib import Path
Path('xtb.stdout').write_text('''zero point energy 0.010 Eh\nG(RRHO) contrib. 0.020 Eh\nH(T)-H(0) 0.030 Eh\nmode frequency/cm-1 IR intensity/km mol-1\n1 100.0 5.0\n''')
print('zero point energy 0.010 Eh')
print('G(RRHO) contrib. 0.020 Eh')
print('H(T)-H(0) 0.030 Eh')
print('mode frequency/cm-1 IR intensity/km mol-1')
print('1 100.0 5.0')
""",
        encoding="utf-8",
    )
    fake_xtb.chmod(fake_xtb.stat().st_mode | stat.S_IXUSR)

    summary = run_xtb_phase2_hessian(
        requests_path=requests_path,
        xyz_manifest_path=xyz_manifest_path,
        output_path=output_path,
        artifact_dir=artifact_dir,
        vibrational_modes_path=detail_path,
        xtb_bin=fake_xtb,
        completion_required=True,
    )

    rows = pd.read_csv(output_path).fillna("")
    assert summary["succeeded"] == 1
    assert set(rows["canonical_feature_key"]) == set(PHASE2_XTB_HESSIAN_FEATURE_KEYS)
    assert rows["artifact_sha256"].str.len().eq(64).all()
    modes = pd.read_csv(detail_path).fillna("")
    assert len(modes) == 1
    assert modes.iloc[0]["frequency_cm_inv"] == pytest.approx(100.0)


def test_crest_runner_writes_singleton_without_external_binary(tmp_path: Path) -> None:
    requests_path, xyz_manifest_path = _phase2_manifest(
        tmp_path,
        program="crest",
        task="conformer",
        mol_id="mol_a",
        smiles="CF",
        execution_kind="singleton",
    )
    output_path = tmp_path / "quantum_pilot_results.csv"
    artifact_dir = tmp_path / "artifacts"
    conformer_path = tmp_path / "conformers.csv"

    summary = run_crest_conformer_phase2(
        requests_path=requests_path,
        xyz_manifest_path=xyz_manifest_path,
        output_path=output_path,
        artifact_dir=artifact_dir,
        conformer_detail_path=conformer_path,
        crest_bin=None,
        completion_required=True,
    )

    rows = pd.read_csv(output_path).fillna("")
    conformers = pd.read_csv(conformer_path).fillna("")
    assert summary["succeeded"] == 1
    assert set(rows["canonical_feature_key"]) == set(PHASE2_CONFORMER_FEATURE_KEYS)
    assert rows.loc[rows["canonical_feature_key"].eq("quantum.conformer_count"), "value_num"].iloc[0] == pytest.approx(1.0)
    assert list(conformers.columns) == CREST_DETAIL_COLUMNS
    assert conformers.iloc[0]["boltzmann_weight"] == pytest.approx(1.0)


def _queue(substances: list[tuple[str, str]]) -> pd.DataFrame:
    rows = []
    for substance_id, refrigerant_number in substances:
        for requested_property in ["dft_homo_energy", "dft_lumo_energy"]:
            rows.append(
                {
                    "queue_id": f"Q{len(rows) + 1:03d}",
                    "substance_id": substance_id,
                    "refrigerant_number": refrigerant_number,
                    "requested_property": requested_property,
                }
            )
    return pd.DataFrame(rows)


def _mapped_catalog(rows: list[tuple[str, str, str, str, int]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    seed_catalog = pd.DataFrame(
        [
            {"seed_id": seed_id, "r_number": refrigerant_number, "coverage_tier": "A", "model_inclusion": "yes"}
            for seed_id, refrigerant_number, _, _, _ in rows
        ]
    )
    molecule_core = pd.DataFrame(
        [
            {
                "seed_id": seed_id,
                "mol_id": mol_id,
                "canonical_smiles": smiles,
                "isomeric_smiles": smiles,
                "heavy_atom_count": heavy_atom_count,
                "status": "resolved",
            }
            for seed_id, _, mol_id, smiles, heavy_atom_count in rows
        ]
    )
    return seed_catalog, molecule_core


def _completed_xtb_rows(tmp_path: Path, molecules: list[tuple[str, str]]) -> pd.DataFrame:
    rows = []
    for mol_id, smiles in molecules:
        request_id = "qreq_" + __import__("hashlib").sha256(f"{mol_id}|{smiles}|xtb-gfn2".encode()).hexdigest()[:16]
        artifact_dir = tmp_path / "xtb_artifacts" / request_id
        artifact_dir.mkdir(parents=True)
        (artifact_dir / "xtbopt.xyz").write_text("1\noptimized\nC 0 0 0\n", encoding="utf-8")
        artifact_uri = artifact_dir / f"{request_id}_xtb_artifact.tar.gz"
        artifact_uri.write_text("placeholder", encoding="utf-8")
        for feature_key in QUANTUM_CANONICAL_FEATURE_KEYS:
            rows.append(
                {
                    "request_id": request_id,
                    "mol_id": mol_id,
                    "canonical_feature_key": feature_key,
                    "value_num": -1.0,
                    "unit": "eV",
                    "program": "xtb",
                    "program_version": "fake",
                    "method_family": "semiempirical",
                    "theory_level": "GFN2-xTB",
                    "basis_set": "",
                    "solvation_model": "gas_phase",
                    "converged": 1,
                    "imaginary_frequency_count": 0,
                    "artifact_uri": str(artifact_uri),
                    "artifact_sha256": "a" * 64,
                    "quality_level": "calculated_open_source",
                    "notes": "completed xtb",
                }
            )
    return pd.DataFrame(rows)


def _phase2_manifest(
    tmp_path: Path,
    *,
    program: str,
    task: str,
    mol_id: str,
    smiles: str,
    execution_kind: str = "executor",
) -> tuple[Path, Path]:
    request_id = governance_phase2_request_id(mol_id, smiles, program=program, task=task)
    xyz_path = tmp_path / f"{request_id}.xyz"
    xyz_path.write_text("1\nfixture\nC 0 0 0\n", encoding="utf-8")
    requests_path = tmp_path / "requests.csv"
    xyz_manifest_path = tmp_path / "xyz_manifest.csv"
    pd.DataFrame(
        [
            {
                "request_id": request_id,
                "mol_id": mol_id,
                "canonical_smiles": smiles,
                "isomeric_smiles": smiles,
                "program": program,
                "method_family": "DFT" if program == "orca" else "semiempirical",
                "theory_level": "GFN2-xTB+hessian" if program == "xtb" else ("CREST-GFN2-xTB" if program == "crest" else "B3LYP-D3BJ"),
                "basis_set": "def2-SVP" if program == "orca" else "",
                "solvation_model": "gas_phase",
                "status": "ready_for_executor",
                "recommended_next_action": "run_quantum",
                "notes": "fixture",
                "phase2_task": task,
                "execution_kind": execution_kind,
            }
        ]
    ).to_csv(requests_path, index=False)
    pd.DataFrame([{"request_id": request_id, "mol_id": mol_id, "xyz_path": str(xyz_path), "xyz_status": "generated", "notes": "fixture"}]).to_csv(
        xyz_manifest_path,
        index=False,
    )
    return requests_path, xyz_manifest_path
