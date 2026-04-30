from __future__ import annotations

import json
import tarfile
import sys

import pandas as pd
import pytest

from r_physgen_db.psi4_quantum import _write_psi4_input, parse_psi4_scalar_features, run_psi4_quantum_pilot
from r_physgen_db.quantum_pilot import QUANTUM_CANONICAL_FEATURE_KEYS


def test_psi4_parser_extracts_scalar_features_from_json_sidecar(tmp_path) -> None:
    result_path = tmp_path / "psi4_result.json"
    result_path.write_text(
        json.dumps(
            {
                "program_version": "psi4-fake-1.9",
                "theory_level": "B3LYP-D3BJ",
                "basis_set": "def2-SVP",
                "values": {
                    "total_energy_eh": -154.25,
                    "homo_ev": -8.2,
                    "lumo_ev": -0.9,
                    "dipole_moment_debye": 2.6,
                    "polarizability_au": 31.5,
                },
            }
        ),
        encoding="utf-8",
    )

    parsed = parse_psi4_scalar_features(result_path)

    assert parsed.program_version == "psi4-fake-1.9"
    assert parsed.theory_level == "B3LYP-D3BJ"
    assert parsed.basis_set == "def2-SVP"
    assert parsed.values == {
        "quantum.homo_energy": pytest.approx(-8.2),
        "quantum.lumo_energy": pytest.approx(-0.9),
        "quantum.homo_lumo_gap": pytest.approx(7.3),
        "quantum.total_energy": pytest.approx(-154.25),
        "quantum.dipole_moment": pytest.approx(2.6),
        "quantum.polarizability": pytest.approx(31.5),
    }


def test_psi4_parser_flattens_symmetry_blocked_orbital_energies(tmp_path) -> None:
    result_path = tmp_path / "psi4_result.json"
    result_path.write_text(
        json.dumps(
            {
                "program_version": "psi4-fake-1.10",
                "values": {
                    "total_energy_eh": -76.0,
                    "dipole_moment_debye": 1.5,
                    "polarizability_au": 5.0,
                },
                "wavefunction": {
                    "epsilon_a_hartree": [[-0.50, -0.30], [-0.10, 0.05]],
                    "nalpha": 3,
                },
            }
        ),
        encoding="utf-8",
    )

    parsed = parse_psi4_scalar_features(result_path)

    assert parsed.values["quantum.homo_energy"] == pytest.approx(-0.10 * 27.211386245988)
    assert parsed.values["quantum.lumo_energy"] == pytest.approx(0.05 * 27.211386245988)
    assert parsed.values["quantum.homo_lumo_gap"] == pytest.approx(0.15 * 27.211386245988)


def test_psi4_input_uses_open_shell_reference_for_radical_smiles(tmp_path) -> None:
    xyz_path = tmp_path / "radical.xyz"
    xyz_path.write_text("3\nfixture\nC 0 0 0\nF 0 0 1.3\nF 0 1.3 0\n", encoding="utf-8")
    input_path = tmp_path / "psi4_input.py"

    _write_psi4_input(
        input_path,
        xyz_path,
        {
            "canonical_smiles": "F[C](F)C(F)(F)C(F)(F)OC(F)=C(F)F",
            "theory_level": "B3LYP-D3BJ",
            "basis_set": "def2-SVP",
        },
    )

    text = input_path.read_text(encoding="utf-8")
    assert "psi4.geometry('0 2\\n" in text
    assert '"reference": \'uhf\'' in text


def test_psi4_runner_writes_missing_executor_audit_artifact(tmp_path) -> None:
    requests_path = tmp_path / "quantum_dft_requests.csv"
    xyz_manifest_path = tmp_path / "quantum_dft_xyz_manifest.csv"
    output_path = tmp_path / "quantum_pilot_results.csv"
    artifact_dir = tmp_path / "artifacts"
    xyz_path = tmp_path / "qreq_psi4.xyz"
    xyz_path.write_text("2\nfixture\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    pd.DataFrame(
        [
            {
                "request_id": "qreq_psi4",
                "mol_id": "mol_psi4",
                "program": "psi4",
                "method_family": "DFT",
                "theory_level": "B3LYP-D3BJ",
                "basis_set": "def2-SVP",
                "solvation_model": "gas_phase",
                "status": "pending_executor_unavailable",
            }
        ]
    ).to_csv(requests_path, index=False)
    pd.DataFrame(
        [
            {
                "request_id": "qreq_psi4",
                "mol_id": "mol_psi4",
                "xyz_path": str(xyz_path),
                "xyz_status": "generated",
            }
        ]
    ).to_csv(xyz_manifest_path, index=False)

    summary = run_psi4_quantum_pilot(
        requests_path=requests_path,
        xyz_manifest_path=xyz_manifest_path,
        output_path=output_path,
        artifact_dir=artifact_dir,
        psi4_bin=None,
        allow_missing_executor=True,
    )

    rows = pd.read_csv(output_path).fillna("")
    row = rows.iloc[0]
    assert summary["requested"] == 1
    assert summary["failed"] == 1
    assert len(rows) == 1
    assert row["program"] == "psi4"
    assert row["canonical_feature_key"] == ""
    assert row["converged"] == 0
    assert "executor_unavailable" in row["notes"]
    assert len(row["artifact_sha256"]) == 64
    assert (artifact_dir / "qreq_psi4" / "psi4.stderr").exists()


def test_psi4_runner_merges_success_rows_with_existing_xtb_output(tmp_path) -> None:
    requests_path = tmp_path / "quantum_dft_requests.csv"
    xyz_manifest_path = tmp_path / "quantum_dft_xyz_manifest.csv"
    output_path = tmp_path / "quantum_pilot_results.csv"
    artifact_dir = tmp_path / "artifacts"
    xyz_path = tmp_path / "qreq_psi4.xyz"
    xyz_path.write_text("2\nfixture\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    pd.DataFrame(
        [
            {
                "request_id": "qreq_xtb",
                "mol_id": "mol_psi4",
                "canonical_feature_key": "quantum.homo_energy",
                "value_num": -7.0,
                "unit": "eV",
                "program": "xtb",
                "program_version": "6.7.1",
                "method_family": "semiempirical",
                "theory_level": "GFN2-xTB",
                "basis_set": "",
                "solvation_model": "gas_phase",
                "converged": 1,
                "imaginary_frequency_count": 0,
                "artifact_uri": "manual://xtb",
                "artifact_sha256": "a" * 64,
                "quality_level": "calculated_open_source",
                "notes": "existing",
            }
        ]
    ).to_csv(output_path, index=False)
    pd.DataFrame(
        [
            {
                "request_id": "qreq_psi4",
                "mol_id": "mol_psi4",
                "program": "psi4",
                "method_family": "DFT",
                "theory_level": "B3LYP-D3BJ",
                "basis_set": "def2-SVP",
                "solvation_model": "gas_phase",
                "status": "ready_for_executor",
            }
        ]
    ).to_csv(requests_path, index=False)
    pd.DataFrame(
        [
            {
                "request_id": "qreq_psi4",
                "mol_id": "mol_psi4",
                "xyz_path": str(xyz_path),
                "xyz_status": "generated",
            }
        ]
    ).to_csv(xyz_manifest_path, index=False)
    fake_psi4 = tmp_path / "fake_psi4.py"
    fake_psi4.write_text(
        f"""#!{sys.executable}
from __future__ import annotations
import json
from pathlib import Path
Path('psi4_result.json').write_text(json.dumps({{
    'program_version': 'fake-psi4',
    'values': {{
        'total_energy_eh': -1.1,
        'homo_ev': -8.0,
        'lumo_ev': -1.0,
        'dipole_moment_debye': 0.2,
        'polarizability_au': 4.0,
    }},
}}))
""",
        encoding="utf-8",
    )
    fake_psi4.chmod(0o755)

    summary = run_psi4_quantum_pilot(
        requests_path=requests_path,
        xyz_manifest_path=xyz_manifest_path,
        output_path=output_path,
        artifact_dir=artifact_dir,
        psi4_bin=fake_psi4,
    )

    rows = pd.read_csv(output_path).fillna("")
    assert summary["succeeded"] == 1
    assert len(rows.loc[rows["program"].eq("xtb")]) == 1
    psi4_rows = rows.loc[rows["program"].eq("psi4")]
    assert len(psi4_rows) == 6
    assert set(psi4_rows["canonical_feature_key"]) == QUANTUM_CANONICAL_FEATURE_KEYS


def test_psi4_retry_failed_only_writes_attempt_bundle_and_removes_failure_audit(tmp_path) -> None:
    requests_path = tmp_path / "quantum_dft_requests.csv"
    xyz_manifest_path = tmp_path / "quantum_dft_xyz_manifest.csv"
    output_path = tmp_path / "quantum_pilot_results.csv"
    artifact_dir = tmp_path / "artifacts"
    xyz_fail = tmp_path / "qreq_psi4_fail.xyz"
    xyz_never = tmp_path / "qreq_psi4_never.xyz"
    xyz_fail.write_text("2\nfixture\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    xyz_never.write_text("2\nfixture\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    pd.DataFrame(
        [
            {
                "request_id": "qreq_psi4_fail",
                "mol_id": "mol_psi4_fail",
                "program": "psi4",
                "method_family": "DFT",
                "theory_level": "B3LYP-D3BJ",
                "basis_set": "def2-SVP",
                "solvation_model": "gas_phase",
                "status": "ready_for_executor",
            },
            {
                "request_id": "qreq_psi4_never",
                "mol_id": "mol_psi4_never",
                "program": "psi4",
                "method_family": "DFT",
                "theory_level": "B3LYP-D3BJ",
                "basis_set": "def2-SVP",
                "solvation_model": "gas_phase",
                "status": "ready_for_executor",
            },
        ]
    ).to_csv(requests_path, index=False)
    pd.DataFrame(
        [
            {
                "request_id": "qreq_psi4_fail",
                "mol_id": "mol_psi4_fail",
                "xyz_path": str(xyz_fail),
                "xyz_status": "generated",
            },
            {
                "request_id": "qreq_psi4_never",
                "mol_id": "mol_psi4_never",
                "xyz_path": str(xyz_never),
                "xyz_status": "generated",
            },
        ]
    ).to_csv(xyz_manifest_path, index=False)
    pd.DataFrame(
        [
            {
                "request_id": "qreq_psi4_fail",
                "mol_id": "mol_psi4_fail",
                "canonical_feature_key": "",
                "value_num": "",
                "unit": "",
                "program": "psi4",
                "program_version": "",
                "method_family": "DFT",
                "theory_level": "B3LYP-D3BJ",
                "basis_set": "def2-SVP",
                "solvation_model": "gas_phase",
                "converged": 0,
                "imaginary_frequency_count": 0,
                "artifact_uri": "manual://old",
                "artifact_sha256": "b" * 64,
                "quality_level": "computed_high",
                "notes": "failed before retry",
            },
            {
                "request_id": "qreq_stale_old_manifest",
                "mol_id": "mol_stale",
                "canonical_feature_key": "",
                "value_num": "",
                "unit": "",
                "program": "psi4",
                "program_version": "",
                "method_family": "DFT",
                "theory_level": "B3LYP-D3BJ",
                "basis_set": "def2-SVP",
                "solvation_model": "gas_phase",
                "converged": 0,
                "imaginary_frequency_count": 0,
                "artifact_uri": "manual://stale",
                "artifact_sha256": "c" * 64,
                "quality_level": "computed_high",
                "notes": "stale failed row from older manifest",
            }
        ]
    ).to_csv(output_path, index=False)
    fake_psi4 = tmp_path / "fake_psi4.py"
    fake_psi4.write_text(
        f"""#!{sys.executable}
from __future__ import annotations
import json
from pathlib import Path
Path('psi4_result.json').write_text(json.dumps({{
    'program_version': 'fake-psi4',
    'values': {{
        'total_energy_eh': -1.1,
        'homo_ev': -8.0,
        'lumo_ev': -1.0,
        'dipole_moment_debye': 0.2,
        'polarizability_au': 4.0,
    }},
}}))
""",
        encoding="utf-8",
    )
    fake_psi4.chmod(0o755)

    summary = run_psi4_quantum_pilot(
        requests_path=requests_path,
        xyz_manifest_path=xyz_manifest_path,
        output_path=output_path,
        artifact_dir=artifact_dir,
        psi4_bin=fake_psi4,
        retry_failed_only=True,
        completion_required=True,
    )

    rows = pd.read_csv(output_path).fillna("")
    psi4_rows = rows.loc[rows["program"].eq("psi4")]
    assert summary["requested"] == 1
    assert summary["succeeded"] == 1
    assert summary["failed"] == 0
    assert len(psi4_rows) == 6
    assert set(psi4_rows["canonical_feature_key"]) == QUANTUM_CANONICAL_FEATURE_KEYS
    assert not rows["request_id"].eq("qreq_psi4_never").any()
    assert not rows["request_id"].eq("qreq_stale_old_manifest").any()
    attempt_dir = artifact_dir / "qreq_psi4_fail" / "attempt_01_psi4_dft_sp"
    assert (attempt_dir / "psi4_result.json").exists()
    artifact_path = artifact_dir / "qreq_psi4_fail" / "qreq_psi4_fail_psi4_artifact.tar.gz"
    with tarfile.open(artifact_path) as archive:
        names = set(archive.getnames())
    assert "attempt_01_psi4_dft_sp/psi4_result.json" in names


def test_psi4_completion_required_batch_preserves_unrelated_success_rows(tmp_path) -> None:
    requests_path = tmp_path / "governance_dft_batch.csv"
    xyz_manifest_path = tmp_path / "governance_dft_xyz_batch.csv"
    output_path = tmp_path / "quantum_pilot_results.csv"
    artifact_dir = tmp_path / "artifacts"
    xyz_path = tmp_path / "qreq_psi4_new.xyz"
    xyz_path.write_text("2\nfixture\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    pd.DataFrame(
        [
            {
                "request_id": "qreq_psi4_new",
                "mol_id": "mol_psi4_new",
                "program": "psi4",
                "method_family": "DFT",
                "theory_level": "B3LYP-D3BJ",
                "basis_set": "def2-SVP",
                "solvation_model": "gas_phase",
                "status": "ready_for_executor",
            }
        ]
    ).to_csv(requests_path, index=False)
    pd.DataFrame(
        [
            {
                "request_id": "qreq_psi4_new",
                "mol_id": "mol_psi4_new",
                "xyz_path": str(xyz_path),
                "xyz_status": "generated",
            }
        ]
    ).to_csv(xyz_manifest_path, index=False)
    existing_rows = [
        {
            "request_id": "qreq_psi4_existing",
            "mol_id": "mol_psi4_existing",
            "canonical_feature_key": feature_key,
            "value_num": -1.0,
            "unit": "eV",
            "program": "psi4",
            "program_version": "fake-old",
            "method_family": "DFT",
            "theory_level": "B3LYP-D3BJ",
            "basis_set": "def2-SVP",
            "solvation_model": "gas_phase",
            "converged": 1,
            "imaginary_frequency_count": 0,
            "artifact_uri": "manual://existing",
            "artifact_sha256": "d" * 64,
            "quality_level": "computed_high",
            "notes": "completed in a different batch",
        }
        for feature_key in QUANTUM_CANONICAL_FEATURE_KEYS
    ]
    existing_rows.append(
        {
            "request_id": "qreq_psi4_old_failed_audit",
            "mol_id": "mol_old_failed",
            "canonical_feature_key": "",
            "value_num": "",
            "unit": "",
            "program": "psi4",
            "program_version": "",
            "method_family": "DFT",
            "theory_level": "B3LYP-D3BJ",
            "basis_set": "def2-SVP",
            "solvation_model": "gas_phase",
            "converged": 0,
            "imaginary_frequency_count": 0,
            "artifact_uri": "manual://old_failed",
            "artifact_sha256": "e" * 64,
            "quality_level": "computed_high",
            "notes": "stale failed audit",
        }
    )
    pd.DataFrame(existing_rows).to_csv(output_path, index=False)
    fake_psi4 = tmp_path / "fake_psi4.py"
    fake_psi4.write_text(
        f"""#!{sys.executable}
from __future__ import annotations
import json
from pathlib import Path
Path('psi4_result.json').write_text(json.dumps({{
    'program_version': 'fake-psi4',
    'values': {{
        'total_energy_eh': -1.1,
        'homo_ev': -8.0,
        'lumo_ev': -1.0,
        'dipole_moment_debye': 0.2,
        'polarizability_au': 4.0,
    }},
}}))
""",
        encoding="utf-8",
    )
    fake_psi4.chmod(0o755)

    summary = run_psi4_quantum_pilot(
        requests_path=requests_path,
        xyz_manifest_path=xyz_manifest_path,
        output_path=output_path,
        artifact_dir=artifact_dir,
        psi4_bin=fake_psi4,
        completion_required=True,
    )

    rows = pd.read_csv(output_path).fillna("")
    assert summary["succeeded"] == 1
    assert len(rows.loc[rows["request_id"].eq("qreq_psi4_existing")]) == len(QUANTUM_CANONICAL_FEATURE_KEYS)
    assert len(rows.loc[rows["request_id"].eq("qreq_psi4_new")]) == len(QUANTUM_CANONICAL_FEATURE_KEYS)
    assert not rows["request_id"].eq("qreq_psi4_old_failed_audit").any()
