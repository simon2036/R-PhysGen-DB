from __future__ import annotations

import stat
import sys
from pathlib import Path

import pandas as pd
import pytest

from r_physgen_db.phase2_quantum import (
    ORCA_ATOMIC_CHARGE_COLUMNS,
    ORCA_DETAIL_COLUMNS,
    PHASE2_ORCA_FEATURE_KEYS,
    governance_phase2_request_id,
    parse_orca_optfreq_output,
    run_orca_phase2_optfreq,
)


def test_orca_parser_extracts_optfreq_thermochemistry_modes_and_charges(tmp_path: Path) -> None:
    stdout = tmp_path / "orca.stdout"
    stdout.write_text(_orca_fixture(), encoding="utf-8")

    parsed = parse_orca_optfreq_output(stdout)

    assert parsed.normal_termination is True
    assert parsed.optimization_converged is True
    assert parsed.imaginary_frequency_count == 1
    assert parsed.scalars["quantum.zpe"] == pytest.approx(0.111111)
    assert parsed.scalars["quantum.lowest_real_frequency"] == pytest.approx(120.5)
    assert parsed.scalars["quantum.thermal_enthalpy_correction"] == pytest.approx(0.222222)
    assert parsed.scalars["quantum.thermal_gibbs_correction"] == pytest.approx(0.333333)
    assert parsed.modes[0]["frequency_cm_inv"] == pytest.approx(-15.0)
    assert parsed.modes[1]["ir_intensity_km_mol"] == pytest.approx(4.4)
    assert parsed.atomic_charges[0]["charge_scheme"] == "mulliken"
    assert parsed.atomic_charges[0]["partial_charge"] == pytest.approx(-0.123)


def test_orca_runner_writes_success_rows_artifact_and_detail_tables_with_fake_binary(tmp_path: Path) -> None:
    requests_path, xyz_manifest_path = _orca_manifest(tmp_path)
    output_path = tmp_path / "quantum_pilot_results.csv"
    artifact_dir = tmp_path / "artifacts"
    modes_path = tmp_path / "orca_modes.csv"
    charges_path = tmp_path / "orca_charges.csv"
    fake_orca = tmp_path / "fake_orca.py"
    fake_orca.write_text(f"#!{sys.executable}\nprint({_orca_fixture()!r})\n", encoding="utf-8")
    fake_orca.chmod(fake_orca.stat().st_mode | stat.S_IXUSR)

    summary = run_orca_phase2_optfreq(
        requests_path=requests_path,
        xyz_manifest_path=xyz_manifest_path,
        output_path=output_path,
        artifact_dir=artifact_dir,
        vibrational_modes_path=modes_path,
        atomic_charges_path=charges_path,
        orca_bin=fake_orca,
        jobs=1,
        nprocs_per_job=2,
        completion_required=True,
    )

    rows = pd.read_csv(output_path).fillna("")
    modes = pd.read_csv(modes_path).fillna("")
    charges = pd.read_csv(charges_path).fillna("")
    assert summary["succeeded"] == 1
    assert set(rows["canonical_feature_key"]) == set(PHASE2_ORCA_FEATURE_KEYS)
    assert rows["artifact_sha256"].str.len().eq(64).all()
    assert list(modes.columns) == ORCA_DETAIL_COLUMNS
    assert list(charges.columns) == ORCA_ATOMIC_CHARGE_COLUMNS
    assert len(modes) == 2
    assert len(charges) == 2
    orca_input = artifact_dir / rows.iloc[0]["request_id"] / "orca.inp"
    assert "%pal nprocs 2 end" in orca_input.read_text(encoding="utf-8")
    assert "! B3LYP D3BJ def2-SVP Opt Freq TightSCF" in orca_input.read_text(encoding="utf-8")


def test_orca_completion_required_raises_when_fake_binary_fails(tmp_path: Path) -> None:
    requests_path, xyz_manifest_path = _orca_manifest(tmp_path)
    fake_orca = tmp_path / "fake_orca_fail.py"
    fake_orca.write_text(f"#!{sys.executable}\nraise SystemExit(2)\n", encoding="utf-8")
    fake_orca.chmod(fake_orca.stat().st_mode | stat.S_IXUSR)

    with pytest.raises(RuntimeError, match="ORCA completion required"):
        run_orca_phase2_optfreq(
            requests_path=requests_path,
            xyz_manifest_path=xyz_manifest_path,
            output_path=tmp_path / "quantum_pilot_results.csv",
            artifact_dir=tmp_path / "artifacts",
            vibrational_modes_path=tmp_path / "modes.csv",
            atomic_charges_path=tmp_path / "charges.csv",
            orca_bin=fake_orca,
            completion_required=True,
        )


def _orca_manifest(tmp_path: Path) -> tuple[Path, Path]:
    request_id = governance_phase2_request_id("mol_orca", "CF", program="orca", task="optfreq")
    xyz_path = tmp_path / f"{request_id}.xyz"
    xyz_path.write_text("2\nfixture\nC 0 0 0\nF 0 0 1.3\n", encoding="utf-8")
    requests_path = tmp_path / "orca_requests.csv"
    xyz_manifest_path = tmp_path / "orca_xyz_manifest.csv"
    pd.DataFrame(
        [
            {
                "request_id": request_id,
                "mol_id": "mol_orca",
                "canonical_smiles": "CF",
                "isomeric_smiles": "CF",
                "program": "orca",
                "method_family": "DFT",
                "theory_level": "B3LYP-D3BJ",
                "basis_set": "def2-SVP",
                "solvation_model": "gas_phase",
                "status": "ready_for_executor",
                "recommended_next_action": "run_quantum",
                "notes": "fixture",
                "phase2_task": "optfreq",
                "execution_kind": "executor",
            }
        ]
    ).to_csv(requests_path, index=False)
    pd.DataFrame([{"request_id": request_id, "mol_id": "mol_orca", "xyz_path": str(xyz_path), "xyz_status": "generated", "notes": "fixture"}]).to_csv(
        xyz_manifest_path,
        index=False,
    )
    return requests_path, xyz_manifest_path


def _orca_fixture() -> str:
    return """
THE OPTIMIZATION HAS CONVERGED
VIBRATIONAL FREQUENCIES
  1:   -15.00 cm**-1
  2:   120.50 cm**-1
IR SPECTRUM
  Mode    freq (cm**-1)   eps       Int (km/mol)
    1      -15.00         0.0       0.0
    2      120.50         0.0       4.4
Zero point energy                ...     0.111111 Eh
Thermal correction to Enthalpy   ...     0.222222 Eh
Thermal correction to Gibbs Free Energy ... 0.333333 Eh
MULLIKEN ATOMIC CHARGES
   0 C :   -0.123
   1 F :    0.123
Sum of atomic charges: 0.0000000
****ORCA TERMINATED NORMALLY****
"""
