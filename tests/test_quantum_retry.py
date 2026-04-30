from __future__ import annotations

import json
import stat
import sys
import tarfile

import pandas as pd
import pytest

from r_physgen_db.psi4_quantum import parse_psi4_scalar_features
from r_physgen_db.quantum_pilot import QUANTUM_CANONICAL_FEATURE_KEYS, merge_quantum_result_rows
from r_physgen_db.xtb_quantum import _write_sf5_spread_xyz, parse_xtb_scalar_features, run_xtb_quantum_pilot


def test_merge_quantum_result_rows_removes_blank_failure_after_retry_success() -> None:
    failure = _quantum_row("", "", converged=0, notes="original failed audit")
    success_rows = [_quantum_row(feature_key, -1.0) for feature_key in QUANTUM_CANONICAL_FEATURE_KEYS]

    merged = merge_quantum_result_rows(pd.DataFrame([failure]), pd.DataFrame(success_rows))

    assert len(merged) == len(QUANTUM_CANONICAL_FEATURE_KEYS)
    assert not merged["canonical_feature_key"].fillna("").astype(str).str.strip().eq("").any()
    assert set(merged["canonical_feature_key"]) == QUANTUM_CANONICAL_FEATURE_KEYS


def test_merge_quantum_result_rows_removes_cross_theory_failure_after_program_success() -> None:
    failure = {
        **_quantum_row("", "", converged=0, notes="fallback failed audit"),
        "theory_level": "GFN1-xTB",
    }
    success_rows = [_quantum_row(feature_key, -1.0, notes="gfn2 success") for feature_key in QUANTUM_CANONICAL_FEATURE_KEYS]

    merged = merge_quantum_result_rows(pd.DataFrame([failure]), pd.DataFrame(success_rows))

    assert len(merged) == len(QUANTUM_CANONICAL_FEATURE_KEYS)
    assert not merged["canonical_feature_key"].fillna("").astype(str).str.strip().eq("").any()
    assert set(merged["theory_level"]) == {"GFN2-xTB"}


def test_xtb_retry_failed_only_writes_attempt_bundle_and_skips_successes(tmp_path) -> None:
    requests_path = tmp_path / "quantum_pilot_requests.csv"
    xyz_manifest_path = tmp_path / "quantum_pilot_xyz_manifest.csv"
    output_path = tmp_path / "quantum_pilot_results.csv"
    artifact_dir = tmp_path / "artifacts"
    xyz_ok = tmp_path / "qreq_ok.xyz"
    xyz_fail = tmp_path / "qreq_fail.xyz"
    xyz_never = tmp_path / "qreq_never.xyz"
    for path in (xyz_ok, xyz_fail, xyz_never):
        path.write_text("1\nfixture\nH 0 0 0\n", encoding="utf-8")
    pd.DataFrame(
        [
            {"request_id": "qreq_ok", "mol_id": "mol_ok", "status": "ready_for_executor"},
            {"request_id": "qreq_fail", "mol_id": "mol_fail", "status": "ready_for_executor"},
            {"request_id": "qreq_never", "mol_id": "mol_never", "status": "ready_for_executor"},
        ]
    ).to_csv(requests_path, index=False)
    pd.DataFrame(
        [
            {"request_id": "qreq_ok", "mol_id": "mol_ok", "xyz_path": str(xyz_ok), "xyz_status": "generated"},
            {"request_id": "qreq_fail", "mol_id": "mol_fail", "xyz_path": str(xyz_fail), "xyz_status": "generated"},
            {"request_id": "qreq_never", "mol_id": "mol_never", "xyz_path": str(xyz_never), "xyz_status": "generated"},
        ]
    ).to_csv(xyz_manifest_path, index=False)
    existing = [_quantum_row(feature_key, -2.0, request_id="qreq_ok", mol_id="mol_ok") for feature_key in QUANTUM_CANONICAL_FEATURE_KEYS]
    existing.append(_quantum_row("", "", request_id="qreq_fail", mol_id="mol_fail", converged=0, notes="failed before retry"))
    pd.DataFrame(existing).to_csv(output_path, index=False)

    counter_path = tmp_path / "fake_xtb_counter.txt"
    fake_xtb = tmp_path / "fake_xtb.py"
    fake_xtb.write_text(
        f"""#!{sys.executable}
from __future__ import annotations
import json
import pathlib

counter = pathlib.Path({str(counter_path)!r})
counter.write_text(str(int(counter.read_text() or "0") + 1) if counter.exists() else "1")
pathlib.Path("xtbout.json").write_text(json.dumps({{
    "total energy": -5.0,
    "HOMO-LUMO gap / eV": 10.0,
    "orbital energies / eV": [-12.0, -1.5, 8.5],
    "fractional occupation": [2.0, 2.0, 0.0],
    "xtb version": "fake-6.7.1",
}}))
print(" Mol. alpha(0) /au        :          7.5")
print("molecular dipole:")
print("                 x           y           z       tot (Debye)")
print("   full:        0.000       0.000       0.500       1.25")
""",
        encoding="utf-8",
    )
    fake_xtb.chmod(fake_xtb.stat().st_mode | stat.S_IXUSR)

    summary = run_xtb_quantum_pilot(
        requests_path=requests_path,
        xyz_manifest_path=xyz_manifest_path,
        output_path=output_path,
        artifact_dir=artifact_dir,
        xtb_bin=fake_xtb,
        retry_failed_only=True,
        completion_required=True,
        retry_profiles=["gfn2_opt"],
    )

    rows = pd.read_csv(output_path).fillna("")
    fail_rows = rows.loc[rows["request_id"].eq("qreq_fail")]
    assert summary["requested"] == 1
    assert summary["succeeded"] == 1
    assert summary["failed"] == 0
    assert counter_path.read_text(encoding="utf-8") == "1"
    assert len(fail_rows) == len(QUANTUM_CANONICAL_FEATURE_KEYS)
    assert not fail_rows["canonical_feature_key"].astype(str).str.strip().eq("").any()
    assert not rows["request_id"].eq("qreq_never").any()
    attempt_dir = artifact_dir / "qreq_fail" / "attempt_01_gfn2_opt"
    assert (attempt_dir / "xtbout.json").exists()
    artifact_path = artifact_dir / "qreq_fail" / "qreq_fail_xtb_artifact.tar.gz"
    with tarfile.open(artifact_path) as archive:
        names = set(archive.getnames())
    assert "attempt_01_gfn2_opt/xtbout.json" in names
    assert "attempt_01_gfn2_opt/manifest.sha256" in names


def test_xtb_resume_summary_counts_only_targeted_completed_requests(tmp_path) -> None:
    requests_path = tmp_path / "quantum_pilot_requests.csv"
    xyz_manifest_path = tmp_path / "quantum_pilot_xyz_manifest.csv"
    output_path = tmp_path / "quantum_pilot_results.csv"
    artifact_dir = tmp_path / "artifacts"
    xyz_path = tmp_path / "qreq_target.xyz"
    xyz_path.write_text("1\ntarget\nH 0 0 0\n", encoding="utf-8")
    pd.DataFrame([{"request_id": "qreq_target", "mol_id": "mol_target", "status": "completed"}]).to_csv(requests_path, index=False)
    pd.DataFrame(
        [{"request_id": "qreq_target", "mol_id": "mol_target", "xyz_path": str(xyz_path), "xyz_status": "generated"}]
    ).to_csv(xyz_manifest_path, index=False)
    existing = [
        *[_quantum_row(feature_key, -2.0, request_id="qreq_target", mol_id="mol_target") for feature_key in QUANTUM_CANONICAL_FEATURE_KEYS],
        *[
            _quantum_row(feature_key, -3.0, request_id="qreq_unrelated", mol_id="mol_unrelated")
            for feature_key in QUANTUM_CANONICAL_FEATURE_KEYS
        ],
    ]
    pd.DataFrame(existing).to_csv(output_path, index=False)

    summary = run_xtb_quantum_pilot(
        requests_path=requests_path,
        xyz_manifest_path=xyz_manifest_path,
        output_path=output_path,
        artifact_dir=artifact_dir,
        xtb_bin=None,
        allow_missing_executor=True,
        completion_required=True,
    )

    assert summary["requested"] == 0
    assert summary["resumed"] == 1
    assert summary["merged_rows"] == 2 * len(QUANTUM_CANONICAL_FEATURE_KEYS)


def test_xtb_completion_required_raises_when_retry_target_still_fails(tmp_path) -> None:
    requests_path = tmp_path / "quantum_pilot_requests.csv"
    xyz_manifest_path = tmp_path / "quantum_pilot_xyz_manifest.csv"
    output_path = tmp_path / "quantum_pilot_results.csv"
    artifact_dir = tmp_path / "artifacts"
    xyz_path = tmp_path / "qreq_fail.xyz"
    xyz_path.write_text("1\nfixture\nH 0 0 0\n", encoding="utf-8")
    pd.DataFrame([{"request_id": "qreq_fail", "mol_id": "mol_fail", "status": "ready_for_executor"}]).to_csv(requests_path, index=False)
    pd.DataFrame([{"request_id": "qreq_fail", "mol_id": "mol_fail", "xyz_path": str(xyz_path), "xyz_status": "generated"}]).to_csv(
        xyz_manifest_path,
        index=False,
    )
    pd.DataFrame([_quantum_row("", "", request_id="qreq_fail", mol_id="mol_fail", converged=0)]).to_csv(output_path, index=False)
    fake_xtb = tmp_path / "fake_xtb.py"
    fake_xtb.write_text(f"#!{sys.executable}\nraise SystemExit(2)\n", encoding="utf-8")
    fake_xtb.chmod(fake_xtb.stat().st_mode | stat.S_IXUSR)

    with pytest.raises(RuntimeError, match="xTB completion required"):
        run_xtb_quantum_pilot(
            requests_path=requests_path,
            xyz_manifest_path=xyz_manifest_path,
            output_path=output_path,
            artifact_dir=artifact_dir,
            xtb_bin=fake_xtb,
            retry_failed_only=True,
            completion_required=True,
            retry_profiles=["gfn2_opt"],
        )


def test_xtb_parser_tolerates_overflow_stars_in_json_orbital_tail(tmp_path) -> None:
    json_path = tmp_path / "xtbout.json"
    stdout_path = tmp_path / "xtb.stdout"
    json_path.write_text(
        """{
  "total energy": -5.0,
  "HOMO-LUMO gap / eV": 10.0,
  "orbital energies / eV": [-12.0, -1.5, 8.5, ***************],
  "fractional occupation": [2.0, 2.0, 0.0, 0.0],
  "xtb version": "fake-6.7.1"
}
""",
        encoding="utf-8",
    )
    stdout_path.write_text(
        """
 Mol. alpha(0) /au        :          7.5
molecular dipole:
                 x           y           z       tot (Debye)
   full:        0.000       0.000       0.500       1.25
""",
        encoding="utf-8",
    )

    parsed = parse_xtb_scalar_features(json_path, stdout_path)

    assert parsed.values["quantum.homo_energy"] == pytest.approx(-1.5)
    assert parsed.values["quantum.lumo_energy"] == pytest.approx(8.5)
    assert parsed.values["quantum.homo_lumo_gap"] == pytest.approx(10.0)


def test_xtb_sf5_spread_preprocessor_decollides_hypervalent_fluorines(tmp_path) -> None:
    input_xyz = tmp_path / "collapsed_sf5.xyz"
    output_xyz = tmp_path / "spread_sf5.xyz"
    request_row = {
        "request_id": "qreq_sf5",
        "mol_id": "mol_sf5",
        "isomeric_smiles": "FC(F)=C(F)OS(F)(F)(F)(F)F",
    }
    input_xyz.write_text(
        """12
collapsed
F 1.0 0.0 0.0
C 0.0 0.0 0.0
F 0.0 1.0 0.0
C -1.0 0.0 0.0
F -1.0 -1.0 0.0
O -2.0 0.0 0.0
S -3.0 0.0 0.0
F -4.0 0.0 0.0
F -4.0 0.0 0.0
F -4.0 0.0 0.0
F -4.0 0.0 0.0
F -4.0 0.0 0.0
""",
        encoding="utf-8",
    )

    _write_sf5_spread_xyz(request_row, input_xyz, output_xyz)

    lines = output_xyz.read_text(encoding="utf-8").splitlines()[2:]
    coords = [tuple(round(float(part), 3) for part in line.split()[1:4]) for line in lines[7:12]]
    assert len(set(coords)) == 5
    assert all(sum((coords[i][axis] - coords[j][axis]) ** 2 for axis in range(3)) > 0.5 for i in range(5) for j in range(i))


def test_psi4_parser_extracts_wavefunction_orbitals_and_property_variables(tmp_path) -> None:
    result_path = tmp_path / "psi4_result.json"
    result_path.write_text(
        json.dumps(
            {
                "program_version": "psi4-fake-1.9",
                "theory_level": "B3LYP-D3BJ",
                "basis_set": "def2-SVP",
                "values": {"total_energy_eh": -154.25},
                "wavefunction": {
                    "epsilon_a_hartree": [-0.3013138, -0.0330741],
                    "nalpha": 1,
                },
                "variables": {
                    "CURRENT DIPOLE X": 0.0,
                    "CURRENT DIPOLE Y": 0.0,
                    "CURRENT DIPOLE Z": 1.0,
                    "CURRENT POLARIZABILITY XX": 30.0,
                    "CURRENT POLARIZABILITY YY": 33.0,
                    "CURRENT POLARIZABILITY ZZ": 36.0,
                },
            }
        ),
        encoding="utf-8",
    )

    parsed = parse_psi4_scalar_features(result_path)

    assert parsed.values["quantum.total_energy"] == pytest.approx(-154.25)
    assert parsed.values["quantum.homo_energy"] == pytest.approx(-8.199, abs=1e-3)
    assert parsed.values["quantum.lumo_energy"] == pytest.approx(-0.900, abs=1e-3)
    assert parsed.values["quantum.homo_lumo_gap"] == pytest.approx(7.299, abs=1e-3)
    assert parsed.values["quantum.dipole_moment"] == pytest.approx(2.541746473)
    assert parsed.values["quantum.polarizability"] == pytest.approx(33.0)


def test_psi4_parser_accepts_oeprop_vector_and_response_polarizability_names(tmp_path) -> None:
    result_path = tmp_path / "psi4_result.json"
    result_path.write_text(
        json.dumps(
            {
                "program_version": "psi4-fake-1.10",
                "values": {"total_energy_eh": -75.0, "homo_ev": -8.0, "lumo_ev": -1.0},
                "variables": {
                    "SCF DIPOLE": [0.0, 0.0, 0.5],
                    "DIPOLE POLARIZABILITY XX": 12.0,
                    "DIPOLE POLARIZABILITY YY": 15.0,
                    "DIPOLE POLARIZABILITY ZZ": 18.0,
                },
            }
        ),
        encoding="utf-8",
    )

    parsed = parse_psi4_scalar_features(result_path)

    assert parsed.values["quantum.dipole_moment"] == pytest.approx(0.5 * 2.541746473)
    assert parsed.values["quantum.polarizability"] == pytest.approx(15.0)


def _quantum_row(
    canonical_feature_key: str,
    value_num: float | str,
    *,
    request_id: str = "qreq_retry",
    mol_id: str = "mol_retry",
    converged: int = 1,
    notes: str = "fixture",
) -> dict[str, object]:
    return {
        "request_id": request_id,
        "mol_id": mol_id,
        "canonical_feature_key": canonical_feature_key,
        "value_num": value_num,
        "unit": "eV" if canonical_feature_key else "",
        "program": "xtb",
        "program_version": "fake-6.7.1",
        "method_family": "semiempirical",
        "theory_level": "GFN2-xTB",
        "basis_set": "",
        "solvation_model": "gas_phase",
        "converged": converged,
        "imaginary_frequency_count": 0,
        "artifact_uri": "manual://artifact",
        "artifact_sha256": "a" * 64,
        "quality_level": "calculated_open_source",
        "notes": notes,
    }
