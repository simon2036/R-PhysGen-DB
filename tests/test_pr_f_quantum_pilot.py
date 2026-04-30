from __future__ import annotations

import hashlib
import json
import stat
import sys
from pathlib import Path

import pandas as pd
import pytest

from r_physgen_db.condition_sets import backfill_condition_sets
from r_physgen_db.pipeline import _build_model_dataset_index, _build_model_ready, _build_property_matrix, _build_quality_report
from r_physgen_db.quantum_pilot import (
    QUANTUM_CANONICAL_FEATURE_KEYS,
    QUANTUM_FORBIDDEN_WIDE_COLUMNS,
    QUANTUM_PROPERTY_NAMES,
    QUANTUM_SOURCE_ID,
    PSI4_DFT_BASIS_SET,
    PSI4_DFT_THEORY_LEVEL,
    build_psi4_dft_request_manifest,
    build_quantum_pilot_request_manifest,
    build_quantum_pilot,
    merge_quantum_result_rows,
)
from r_physgen_db.xtb_quantum import parse_xtb_scalar_features, run_xtb_quantum_pilot
from r_physgen_db.validate import _merge_quality_quantum_summary, _validate_quantum_pilot


def test_quantum_pilot_csv_builds_jobs_artifacts_and_observations(tmp_path) -> None:
    csv_path = tmp_path / "quantum_pilot_results.csv"
    pd.DataFrame(_quantum_six_scalar_rows()).to_csv(csv_path, index=False)

    result = build_quantum_pilot(csv_path, pd.DataFrame([{"mol_id": "mol_a"}]))
    observation = pd.DataFrame(result.property_rows)
    backfilled, condition_set, _ = backfill_condition_sets(observation)

    assert len(result.quantum_job) == 1
    assert len(result.quantum_artifact) == 1
    assert len(result.property_rows) == 6
    assert set(observation["source_id"]) == {QUANTUM_SOURCE_ID}
    assert set(observation["canonical_feature_key"]) == QUANTUM_CANONICAL_FEATURE_KEYS
    assert set(observation["property_name"]) == QUANTUM_PROPERTY_NAMES
    assert set(observation["quality_level"]) == {"computed_standard"}
    assert backfilled["condition_set_id"].notna().all()
    assert set(condition_set["condition_role"]) == {"gas_phase_298k"}
    assert result.summary["quantum_observation_count"] == 6


def test_quantum_pilot_ingest_dedupes_by_program_theory_feature_key(tmp_path) -> None:
    csv_path = tmp_path / "quantum_pilot_results.csv"
    rows = _quantum_six_scalar_rows()
    duplicate = dict(rows[0])
    duplicate["value_num"] = -7.7
    duplicate["notes"] = "newer duplicate"
    pd.DataFrame([rows[0], duplicate, *rows[1:]]).to_csv(csv_path, index=False)

    result = build_quantum_pilot(csv_path, pd.DataFrame([{"mol_id": "mol_a"}]))
    observation = pd.DataFrame(result.property_rows)
    homo_rows = observation.loc[observation["canonical_feature_key"].eq("quantum.homo_energy")]

    assert len(result.property_rows) == 6
    assert len(homo_rows) == 1
    assert homo_rows.iloc[0]["value_num"] == pytest.approx(-7.7)
    assert "newer duplicate" in homo_rows.iloc[0]["notes"]


def test_merge_quantum_result_rows_keeps_newest_per_program_theory_feature() -> None:
    existing = pd.DataFrame(
        [
            _quantum_csv_row("quantum.homo_energy", -8.1),
            _quantum_csv_row("quantum.lumo_energy", -1.2),
        ]
    )
    incoming = pd.DataFrame(
        [
            _quantum_csv_row("quantum.homo_energy", -7.9),
            {
                **_quantum_csv_row("quantum.homo_energy", -8.4),
                "program": "psi4",
                "method_family": "DFT",
                "theory_level": "B3LYP-D3BJ",
                "basis_set": "def2-SVP",
                "quality_level": "computed_high",
            },
        ]
    )

    merged = merge_quantum_result_rows(existing, incoming)

    assert len(merged) == 3
    xtb_homo = merged.loc[
        merged["program"].eq("ORCA")
        & merged["theory_level"].eq("B3LYP")
        & merged["canonical_feature_key"].eq("quantum.homo_energy")
    ].iloc[0]
    psi4_homo = merged.loc[merged["program"].eq("psi4")].iloc[0]
    assert xtb_homo["value_num"] == pytest.approx(-7.9)
    assert psi4_homo["value_num"] == pytest.approx(-8.4)
    assert psi4_homo["quality_level"] == "computed_high"


def test_quantum_pilot_missing_csv_returns_empty_tables(tmp_path) -> None:
    result = build_quantum_pilot(tmp_path / "missing.csv", pd.DataFrame([{"mol_id": "mol_a"}]))

    assert result.property_rows == []
    assert result.quantum_job.empty
    assert result.quantum_artifact.empty
    assert result.summary["input_status"] == "not_configured"
    assert result.summary["quantum_observation_count"] == 0


def test_quantum_request_manifest_generates_xyz_and_records_missing_executor_without_failing(tmp_path) -> None:
    molecule_core = pd.DataFrame(
        [
            {
                "mol_id": "mol_a",
                "canonical_smiles": "CC",
                "isomeric_smiles": "CC",
                "model_inclusion": "yes",
                "coverage_tier": "A",
            }
        ]
    )

    request_manifest, xyz_manifest, summary = build_quantum_pilot_request_manifest(
        molecule_core,
        tools_available=False,
        max_requests=1,
        xyz_dir=tmp_path / "quantum_xyz",
    )
    xyz_path = tmp_path / "quantum_xyz" / f"{request_manifest.iloc[0]['request_id']}.xyz"

    assert len(request_manifest) == 1
    assert len(xyz_manifest) == 1
    assert request_manifest.iloc[0]["status"] == "pending_executor_unavailable"
    assert request_manifest.iloc[0]["recommended_next_action"] == "run_quantum"
    assert xyz_manifest.iloc[0]["xyz_status"] == "generated"
    assert xyz_manifest.iloc[0]["xyz_path"] == str(xyz_path)
    assert xyz_path.exists()
    assert xyz_path.read_text(encoding="utf-8").splitlines()[0].strip().isdigit()
    assert summary["executor_available"] is False
    assert summary["xyz_generated_count"] == 1


def test_quantum_request_manifest_prefers_highest_priority_active_learning_run_quantum_entries(tmp_path) -> None:
    molecule_core = pd.DataFrame(
        [
            {
                "mol_id": "mol_low",
                "canonical_smiles": "CC",
                "isomeric_smiles": "CC",
                "model_inclusion": "yes",
                "coverage_tier": "A",
            },
            {
                "mol_id": "mol_high",
                "canonical_smiles": "CCC",
                "isomeric_smiles": "CCC",
                "model_inclusion": "no",
                "coverage_tier": "D",
            },
            {
                "mol_id": "mol_lit",
                "canonical_smiles": "CCCC",
                "isomeric_smiles": "CCCC",
                "model_inclusion": "yes",
                "coverage_tier": "A",
            },
        ]
    )
    active_learning_queue = pd.DataFrame(
        [
            {
                "queue_entry_id": "alq_low",
                "mol_id": "mol_low",
                "recommended_next_action": "run_quantum",
                "priority_score": 0.25,
                "status": "proposed",
            },
            {
                "queue_entry_id": "alq_high",
                "mol_id": "mol_high",
                "recommended_next_action": "run_quantum",
                "priority_score": 0.99,
                "status": "proposed",
            },
            {
                "queue_entry_id": "alq_lit",
                "mol_id": "mol_lit",
                "recommended_next_action": "literature_search",
                "priority_score": 1.0,
                "status": "proposed",
            },
        ]
    )

    request_manifest, _, summary = build_quantum_pilot_request_manifest(
        molecule_core,
        active_learning_queue=active_learning_queue,
        tools_available=False,
        max_requests=2,
        xyz_dir=tmp_path / "quantum_xyz",
    )

    assert request_manifest["mol_id"].tolist() == ["mol_high", "mol_low"]
    assert request_manifest["notes"].str.contains("active_learning_queue").all()
    assert summary["selection_source"] == "active_learning_queue"
    assert summary["executor_status"] == "unavailable"
    assert summary["status_counts"] == {"pending_executor_unavailable": 2}


def test_quantum_request_manifest_marks_completed_xtb_request_ids(tmp_path) -> None:
    molecule_core = pd.DataFrame(
        [
            {
                "mol_id": "mol_done",
                "canonical_smiles": "CC",
                "isomeric_smiles": "CC",
                "model_inclusion": "no",
                "coverage_tier": "D",
            }
        ]
    )
    completed_request_id = "qreq_" + hashlib.sha256("mol_done|CC|xtb-gfn2".encode("utf-8")).hexdigest()[:16]

    request_manifest, _, summary = build_quantum_pilot_request_manifest(
        molecule_core,
        completed_request_ids={completed_request_id},
        tools_available=False,
        max_requests=1,
        xyz_dir=tmp_path / "quantum_xyz",
    )

    assert request_manifest.iloc[0]["request_id"] == completed_request_id
    assert request_manifest.iloc[0]["status"] == "completed"
    assert "completed quantum pilot results" in request_manifest.iloc[0]["notes"]
    assert summary["status_counts"] == {"completed": 1}


def test_quantum_request_manifest_uses_env_request_count_and_trashes_stale_xyz(tmp_path, monkeypatch) -> None:
    molecule_core = pd.DataFrame(
        [
            {
                "mol_id": f"mol_{idx}",
                "canonical_smiles": "C" * (idx + 1),
                "isomeric_smiles": "C" * (idx + 1),
                "model_inclusion": "no",
                "coverage_tier": "D",
            }
            for idx in range(3)
        ]
    )
    active_learning_queue = pd.DataFrame(
        [
            {
                "queue_entry_id": f"alq_{idx}",
                "mol_id": f"mol_{idx}",
                "recommended_next_action": "run_quantum",
                "priority_score": 1.0 - idx / 10,
                "status": "proposed",
            }
            for idx in range(3)
        ]
    )
    xyz_dir = tmp_path / "data" / "raw" / "generated" / "quantum_xyz"
    xyz_dir.mkdir(parents=True)
    stale_xyz = xyz_dir / "stale_old_request.xyz"
    stale_xyz.write_text("1\nstale\nH 0 0 0\n", encoding="utf-8")
    monkeypatch.setenv("R_PHYSGEN_QUANTUM_MAX_REQUESTS", "2")

    request_manifest, xyz_manifest, summary = build_quantum_pilot_request_manifest(
        molecule_core,
        active_learning_queue=active_learning_queue,
        tools_available=False,
        xyz_dir=xyz_dir,
        trash_project_root=tmp_path,
    )

    assert len(request_manifest) == 2
    assert len(xyz_manifest) == 2
    assert request_manifest["mol_id"].tolist() == ["mol_0", "mol_1"]
    assert summary["request_count"] == 2
    assert summary["stale_xyz_trashed_count"] == 1
    assert not stale_xyz.exists()
    assert (tmp_path / ".trash" / "data" / "raw" / "generated" / "quantum_xyz" / "stale_old_request.xyz").exists()


def test_psi4_dft_request_manifest_stratifies_completed_xtb_molecules(tmp_path) -> None:
    molecule_core = pd.DataFrame(
        [
            {
                "mol_id": "mol_high_f",
                "canonical_smiles": "CF",
                "isomeric_smiles": "CF",
                "coverage_tier": "A",
                "scaffold_key": "methane",
            },
            {
                "mol_id": "mol_high_cl",
                "canonical_smiles": "CCl",
                "isomeric_smiles": "CCl",
                "coverage_tier": "A",
                "scaffold_key": "methane",
            },
            {
                "mol_id": "mol_low_hc",
                "canonical_smiles": "CC",
                "isomeric_smiles": "CC",
                "coverage_tier": "D",
                "scaffold_key": "ethane",
            },
        ]
    )
    active_learning_queue = pd.DataFrame(
        [
            {"queue_entry_id": "alq_f", "mol_id": "mol_high_f", "priority_score": 0.99, "recommended_next_action": "run_quantum"},
            {"queue_entry_id": "alq_cl", "mol_id": "mol_high_cl", "priority_score": 0.98, "recommended_next_action": "run_quantum"},
            {"queue_entry_id": "alq_hc", "mol_id": "mol_low_hc", "priority_score": 0.20, "recommended_next_action": "run_quantum"},
        ]
    )
    xtb_rows = []
    for mol_id in molecule_core["mol_id"]:
        xtb_artifact_root = tmp_path / "xtb_artifacts" / f"qreq_{mol_id}"
        xtb_artifact_root.mkdir(parents=True, exist_ok=True)
        (xtb_artifact_root / "xtbopt.xyz").write_text(
            f"1\nxtb optimized geometry for {mol_id}\nC 9.00000000 0.00000000 0.00000000\n",
            encoding="utf-8",
        )
        for feature_key in QUANTUM_CANONICAL_FEATURE_KEYS:
            xtb_rows.append(
                {
                    **_quantum_csv_row(feature_key, -1.0),
                    "request_id": f"qreq_{mol_id}",
                    "mol_id": mol_id,
                    "program": "xtb",
                    "method_family": "semiempirical",
                    "theory_level": "GFN2-xTB",
                    "basis_set": "",
                    "converged": 1,
                    "artifact_uri": str(xtb_artifact_root / f"qreq_{mol_id}_xtb_artifact.tar.gz"),
                }
            )

    request_manifest, xyz_manifest, summary = build_psi4_dft_request_manifest(
        molecule_core,
        xtb_results=pd.DataFrame(xtb_rows),
        active_learning_queue=active_learning_queue,
        max_requests=2,
        xyz_dir=tmp_path / "psi4_xyz",
    )

    assert len(request_manifest) == 2
    assert len(xyz_manifest) == 2
    assert set(request_manifest["program"]) == {"psi4"}
    assert set(request_manifest["method_family"]) == {"DFT"}
    assert set(request_manifest["basis_set"]) == {"def2-SVP"}
    assert request_manifest["mol_id"].tolist() == ["mol_high_f", "mol_low_hc"]
    high_f_xyz = xyz_manifest.loc[xyz_manifest["mol_id"].eq("mol_high_f"), "xyz_path"].iloc[0]
    assert "xtb optimized geometry for mol_high_f" in Path(high_f_xyz).read_text(encoding="utf-8")
    assert "copied from completed xTB optimized geometry" in xyz_manifest.loc[
        xyz_manifest["mol_id"].eq("mol_high_f"), "notes"
    ].iloc[0]
    assert summary["selection_source"] == "completed_xtb_stratified"
    assert summary["status_counts"] == {"pending_executor_unavailable": 2}


def test_psi4_dft_request_manifest_marks_completed_request_ids(tmp_path) -> None:
    molecule_core = pd.DataFrame(
        [
            {
                "mol_id": "mol_done",
                "canonical_smiles": "CF",
                "isomeric_smiles": "CF",
                "coverage_tier": "A",
                "scaffold_key": "methane",
            }
        ]
    )
    xtb_artifact_root = tmp_path / "xtb_artifacts" / "qreq_mol_done"
    xtb_artifact_root.mkdir(parents=True, exist_ok=True)
    (xtb_artifact_root / "xtbopt.xyz").write_text(
        "1\nxtb optimized geometry for mol_done\nC 0.00000000 0.00000000 0.00000000\n",
        encoding="utf-8",
    )
    xtb_rows = [
        {
            **_quantum_csv_row(feature_key, -1.0),
            "request_id": "qreq_mol_done",
            "mol_id": "mol_done",
            "program": "xtb",
            "theory_level": "GFN2-xTB",
            "converged": 1,
            "artifact_uri": str(xtb_artifact_root / "qreq_mol_done_xtb_artifact.tar.gz"),
        }
        for feature_key in QUANTUM_CANONICAL_FEATURE_KEYS
    ]
    completed_request_id = "qreq_" + hashlib.sha256(
        f"mol_done|CF|psi4|{PSI4_DFT_THEORY_LEVEL}|{PSI4_DFT_BASIS_SET}".encode("utf-8")
    ).hexdigest()[:16]

    request_manifest, _, summary = build_psi4_dft_request_manifest(
        molecule_core,
        xtb_results=pd.DataFrame(xtb_rows),
        completed_request_ids={completed_request_id},
        max_requests=1,
        tools_available=False,
        xyz_dir=tmp_path / "psi4_xyz",
    )

    assert request_manifest.iloc[0]["request_id"] == completed_request_id
    assert request_manifest.iloc[0]["status"] == "completed"
    assert "completed Psi4 DFT results already present" in request_manifest.iloc[0]["notes"]
    assert summary["status_counts"] == {"completed": 1}


def test_xtb_parser_extracts_six_scalar_features_from_json_and_stdout(tmp_path) -> None:
    json_path = tmp_path / "xtbout.json"
    stdout_path = tmp_path / "xtb.stdout"
    json_path.write_text(
        json.dumps(
            {
                "total energy": -5.07053662,
                "HOMO-LUMO gap / eV": 14.29298117,
                "orbital energies / eV": [-18.49, -15.46, -13.88, -12.14109228, 2.15188889, 6.77],
                "fractional occupation": [2.0, 2.0, 2.0, 2.0, 0.0, 0.0],
                "xtb version": "6.7.1 (edcfbbe)",
            }
        ),
        encoding="utf-8",
    )
    stdout_path.write_text(
        """
 Mol. α(0) /au        :          9.429201

molecular dipole:
                 x           y           z       tot (Debye)
 q only:       -0.000      -0.000       0.606
   full:       -0.000      -0.000       0.870       2.212
""",
        encoding="utf-8",
    )

    parsed = parse_xtb_scalar_features(json_path, stdout_path)

    assert parsed.program_version == "6.7.1 (edcfbbe)"
    assert parsed.values == {
        "quantum.homo_energy": pytest.approx(-12.14109228),
        "quantum.lumo_energy": pytest.approx(2.15188889),
        "quantum.homo_lumo_gap": pytest.approx(14.29298117),
        "quantum.total_energy": pytest.approx(-5.07053662),
        "quantum.dipole_moment": pytest.approx(2.212),
        "quantum.polarizability": pytest.approx(9.429201),
    }


def test_xtb_executor_fake_success_failure_resume_and_artifact_sha(tmp_path) -> None:
    requests_path = tmp_path / "quantum_pilot_requests.csv"
    xyz_manifest_path = tmp_path / "quantum_pilot_xyz_manifest.csv"
    output_path = tmp_path / "quantum_pilot_results.csv"
    artifact_dir = tmp_path / "artifacts"
    xyz_ok = tmp_path / "qreq_ok.xyz"
    xyz_fail = tmp_path / "qreq_fail.xyz"
    xyz_ok.write_text("1\nok\nH 0 0 0\n", encoding="utf-8")
    xyz_fail.write_text("1\nfail\nH 0 0 0\n", encoding="utf-8")
    pd.DataFrame(
        [
            {"request_id": "qreq_ok", "mol_id": "mol_ok", "status": "ready_for_executor"},
            {"request_id": "qreq_fail", "mol_id": "mol_fail", "status": "ready_for_executor"},
        ]
    ).to_csv(requests_path, index=False)
    pd.DataFrame(
        [
            {"request_id": "qreq_ok", "mol_id": "mol_ok", "xyz_path": str(xyz_ok), "xyz_status": "generated"},
            {"request_id": "qreq_fail", "mol_id": "mol_fail", "xyz_path": str(xyz_fail), "xyz_status": "generated"},
        ]
    ).to_csv(xyz_manifest_path, index=False)
    counter_path = tmp_path / "fake_xtb_counter.txt"
    fake_xtb = tmp_path / "fake_xtb.py"
    fake_xtb.write_text(
        f"""#!{sys.executable}
from __future__ import annotations
import json
import pathlib
import sys

counter = pathlib.Path({str(counter_path)!r})
counter.write_text(str(int(counter.read_text() or "0") + 1) if counter.exists() else "1")
xyz = pathlib.Path(sys.argv[1]).name
if "fail" in xyz:
    pathlib.Path("xtbout.json").write_text("{{}}")
    pathlib.Path("charges").write_text("")
    pathlib.Path("wbo").write_text("")
    print("simulated failure")
    sys.exit(2)
pathlib.Path("xtbout.json").write_text(json.dumps({{
    "total energy": -5.0,
    "HOMO-LUMO gap / eV": 10.0,
    "orbital energies / eV": [-12.0, -1.5, 8.5],
    "fractional occupation": [2.0, 2.0, 0.0],
    "xtb version": "fake-6.7.1",
}}))
pathlib.Path("charges").write_text("0.0\\n")
pathlib.Path("wbo").write_text("1 1 0.0\\n")
print(" Mol. alpha(0) /au        :          7.5")
print("molecular dipole:")
print("                 x           y           z       tot (Debye)")
print("   full:        0.000       0.000       0.500       1.25")
""",
        encoding="utf-8",
    )
    fake_xtb.chmod(fake_xtb.stat().st_mode | stat.S_IXUSR)

    first = run_xtb_quantum_pilot(
        requests_path=requests_path,
        xyz_manifest_path=xyz_manifest_path,
        output_path=output_path,
        artifact_dir=artifact_dir,
        xtb_bin=fake_xtb,
        limit=2,
        jobs=1,
        threads_per_job=1,
        resume=True,
    )
    second = run_xtb_quantum_pilot(
        requests_path=requests_path,
        xyz_manifest_path=xyz_manifest_path,
        output_path=output_path,
        artifact_dir=artifact_dir,
        xtb_bin=fake_xtb,
        limit=2,
        jobs=1,
        threads_per_job=1,
        resume=True,
    )

    rows = pd.read_csv(output_path).fillna("")
    ok_rows = rows.loc[rows["request_id"].eq("qreq_ok")]
    fail_rows = rows.loc[rows["request_id"].eq("qreq_fail")]
    assert first["succeeded"] == 1
    assert first["failed"] == 1
    assert second["resumed"] == 2
    assert counter_path.read_text(encoding="utf-8") == "2"
    assert len(ok_rows) == 6
    assert set(ok_rows["canonical_feature_key"]) == QUANTUM_CANONICAL_FEATURE_KEYS
    assert len(fail_rows) == 1
    assert fail_rows.iloc[0]["converged"] == 0
    assert ok_rows["artifact_sha256"].str.fullmatch(r"[0-9a-f]{64}").all()
    assert fail_rows["artifact_sha256"].str.fullmatch(r"[0-9a-f]{64}").all()


def test_xtb_executor_can_write_missing_executor_audit_artifact(tmp_path) -> None:
    requests_path = tmp_path / "quantum_pilot_requests.csv"
    xyz_manifest_path = tmp_path / "quantum_pilot_xyz_manifest.csv"
    output_path = tmp_path / "quantum_pilot_results.csv"
    artifact_dir = tmp_path / "artifacts"
    xyz_path = tmp_path / "qreq_missing_xtb.xyz"
    xyz_path.write_text("1\nfixture\nH 0 0 0\n", encoding="utf-8")
    pd.DataFrame(
        [{"request_id": "qreq_missing_xtb", "mol_id": "mol_missing_xtb", "status": "pending_executor_unavailable"}]
    ).to_csv(requests_path, index=False)
    pd.DataFrame(
        [{"request_id": "qreq_missing_xtb", "mol_id": "mol_missing_xtb", "xyz_path": str(xyz_path), "xyz_status": "generated"}]
    ).to_csv(xyz_manifest_path, index=False)

    summary = run_xtb_quantum_pilot(
        requests_path=requests_path,
        xyz_manifest_path=xyz_manifest_path,
        output_path=output_path,
        artifact_dir=artifact_dir,
        xtb_bin=None,
        allow_missing_executor=True,
    )

    rows = pd.read_csv(output_path).fillna("")
    assert summary["failed"] == 1
    assert len(rows) == 1
    assert rows.iloc[0]["converged"] == 0
    assert "executor_unavailable" in rows.iloc[0]["notes"]
    assert len(rows.iloc[0]["artifact_sha256"]) == 64
    assert (artifact_dir / "qreq_missing_xtb" / "manifest.sha256").exists()


def test_quantum_request_manifest_falls_back_to_promoted_tiers_without_active_learning_queue(tmp_path) -> None:
    molecule_core = pd.DataFrame(
        [
            {
                "mol_id": "mol_tier_d",
                "canonical_smiles": "CC",
                "isomeric_smiles": "CC",
                "model_inclusion": "no",
                "coverage_tier": "D",
            },
            {
                "mol_id": "mol_tier_a",
                "canonical_smiles": "CCC",
                "isomeric_smiles": "CCC",
                "model_inclusion": "yes",
                "coverage_tier": "A",
            },
        ]
    )

    request_manifest, _, summary = build_quantum_pilot_request_manifest(
        molecule_core,
        active_learning_queue=pd.DataFrame(),
        tools_available=False,
        max_requests=2,
        xyz_dir=tmp_path / "quantum_xyz",
    )

    assert request_manifest["mol_id"].tolist() == ["mol_tier_a"]
    assert summary["selection_source"] == "promoted_coverage_fallback"


def test_completed_active_learning_quantum_rows_do_not_trigger_promoted_fallback(tmp_path) -> None:
    molecule_core = pd.DataFrame(
        [
            {
                "mol_id": "mol_completed_quantum",
                "canonical_smiles": "CC",
                "isomeric_smiles": "CC",
                "model_inclusion": "no",
                "coverage_tier": "D",
            },
            {
                "mol_id": "mol_promoted",
                "canonical_smiles": "CCC",
                "isomeric_smiles": "CCC",
                "model_inclusion": "yes",
                "coverage_tier": "A",
            },
        ]
    )
    active_learning_queue = pd.DataFrame(
        [
            {
                "queue_entry_id": "alq_done",
                "mol_id": "mol_completed_quantum",
                "recommended_next_action": "run_quantum",
                "priority_score": 0.99,
                "status": "completed",
            }
        ]
    )
    completed_request_id = "qreq_" + hashlib.sha256("mol_completed_quantum|CC|xtb-gfn2".encode("utf-8")).hexdigest()[:16]

    request_manifest, _, summary = build_quantum_pilot_request_manifest(
        molecule_core,
        active_learning_queue=active_learning_queue,
        completed_request_ids={completed_request_id},
        tools_available=False,
        max_requests=2,
        xyz_dir=tmp_path / "quantum_xyz",
    )

    assert request_manifest["mol_id"].tolist() == ["mol_completed_quantum"]
    assert request_manifest["status"].tolist() == ["completed"]
    assert summary["selection_source"] == "active_learning_queue"


def test_validation_quantum_summary_keeps_quality_report_request_manifest_and_input_path() -> None:
    validation_summary = {
        "input_status": "not_configured",
        "input_path": "",
        "input_row_count": 0,
        "quantum_job_count": 0,
        "quantum_artifact_count": 0,
        "quantum_observation_count": 0,
        "quantum_molecule_count": 0,
        "feature_counts": {},
    }
    quality_summary = {
        "input_status": "loaded",
        "input_path": "data/raw/manual/quantum_pilot_results.csv",
        "input_row_count": 3,
        "request_manifest": {
            "request_count": 2,
            "executor_available": False,
            "executor_status": "unavailable",
            "status_counts": {"pending_executor_unavailable": 2},
        },
    }

    merged = _merge_quality_quantum_summary(validation_summary, quality_summary)

    assert merged["input_status"] == "loaded"
    assert merged["input_path"] == "data/raw/manual/quantum_pilot_results.csv"
    assert merged["input_row_count"] == 3
    assert merged["request_manifest"] == quality_summary["request_manifest"]


def test_quantum_quality_report_and_wide_outputs_keep_boundary(tmp_path) -> None:
    csv_path = tmp_path / "quantum_pilot_results.csv"
    pd.DataFrame([_quantum_csv_row("quantum.homo_energy", -8.1)]).to_csv(csv_path, index=False)
    result = build_quantum_pilot(csv_path, pd.DataFrame([{"mol_id": "mol_a"}]))
    observation = pd.DataFrame(result.property_rows)
    recommended = pd.DataFrame(
        [
            {
                "mol_id": "mol_a",
                "property_name": "homo_ev",
                "value": "-8.1",
                "value_num": -8.1,
                "unit": "eV",
                "selected_source_id": QUANTUM_SOURCE_ID,
                "selected_source_name": "Quantum",
                "selected_quality_level": "computed_standard",
            }
        ]
    )

    report = _build_quality_report(
        seed_catalog=pd.DataFrame([{"seed_id": "seed_a", "entity_scope": "candidate", "coverage_tier": "D"}]),
        molecule_core=pd.DataFrame([{"mol_id": "mol_a", "seed_id": "seed_a", "model_inclusion": "yes"}]),
        property_observation=observation,
        property_recommended=recommended,
        model_ready=pd.DataFrame([{"mol_id": "mol_a", "split": "train"}]),
        qc_issues=pd.DataFrame(),
        resolution_df=pd.DataFrame(columns=["seed_id", "stage", "status", "detail"]),
        regulatory_status=pd.DataFrame(),
        pending_sources=pd.DataFrame(),
        quantum_summary=result.summary,
    )
    matrix = _build_property_matrix(recommended)
    model_index = _build_model_dataset_index(
        pd.DataFrame([{"mol_id": "mol_a", "scaffold_key": "scaf_a"}]),
        recommended,
        pd.DataFrame([{"mol_id": "mol_a", "model_inclusion": "yes"}]),
    )
    model_ready = _build_model_ready(
        pd.DataFrame([{"mol_id": "mol_a", "canonical_smiles": "CC", "isomeric_smiles": "CC", "selfies": "[C][C]", "scaffold_key": "scaf_a"}]),
        matrix,
        model_index,
    )

    assert report["quantum_pilot_summary"]["quantum_observation_count"] == 1
    assert not (QUANTUM_FORBIDDEN_WIDE_COLUMNS & set(matrix.columns))
    assert not (QUANTUM_FORBIDDEN_WIDE_COLUMNS & set(model_ready.columns))
    assert not (QUANTUM_FORBIDDEN_WIDE_COLUMNS & set(model_index.columns))
    assert int(model_index.loc[0, "source_coverage_count"]) == 0


def test_validate_quantum_pilot_detects_invalid_rows() -> None:
    property_observation = pd.DataFrame(
        [
            {
                "observation_id": "obs_bad",
                "mol_id": "mol_a",
                "property_name": "homo_ev",
                "value": "-1",
                "value_num": -1.0,
                "unit": "eV",
                "source_id": QUANTUM_SOURCE_ID,
                "source_record_id": "q_bad:quantum.bad",
                "canonical_feature_key": "quantum.bad",
                "quality_level": "computed_standard",
                "convergence_flag": 0,
                "condition_set_id": "",
            }
        ]
    )
    recommended = pd.DataFrame(
        [{"mol_id": "mol_a", "property_name": "homo_ev", "value": "-1", "value_num": -1.0, "selected_source_id": QUANTUM_SOURCE_ID}]
    )
    quantum_job = pd.DataFrame(
        [
            {
                "request_id": "q_bad",
                "mol_id": "mol_a",
                "status": "succeeded",
                "quality_level": "computed_standard",
                "converged": 1,
                "imaginary_frequency_count": 0,
                "derived_observation_count": 1,
                "source_id": QUANTUM_SOURCE_ID,
            }
        ]
    )
    quantum_artifact = pd.DataFrame(
        [
            {
                "artifact_id": "qart_bad",
                "request_id": "q_bad",
                "mol_id": "mol_a",
                "artifact_uri": "manual://bad",
                "artifact_sha256": "",
                "source_id": QUANTUM_SOURCE_ID,
            }
        ]
    )
    results = {"integration_checks": [], "errors": []}

    _validate_quantum_pilot(
        results,
        property_observation,
        recommended,
        pd.DataFrame([{"source_id": QUANTUM_SOURCE_ID}]),
        pd.DataFrame(columns=["condition_set_id", "condition_role"]),
        quantum_job,
        quantum_artifact,
        pd.DataFrame(columns=["mol_id", "homo_ev"]),
        pd.DataFrame(columns=["mol_id"]),
        pd.DataFrame(columns=["mol_id"]),
    )

    assert any("Unexpected quantum canonical feature keys" in error for error in results["errors"])
    assert any("convergence_flag=1" in error for error in results["errors"])
    assert any("missing condition_set_id" in error for error in results["errors"])
    assert any("blank artifact_sha256" in error for error in results["errors"])
    assert any("Quantum columns leaked into wide ML outputs" in error for error in results["errors"])


def _quantum_csv_row(canonical_feature_key: str, value_num: float) -> dict[str, object]:
    return {
        "request_id": "q_mol_a_b3lyp",
        "mol_id": "mol_a",
        "canonical_feature_key": canonical_feature_key,
        "value_num": value_num,
        "unit": "eV",
        "program": "ORCA",
        "program_version": "6.0",
        "method_family": "DFT",
        "theory_level": "B3LYP",
        "basis_set": "6-311+G(2d,p)",
        "solvation_model": "gas_phase",
        "converged": 1,
        "imaginary_frequency_count": 0,
        "artifact_uri": "manual://quantum/q_mol_a_b3lyp",
        "artifact_sha256": "a" * 64,
        "quality_level": "computed_standard",
        "notes": "fixture",
    }


def _quantum_six_scalar_rows() -> list[dict[str, object]]:
    return [
        _quantum_csv_row("quantum.homo_energy", -8.1),
        _quantum_csv_row("quantum.lumo_energy", -1.2),
        _quantum_csv_row("quantum.homo_lumo_gap", 6.9),
        _quantum_csv_row("quantum.total_energy", -455.123),
        _quantum_csv_row("quantum.dipole_moment", 2.4),
        _quantum_csv_row("quantum.polarizability", 47.2),
    ]
