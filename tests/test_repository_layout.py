from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_ROOT = ROOT / "archive" / "2026-05-05"


def _tracked_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    return [Path(item.decode("utf-8")) for item in result.stdout.split(b"\0") if item]


def test_tracked_root_contains_only_project_entrypoints_and_categorized_dirs() -> None:
    allowed_roots = {
        ".gitattributes",
        ".github",
        ".gitignore",
        "AGENTS.md",
        "PLAN.md",
        "README.md",
        "archive",
        "data",
        "deploy",
        "docs",
        "pipelines",
        "requirements.txt",
        "schemas",
        "scripts",
        "src",
        "tests",
    }

    ignored_local_roots = {".cache", ".git", ".omx", ".pytest_cache", ".trash", ".venv"}
    tracked_roots = {
        path.name
        for path in ROOT.iterdir()
        if path.name not in ignored_local_roots
    }

    assert tracked_roots <= allowed_roots


def test_docs_have_index_and_no_tracked_top_level_mixed_documents() -> None:
    assert (ROOT / "docs" / "README.md").exists()

    mixed_top_level_docs = [
        path.relative_to(ROOT).as_posix()
        for path in (ROOT / "docs").iterdir()
        if path.is_file() and path.name != "README.md"
    ]

    assert mixed_top_level_docs == []


def test_data_files_live_under_lake_sources_indexes_or_local_artifact_policy() -> None:
    assert (ROOT / "data" / "README.md").exists()
    for layer in ["raw", "bronze", "silver", "gold", "extensions"]:
        assert (ROOT / "data" / "lake" / layer).exists()
    assert (ROOT / "data" / "sources").exists()
    assert (ROOT / "data" / "indexes").exists()
    assert (ROOT / "data" / "artifacts" / "local").exists()

    misplaced = [
        path.as_posix()
        for path in [ROOT / "data" / name for name in ["raw", "bronze", "silver", "gold", "extensions", "index"]]
        if path.exists()
    ]
    assert misplaced == []

    local_artifacts = [path.as_posix() for path in _tracked_files() if path.as_posix().startswith("data/artifacts/local/")]
    assert local_artifacts == []


def test_static_frontend_is_categorized_and_legacy_frontend_bundles_are_archived() -> None:
    assert (ROOT / "deploy" / "static" / "R-PhysGen-DB.html").exists()
    assert not (ROOT / "R-PhysGen-DB.html").exists()
    assert not (ROOT / "r-physgen-db-frontend.zip").exists()
    assert not (ROOT / "前端v2据库.zip").exists()
    assert not (ROOT / "前端v3库.zip").exists()
    assert not (ROOT / "前端claudeclaudedesignclaudedesignv1.zip").exists()


def test_archive_manifest_records_moves_without_trash_paths() -> None:
    manifest = ARCHIVE_ROOT / "manifest.md"

    assert manifest.exists()
    text = manifest.read_text(encoding="utf-8")
    for expected in [
        "R-PhysGen-DB.html",
        "r-physgen-db-frontend.zip",
        "methods/R-PhysGen-DB_P0_package_v3.zip",
        "周报/R-PhysGen-DB_第一阶段数据库汇总20260423.docx",
    ]:
        assert expected in text
    assert ".trash" not in text


def test_published_reports_do_not_leak_trash_paths() -> None:
    report_paths = [
        ROOT / "data" / "lake" / "gold" / "quality_report.json",
        ROOT / "data" / "lake" / "gold" / "validation_report.json",
    ]

    for path in report_paths:
        assert path.exists()
        assert ".trash" not in path.read_text(encoding="utf-8")


def test_path_constants_expose_new_layout_with_legacy_fallbacks() -> None:
    from r_physgen_db.paths import (  # noqa: PLC0415
        DATA_ARTIFACTS_LOCAL_DIR,
        DATA_DIR,
        DATA_INDEX_DIR,
        DATA_LAKE_DIR,
        DATA_ROOT,
        DATA_SOURCES_DIR,
        STATIC_FRONTEND_HTML,
        legacy_data_path,
    )

    assert DATA_ROOT == ROOT / "data"
    assert DATA_LAKE_DIR == ROOT / "data" / "lake"
    assert DATA_DIR == DATA_LAKE_DIR
    assert DATA_SOURCES_DIR == ROOT / "data" / "sources"
    assert DATA_INDEX_DIR == ROOT / "data" / "indexes"
    assert DATA_ARTIFACTS_LOCAL_DIR == ROOT / "data" / "artifacts" / "local"
    assert STATIC_FRONTEND_HTML == ROOT / "deploy" / "static" / "R-PhysGen-DB.html"
    assert legacy_data_path("gold", "VERSION") == ROOT / "data" / "gold" / "VERSION"
