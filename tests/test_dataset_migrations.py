from __future__ import annotations

from pathlib import Path

from r_physgen_db.constants import PROJECT_ROOT
from r_physgen_db.dataset_migrations import validate_dataset_migrations


def _write_version(root: Path, version: str) -> None:
    version_path = root / "data" / "gold" / "VERSION"
    version_path.parent.mkdir(parents=True)
    version_path.write_text(version + "\n", encoding="utf-8")


def _write_record(root: Path, name: str, front_matter: str) -> Path:
    record_path = root / "docs" / "dataset_migrations" / name
    record_path.parent.mkdir(parents=True)
    record_path.write_text(f"---\n{front_matter.strip()}\n---\n\n# Migration\n", encoding="utf-8")
    return record_path


def test_current_dataset_version_has_matching_migration_record() -> None:
    result = validate_dataset_migrations(PROJECT_ROOT)

    assert result["errors"] == []
    assert result["current_version"] == "v1.5.0-draft"
    assert "2026-04-24-v1.5.0-draft-baseline.md" in result["matching_current_version_records"]


def test_valid_baseline_record_passes_validation(tmp_path: Path) -> None:
    _write_version(tmp_path, "v2.0.0-draft")
    _write_record(
        tmp_path,
        "2026-05-01-v2.0.0-draft-baseline.md",
        """
migration_id: 2026-05-01-v2.0.0-draft-baseline
target_version: v2.0.0-draft
compatibility: additive
rebuild_required: true
affected_layers:
  - schemas
  - gold
migration_script: none
review_status: applied
""",
    )

    result = validate_dataset_migrations(tmp_path)

    assert result["errors"] == []
    assert result["matching_current_version_records"] == ["2026-05-01-v2.0.0-draft-baseline.md"]


def test_non_semver_target_version_fails(tmp_path: Path) -> None:
    _write_version(tmp_path, "2026.05.01")
    _write_record(
        tmp_path,
        "invalid-version.md",
        """
migration_id: invalid-version
target_version: 2026.05.01
compatibility: additive
rebuild_required: true
affected_layers:
  - gold
migration_script: none
review_status: applied
""",
    )

    result = validate_dataset_migrations(tmp_path)

    assert any("target_version must use SemVer dataset format" in error for error in result["errors"])


def test_missing_required_front_matter_field_fails(tmp_path: Path) -> None:
    _write_version(tmp_path, "v2.0.0")
    _write_record(
        tmp_path,
        "missing-field.md",
        """
migration_id: missing-field
target_version: v2.0.0
compatibility: additive
rebuild_required: true
affected_layers:
  - gold
migration_script: none
""",
    )

    result = validate_dataset_migrations(tmp_path)

    assert any("missing required front matter field: review_status" in error for error in result["errors"])


def test_invalid_compatibility_and_layer_fail(tmp_path: Path) -> None:
    _write_version(tmp_path, "v2.0.0")
    _write_record(
        tmp_path,
        "invalid-enums.md",
        """
migration_id: invalid-enums
target_version: v2.0.0
compatibility: compatible
rebuild_required: true
affected_layers:
  - warehouse
migration_script: none
review_status: applied
""",
    )

    result = validate_dataset_migrations(tmp_path)

    assert any("invalid compatibility" in error for error in result["errors"])
    assert any("invalid affected layer: warehouse" in error for error in result["errors"])


def test_missing_migration_script_fails(tmp_path: Path) -> None:
    _write_version(tmp_path, "v2.0.0")
    _write_record(
        tmp_path,
        "missing-script.md",
        """
migration_id: missing-script
target_version: v2.0.0
compatibility: breaking
rebuild_required: true
affected_layers:
  - silver
migration_script: scripts/migrations/missing.py
review_status: applied
""",
    )

    result = validate_dataset_migrations(tmp_path)

    assert any("migration_script does not exist" in error for error in result["errors"])
