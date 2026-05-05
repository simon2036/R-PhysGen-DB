"""Dataset migration registry validation."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from r_physgen_db.constants import DATA_DIR, PROJECT_ROOT
from r_physgen_db.paths import dataset_migrations_dir

REQUIRED_FRONT_MATTER_FIELDS = (
    "migration_id",
    "target_version",
    "compatibility",
    "rebuild_required",
    "affected_layers",
    "migration_script",
    "review_status",
)

ALLOWED_COMPATIBILITY = {
    "breaking",
    "additive",
    "patch",
    "data-only",
    "pipeline-only",
}

ALLOWED_AFFECTED_LAYERS = {
    "raw",
    "bronze",
    "silver",
    "gold",
    "extensions",
    "schemas",
    "docs",
    "pipeline",
    "duckdb",
    "tests",
    "ci",
}

ALLOWED_REVIEW_STATUS = {
    "draft",
    "applied",
    "released",
    "superseded",
}

CURRENT_VERSION_REVIEW_STATUSES = {"applied", "released"}

SEMVER_DATASET_VERSION_RE = re.compile(r"^v\d+\.\d+\.\d+(?:-(?:draft|rc\.\d+))?$")


def validate_dataset_migrations(project_root: Path = PROJECT_ROOT) -> dict[str, Any]:
    """Validate dataset migration records and current VERSION coverage."""

    project_root = Path(project_root)
    if project_root == PROJECT_ROOT:
        version_path = DATA_DIR / "gold" / "VERSION"
    else:
        version_path = project_root / "data" / "lake" / "gold" / "VERSION"
        legacy_version_path = project_root / "data" / "gold" / "VERSION"
        if not version_path.exists() and legacy_version_path.exists():
            version_path = legacy_version_path
    migration_dir = dataset_migrations_dir() if project_root == PROJECT_ROOT else project_root / "docs" / "migrations" / "dataset"
    if not migration_dir.exists() and project_root != PROJECT_ROOT:
        migration_dir = project_root / "docs" / "dataset_migrations"
    errors: list[str] = []
    records: list[dict[str, Any]] = []

    current_version = ""
    if version_path.exists():
        current_version = version_path.read_text(encoding="utf-8").strip()
    else:
        errors.append(f"missing dataset VERSION file: {version_path.relative_to(project_root)}")

    if current_version and not _is_semver_dataset_version(current_version):
        errors.append(f"current VERSION must use SemVer dataset format: {current_version}")

    if not migration_dir.exists():
        errors.append(f"missing dataset migration directory: {migration_dir.relative_to(project_root)}")
        record_paths: list[Path] = []
    else:
        record_paths = sorted(path for path in migration_dir.glob("*.md") if path.name != "TEMPLATE.md")

    if not record_paths:
        errors.append("no dataset migration records found")

    for record_path in record_paths:
        record, record_errors = _load_and_validate_record(record_path, project_root)
        records.append(record)
        errors.extend(record_errors)

    matching_current_version_records = sorted(
        record["path"]
        for record in records
        if record.get("target_version") == current_version and record.get("review_status") in CURRENT_VERSION_REVIEW_STATUSES
    )
    if current_version and not matching_current_version_records:
        errors.append(
            "current VERSION has no applied or released migration record: "
            f"{current_version}"
        )

    return {
        "current_version": current_version,
        "record_count": len(records),
        "records": records,
        "matching_current_version_records": matching_current_version_records,
        "errors": errors,
    }


def _load_and_validate_record(record_path: Path, project_root: Path) -> tuple[dict[str, Any], list[str]]:
    relative_path = record_path.relative_to(project_root).as_posix()
    errors: list[str] = []

    try:
        front_matter = _extract_front_matter(record_path)
    except ValueError as exc:
        return {"path": record_path.name}, [f"{relative_path}: {exc}"]

    record: dict[str, Any] = {"path": record_path.name, **front_matter}

    for field in REQUIRED_FRONT_MATTER_FIELDS:
        if field not in front_matter:
            errors.append(f"{relative_path}: missing required front matter field: {field}")

    target_version = str(front_matter.get("target_version", "")).strip()
    if target_version and not _is_semver_dataset_version(target_version):
        errors.append(f"{relative_path}: target_version must use SemVer dataset format: {target_version}")

    compatibility = str(front_matter.get("compatibility", "")).strip()
    if compatibility and compatibility not in ALLOWED_COMPATIBILITY:
        errors.append(f"{relative_path}: invalid compatibility: {compatibility}")

    if "rebuild_required" in front_matter and not isinstance(front_matter["rebuild_required"], bool):
        errors.append(f"{relative_path}: rebuild_required must be a boolean")

    affected_layers = front_matter.get("affected_layers", [])
    if not isinstance(affected_layers, list) or not affected_layers:
        errors.append(f"{relative_path}: affected_layers must be a non-empty list")
    else:
        for layer in affected_layers:
            layer_name = str(layer).strip()
            if layer_name not in ALLOWED_AFFECTED_LAYERS:
                errors.append(f"{relative_path}: invalid affected layer: {layer_name}")

    migration_script = str(front_matter.get("migration_script", "")).strip()
    if migration_script and migration_script != "none":
        script_path = project_root / migration_script
        if not script_path.exists():
            errors.append(f"{relative_path}: migration_script does not exist: {migration_script}")

    review_status = str(front_matter.get("review_status", "")).strip()
    if review_status and review_status not in ALLOWED_REVIEW_STATUS:
        errors.append(f"{relative_path}: invalid review_status: {review_status}")

    return record, errors


def _extract_front_matter(record_path: Path) -> dict[str, Any]:
    text = record_path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        raise ValueError("missing YAML front matter")

    end_marker = text.find("\n---", 4)
    if end_marker == -1:
        raise ValueError("unterminated YAML front matter")

    payload = yaml.safe_load(text[4:end_marker]) or {}
    if not isinstance(payload, dict):
        raise ValueError("YAML front matter must be a mapping")
    return payload


def _is_semver_dataset_version(version: str) -> bool:
    return bool(SEMVER_DATASET_VERSION_RE.fullmatch(version.strip()))
