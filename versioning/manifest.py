"""Snapshot manifest models and persistence utilities."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, Field


class SnapshotChecksums(BaseModel):
    """Checksums for snapshot verification."""

    artifact_sha256: str
    dataset_sha256: str


class SourceInfo(BaseModel):
    """Source metadata derived from records."""

    sources: list[str] = Field(default_factory=list)


class DateRange(BaseModel):
    """Dataset date range metadata."""

    min_timestamp: str | None = None
    max_timestamp: str | None = None


class SnapshotManifest(BaseModel):
    """Manifest for immutable dataset snapshot metadata and integrity."""

    dataset_name: str
    snapshot_id: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    input_path: str
    artifact_path: str
    schema_version: str
    row_count: int
    symbols_count: int
    symbols: list[str] = Field(default_factory=list)
    date_range: DateRange = Field(default_factory=DateRange)
    source_info: SourceInfo = Field(default_factory=SourceInfo)
    checksums: SnapshotChecksums


def write_manifest(manifest: SnapshotManifest, path: str | Path) -> Path:
    """Write manifest JSON to disk."""
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    return output_path


def read_manifest(path: str | Path) -> SnapshotManifest:
    """Read and validate manifest JSON from disk."""
    payload = Path(path).read_text(encoding="utf-8")
    return SnapshotManifest.model_validate_json(payload)
