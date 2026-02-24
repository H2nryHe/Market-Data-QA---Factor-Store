"""Snapshot creation and verification workflows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from versioning.checksum import parquet_dataset_sha256, sha256_file
from versioning.manifest import (
    DateRange,
    SnapshotChecksums,
    SnapshotManifest,
    SourceInfo,
    read_manifest,
    write_manifest,
)


@dataclass(frozen=True)
class VerifyResult:
    """Outcome of snapshot verification."""

    ok: bool
    message: str


def create_snapshot(
    *,
    input_path: str | Path,
    dataset_name: str,
    snapshots_root: str | Path = "data/snapshots",
    schema_version: str = "1.0.0",
) -> Path:
    """Create immutable snapshot directory with parquet artifact and manifest."""
    snapshot_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    snapshot_dir = Path(snapshots_root) / dataset_name / snapshot_id
    if snapshot_dir.exists():
        raise FileExistsError(f"Snapshot directory already exists: {snapshot_dir}")
    snapshot_dir.mkdir(parents=True, exist_ok=False)

    table = _load_csv_table(input_path)
    artifact_path = snapshot_dir / "data.parquet"
    pq.write_table(table, artifact_path, compression="zstd")

    manifest = _build_manifest(
        table=table,
        input_path=Path(input_path),
        dataset_name=dataset_name,
        snapshot_id=snapshot_id,
        artifact_path=artifact_path,
        schema_version=schema_version,
    )
    write_manifest(manifest, snapshot_dir / "manifest.json")
    return snapshot_dir


def verify_snapshot(snapshot_dir: str | Path) -> VerifyResult:
    """Verify snapshot checksums against manifest."""
    snapshot_path = Path(snapshot_dir)
    manifest_path = snapshot_path / "manifest.json"
    if not manifest_path.exists():
        return VerifyResult(ok=False, message=f"Manifest not found: {manifest_path}")

    manifest = read_manifest(manifest_path)
    artifact_path = snapshot_path / Path(manifest.artifact_path).name
    if not artifact_path.exists():
        return VerifyResult(ok=False, message=f"Artifact not found: {artifact_path}")

    actual_artifact_checksum = sha256_file(artifact_path)
    if actual_artifact_checksum != manifest.checksums.artifact_sha256:
        return VerifyResult(
            ok=False,
            message=(
                "Artifact checksum mismatch: "
                f"expected={manifest.checksums.artifact_sha256} "
                f"actual={actual_artifact_checksum}"
            ),
        )

    actual_dataset_checksum = parquet_dataset_sha256(artifact_path)
    if actual_dataset_checksum != manifest.checksums.dataset_sha256:
        return VerifyResult(
            ok=False,
            message=(
                "Dataset checksum mismatch: "
                f"expected={manifest.checksums.dataset_sha256} "
                f"actual={actual_dataset_checksum}"
            ),
        )

    return VerifyResult(
        ok=True, message=f"Snapshot verification passed: {snapshot_path}"
    )


def _load_csv_table(path: str | Path) -> pa.Table:
    return pacsv.read_csv(path)


def _build_manifest(
    *,
    table: pa.Table,
    input_path: Path,
    dataset_name: str,
    snapshot_id: str,
    artifact_path: Path,
    schema_version: str,
) -> SnapshotManifest:
    symbols = _unique_values(table, "symbol")
    sources = _unique_values(table, "source")
    date_range = _timestamp_range(table)
    checksums = SnapshotChecksums(
        artifact_sha256=sha256_file(artifact_path),
        dataset_sha256=parquet_dataset_sha256(artifact_path),
    )

    return SnapshotManifest(
        dataset_name=dataset_name,
        snapshot_id=snapshot_id,
        input_path=str(input_path),
        artifact_path=str(artifact_path),
        schema_version=schema_version,
        row_count=table.num_rows,
        symbols_count=len(symbols),
        symbols=symbols,
        date_range=date_range,
        source_info=SourceInfo(sources=sources),
        checksums=checksums,
    )


def _unique_values(table: pa.Table, column: str) -> list[str]:
    if column not in table.column_names:
        return []
    uniques = pc.unique(table[column]).to_pylist()
    values = [str(v) for v in uniques if v is not None]
    values.sort()
    return values


def _timestamp_range(table: pa.Table) -> DateRange:
    if "timestamp" not in table.column_names:
        return DateRange(min_timestamp=None, max_timestamp=None)

    values = [v for v in table["timestamp"].to_pylist() if v is not None]
    if not values:
        return DateRange(min_timestamp=None, max_timestamp=None)
    min_ts = min(str(v) for v in values)
    max_ts = max(str(v) for v in values)
    return DateRange(min_timestamp=min_ts, max_timestamp=max_ts)
