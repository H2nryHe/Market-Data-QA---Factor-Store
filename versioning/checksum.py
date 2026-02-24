"""Checksum helpers for snapshot integrity and deterministic dataset hashing."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_SORT_COLUMNS = ["symbol", "timestamp", "source", "ingested_at"]


def sha256_file(path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    """Compute SHA256 checksum for a file."""
    hasher = hashlib.sha256()
    with Path(path).open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def canonical_dataset_sha256(
    table: pa.Table, sort_columns: list[str] | None = None
) -> str:
    """Compute deterministic dataset checksum from a canonical row serialization.

    Stability approach:
    1. Sort rows by explicit columns (`sort_columns`) to remove input-order effects.
    2. Serialize each row as JSON with sorted keys and compact separators.
    3. Hash newline-delimited canonical row strings with SHA256.
    """
    hasher = hashlib.sha256()
    for row in canonical_row_dicts(table, sort_columns=sort_columns):
        payload = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
        hasher.update(payload.encode("utf-8"))
        hasher.update(b"\n")
    return hasher.hexdigest()


def parquet_dataset_sha256(
    path: str | Path, sort_columns: list[str] | None = None
) -> str:
    """Load a parquet file and compute deterministic dataset checksum."""
    table = pq.read_table(path)
    return canonical_dataset_sha256(table, sort_columns=sort_columns)


def canonical_row_dicts(
    table: pa.Table, sort_columns: list[str] | None = None
) -> list[dict[str, Any]]:
    """Return rows sorted by explicit keys for deterministic processing."""
    sort_cols = _resolve_sort_columns(table.column_names, sort_columns)
    rows = table.to_pylist()
    if sort_cols:
        rows.sort(key=lambda row: tuple(_sort_value(row.get(col)) for col in sort_cols))
    return rows


def _resolve_sort_columns(
    column_names: list[str], sort_columns: list[str] | None
) -> list[str]:
    candidates = sort_columns or DEFAULT_SORT_COLUMNS
    return [column for column in candidates if column in column_names]


def _sort_value(value: Any) -> Any:
    if value is None:
        return ""
    return value
