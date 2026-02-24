"""Feature cache keying and cache-path helpers."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CachePaths:
    """Resolved output paths for a cache key."""

    root: Path
    artifact_path: Path
    manifest_path: Path


def config_hash(config_payload: dict) -> str:
    """Hash feature config payload deterministically."""
    blob = json.dumps(config_payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def build_cache_key(
    *,
    snapshot_checksum: str,
    feature_config_hash: str,
    feature_versions: dict[str, str],
) -> str:
    """Build cache key from snapshot checksum, config hash, and feature versions."""
    payload = {
        "snapshot_checksum": snapshot_checksum,
        "feature_config_hash": feature_config_hash,
        "feature_versions": feature_versions,
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def resolve_cache_paths(
    cache_root: str | Path, dataset_name: str, key: str
) -> CachePaths:
    """Resolve deterministic artifact/manifest paths for a cache key."""
    root = Path(cache_root) / dataset_name / key
    return CachePaths(
        root=root,
        artifact_path=root / "features.parquet",
        manifest_path=root / "feature_manifest.json",
    )


def is_cache_hit(paths: CachePaths) -> bool:
    """Cache hit requires both artifact and manifest."""
    return paths.artifact_path.exists() and paths.manifest_path.exists()
