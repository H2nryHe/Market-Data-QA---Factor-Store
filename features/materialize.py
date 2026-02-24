"""Feature materialization from immutable snapshots."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, Field

from features.base import Feature
from features.cache import (
    build_cache_key,
    config_hash,
    is_cache_hit,
    resolve_cache_paths,
)
from features.registry import get_features
from versioning.manifest import read_manifest
from versioning.snapshot import verify_snapshot


class FeatureSettings(BaseModel):
    """Feature set selection and params."""

    enabled: list[str] = Field(
        default_factory=lambda: [
            "ret_1d",
            "ret_5d",
            "vol_20d",
            "mom_20d",
            "dollar_volume",
        ]
    )
    params: dict[str, dict[str, Any]] = Field(default_factory=dict)


class MaterializeConfig(BaseModel):
    """Config for deterministic feature materialization."""

    output_root: str = "data/features"
    feature_set_name: str = "default"
    features: FeatureSettings = Field(default_factory=FeatureSettings)


@dataclass(frozen=True)
class MaterializeResult:
    """Materialization result and cache metadata."""

    cache_hit: bool
    cache_key: str
    artifact_path: Path
    manifest_path: Path


def load_materialize_config(path: str | Path) -> MaterializeConfig:
    """Load materialization config from YAML."""
    try:
        import yaml
    except ModuleNotFoundError as exc:  # pragma: no cover - environment-specific
        raise RuntimeError("PyYAML is required to load feature configs.") from exc

    config_path = Path(path)
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    return MaterializeConfig.model_validate(payload)


def materialize_from_snapshot(
    snapshot_dir: str | Path, config_path: str | Path
) -> MaterializeResult:
    """Materialize features from a verified snapshot artifact only."""
    verify_result = verify_snapshot(snapshot_dir)
    if not verify_result.ok:
        raise RuntimeError(
            f"Input snapshot verification failed: {verify_result.message}"
        )

    cfg = load_materialize_config(config_path)
    snapshot_path = Path(snapshot_dir)
    snapshot_manifest = read_manifest(snapshot_path / "manifest.json")
    artifact_path = snapshot_path / Path(snapshot_manifest.artifact_path).name

    base_rows = _load_and_sort_rows(artifact_path)
    selected_features = get_features(cfg.features.enabled)

    feature_versions = {feature.name: feature.version for feature in selected_features}
    active_params = {
        name: cfg.features.params.get(name, {}) for name in cfg.features.enabled
    }
    feature_cfg_hash = config_hash(
        {
            "feature_set_name": cfg.feature_set_name,
            "enabled": cfg.features.enabled,
            "params": active_params,
        }
    )
    key = build_cache_key(
        snapshot_checksum=snapshot_manifest.checksums.dataset_sha256,
        feature_config_hash=feature_cfg_hash,
        feature_versions=feature_versions,
    )

    cache_paths = resolve_cache_paths(
        cfg.output_root, snapshot_manifest.dataset_name, key
    )
    if is_cache_hit(cache_paths):
        return MaterializeResult(
            cache_hit=True,
            cache_key=key,
            artifact_path=cache_paths.artifact_path,
            manifest_path=cache_paths.manifest_path,
        )

    cache_paths.root.mkdir(parents=True, exist_ok=False)
    enriched_rows = _compute_features(base_rows, selected_features, active_params)
    pq.write_table(
        pa.Table.from_pylist(enriched_rows),
        cache_paths.artifact_path,
        compression="zstd",
    )

    feature_manifest = {
        "feature_set_name": cfg.feature_set_name,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "dataset_name": snapshot_manifest.dataset_name,
        "input_snapshot_id": snapshot_manifest.snapshot_id,
        "input_snapshot_checksum": snapshot_manifest.checksums.dataset_sha256,
        "input_snapshot_artifact": snapshot_manifest.artifact_path,
        "cache_key": key,
        "feature_config_hash": feature_cfg_hash,
        "feature_versions": feature_versions,
        "enabled_features": cfg.features.enabled,
        "row_count": len(enriched_rows),
        "sort_order": ["symbol", "timestamp"],
        "output_artifact": str(cache_paths.artifact_path),
    }
    cache_paths.manifest_path.write_text(
        json.dumps(feature_manifest, indent=2), encoding="utf-8"
    )

    return MaterializeResult(
        cache_hit=False,
        cache_key=key,
        artifact_path=cache_paths.artifact_path,
        manifest_path=cache_paths.manifest_path,
    )


def _load_and_sort_rows(parquet_path: Path) -> list[dict[str, Any]]:
    table = pq.read_table(parquet_path)
    rows = table.to_pylist()
    rows.sort(
        key=lambda row: (str(row.get("symbol", "")), str(row.get("timestamp", "")))
    )
    return rows


def _compute_features(
    rows: list[dict[str, Any]],
    features: list[Feature],
    params_by_feature: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    enriched = [dict(row) for row in rows]
    for feature in features:
        _validate_required_columns(feature, rows)
        values = feature.compute(rows, params=params_by_feature.get(feature.name, {}))
        if len(values) != len(enriched):
            raise ValueError(
                f"Feature '{feature.name}' produced {len(values)} values for {len(enriched)} rows."
            )
        for idx, value in enumerate(values):
            enriched[idx][feature.name] = value
    return enriched


def _validate_required_columns(feature: Feature, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    available = set(rows[0].keys())
    missing = [col for col in feature.required_columns if col not in available]
    if missing:
        raise ValueError(
            f"Feature '{feature.name}' missing required columns: {missing}"
        )
