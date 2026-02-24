from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from features.materialize import materialize_from_snapshot
from versioning.snapshot import create_snapshot


def _build_input_csv(path: Path, days: int = 25) -> None:
    start = datetime(2026, 1, 1)
    rows = [
        "symbol,timestamp,open,high,low,close,volume,source,ingested_at",
    ]
    for i in range(days):
        close = 100.0 * (1.01**i)
        ts = (start + timedelta(days=i)).isoformat()
        ingested = (start + timedelta(days=i, hours=8)).isoformat()
        row = (
            f"AAPL,{ts},{close:.8f},{close * 1.01:.8f},{close * 0.99:.8f},"
            f"{close:.8f},1000,test_vendor,{ingested}"
        )
        rows.append(row)
    path.write_text("\n".join(rows), encoding="utf-8")


def _snapshot_from_csv(tmp_path: Path) -> Path:
    input_path = tmp_path / "input.csv"
    _build_input_csv(input_path)
    snapshot_dir = create_snapshot(
        input_path=input_path,
        dataset_name="market_ohlcv",
        snapshots_root=tmp_path / "snapshots",
        schema_version="1.0.0",
    )
    return snapshot_dir


def _write_config(path: Path, output_root: Path, ret5_lookback: int = 5) -> None:
    payload = {
        "output_root": str(output_root),
        "feature_set_name": "test_set",
        "features": {
            "enabled": ["ret_1d", "ret_5d", "vol_20d", "mom_20d", "dollar_volume"],
            "params": {
                "ret_1d": {"lookback": 1},
                "ret_5d": {"lookback": ret5_lookback},
                "vol_20d": {"lookback": 20},
                "mom_20d": {"lookback": 20},
            },
        },
    }
    try:
        import yaml
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("PyYAML is required for tests.") from exc
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def test_feature_correctness_known_values(tmp_path: Path) -> None:
    snapshot_dir = _snapshot_from_csv(tmp_path)
    config_path = tmp_path / "features.yaml"
    output_root = tmp_path / "features"
    _write_config(config_path, output_root, ret5_lookback=5)

    result = materialize_from_snapshot(
        snapshot_dir=snapshot_dir, config_path=config_path
    )
    assert result.cache_hit is False

    table = pq.read_table(result.artifact_path)
    rows = table.to_pylist()
    rows.sort(key=lambda r: (r["symbol"], r["timestamp"]))

    assert rows[0]["ret_1d"] is None
    assert abs(rows[1]["ret_1d"] - 0.01) < 1e-9
    assert abs(rows[5]["ret_5d"] - ((1.01**5) - 1.0)) < 1e-9
    assert abs(rows[20]["mom_20d"] - ((1.01**20) - 1.0)) < 1e-9
    assert rows[20]["vol_20d"] is not None
    assert abs(rows[20]["vol_20d"]) < 1e-9
    assert abs(rows[0]["dollar_volume"] - rows[0]["close"] * rows[0]["volume"]) < 1e-9


def test_cache_hit_and_miss_behavior(tmp_path: Path) -> None:
    snapshot_dir = _snapshot_from_csv(tmp_path)
    config_path = tmp_path / "features.yaml"
    output_root = tmp_path / "features"
    _write_config(config_path, output_root, ret5_lookback=5)

    first = materialize_from_snapshot(
        snapshot_dir=snapshot_dir, config_path=config_path
    )
    second = materialize_from_snapshot(
        snapshot_dir=snapshot_dir, config_path=config_path
    )

    assert first.cache_hit is False
    assert second.cache_hit is True
    assert first.cache_key == second.cache_key
    assert first.artifact_path == second.artifact_path


def test_config_change_invalidates_cache(tmp_path: Path) -> None:
    snapshot_dir = _snapshot_from_csv(tmp_path)
    output_root = tmp_path / "features"

    config_a = tmp_path / "features_a.yaml"
    config_b = tmp_path / "features_b.yaml"
    _write_config(config_a, output_root, ret5_lookback=5)
    _write_config(config_b, output_root, ret5_lookback=3)

    result_a = materialize_from_snapshot(
        snapshot_dir=snapshot_dir, config_path=config_a
    )
    result_b = materialize_from_snapshot(
        snapshot_dir=snapshot_dir, config_path=config_b
    )

    assert result_a.cache_key != result_b.cache_key
    assert result_a.artifact_path != result_b.artifact_path
    assert result_b.cache_hit is False

    manifest_a = json.loads(result_a.manifest_path.read_text(encoding="utf-8"))
    manifest_b = json.loads(result_b.manifest_path.read_text(encoding="utf-8"))
    assert manifest_a["feature_config_hash"] != manifest_b["feature_config_hash"]
