from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from versioning.cli import app
from versioning.manifest import read_manifest

runner = CliRunner()


def test_snapshot_create_and_verify_happy_path(tmp_path: Path) -> None:
    output_root = tmp_path / "snapshots"
    result_snapshot = runner.invoke(
        app,
        [
            "snapshot",
            "--input",
            "data/sample/market_ohlcv_sample.csv",
            "--dataset",
            "market_ohlcv",
            "--output-root",
            str(output_root),
            "--schema-version",
            "1.0.0",
        ],
    )
    assert result_snapshot.exit_code == 0

    dataset_root = output_root / "market_ohlcv"
    snapshot_dirs = [p for p in dataset_root.iterdir() if p.is_dir()]
    assert len(snapshot_dirs) == 1

    result_verify = runner.invoke(
        app,
        ["verify", "--snapshot-dir", str(snapshot_dirs[0])],
    )
    assert result_verify.exit_code == 0
    assert "verification passed" in result_verify.output.lower()


def test_tamper_snapshot_fails_verify(tmp_path: Path) -> None:
    output_root = tmp_path / "snapshots"
    result_snapshot = runner.invoke(
        app,
        [
            "snapshot",
            "--input",
            "data/sample/market_ohlcv_sample.csv",
            "--dataset",
            "market_ohlcv",
            "--output-root",
            str(output_root),
        ],
    )
    assert result_snapshot.exit_code == 0

    snapshot_dir = next((output_root / "market_ohlcv").iterdir())
    artifact_path = snapshot_dir / "data.parquet"
    with artifact_path.open("ab") as f:
        f.write(b"tamper")

    result_verify = runner.invoke(app, ["verify", "--snapshot-dir", str(snapshot_dir)])
    assert result_verify.exit_code == 1
    assert "checksum mismatch" in result_verify.output.lower()


def test_manifest_contains_required_fields(tmp_path: Path) -> None:
    output_root = tmp_path / "snapshots"
    result_snapshot = runner.invoke(
        app,
        [
            "snapshot",
            "--input",
            "data/sample/market_ohlcv_sample.csv",
            "--dataset",
            "market_ohlcv",
            "--output-root",
            str(output_root),
            "--schema-version",
            "2026.1",
        ],
    )
    assert result_snapshot.exit_code == 0

    snapshot_dir = next((output_root / "market_ohlcv").iterdir())
    manifest = read_manifest(snapshot_dir / "manifest.json")

    assert manifest.dataset_name == "market_ohlcv"
    assert manifest.input_path.endswith("data/sample/market_ohlcv_sample.csv")
    assert manifest.artifact_path.endswith("data.parquet")
    assert manifest.row_count > 0
    assert manifest.symbols_count >= 1
    assert len(manifest.symbols) == manifest.symbols_count
    assert manifest.date_range.min_timestamp is not None
    assert manifest.date_range.max_timestamp is not None
    assert manifest.schema_version == "2026.1"
    assert len(manifest.checksums.artifact_sha256) == 64
    assert len(manifest.checksums.dataset_sha256) == 64

    payload = json.loads((snapshot_dir / "manifest.json").read_text(encoding="utf-8"))
    required_keys = {
        "dataset_name",
        "created_at",
        "source_info",
        "row_count",
        "symbols_count",
        "symbols",
        "date_range",
        "schema_version",
        "checksums",
        "input_path",
    }
    assert required_keys.issubset(payload.keys())
