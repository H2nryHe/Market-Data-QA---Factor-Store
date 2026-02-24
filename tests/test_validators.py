from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta
from pathlib import Path

from typer.testing import CliRunner

from validators.base import ValidationStatus, build_context
from validators.cli import app
from validators.orchestrator import run_validators

runner = CliRunner()


def _base_records() -> list[dict[str, object]]:
    start = datetime(2026, 1, 2)
    return [
        {
            "symbol": "AAPL",
            "timestamp": start + timedelta(days=i),
            "open": 100.0 + i,
            "high": 101.0 + i,
            "low": 99.0 + i,
            "close": 100.5 + i,
            "volume": 1000 + i * 10,
            "source": "test_vendor",
            "ingested_at": start + timedelta(days=i, hours=8),
        }
        for i in range(6)
    ]


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        "symbol",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source",
        "ingested_at",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    **row,
                    "timestamp": row["timestamp"].isoformat(),
                    "ingested_at": row["ingested_at"].isoformat(),
                }
            )


def test_clean_sample_passes_or_warns() -> None:
    context = build_context("configs/validation.yaml")

    report = run_validators(
        records=_base_records(), context=context, dataset_path="in-memory"
    )

    assert report.overall_status in {ValidationStatus.PASS, ValidationStatus.WARN}
    assert all(result.status != ValidationStatus.FAIL for result in report.results)


def test_duplicates_validator_fails() -> None:
    context = build_context("configs/validation.yaml")
    records = _base_records()
    records.append(records[0].copy())

    report = run_validators(records=records, context=context, dataset_path="in-memory")

    duplicates_result = next(
        result for result in report.results if result.validator == "duplicates"
    )
    assert duplicates_result.status == ValidationStatus.FAIL
    assert duplicates_result.affected_row_count >= 2


def test_temporal_validator_fails_for_reverse_timestamp() -> None:
    context = build_context("configs/validation.yaml")
    records = _base_records()
    records[3]["timestamp"] = records[1]["timestamp"] - timedelta(days=1)

    report = run_validators(records=records, context=context, dataset_path="in-memory")

    temporal_result = next(
        result for result in report.results if result.validator == "temporal"
    )
    assert temporal_result.status == ValidationStatus.FAIL
    assert "Temporal ordering/gap violations detected" in temporal_result.message


def test_outlier_validator_fails() -> None:
    context = build_context("configs/validation.yaml")
    records = _base_records()
    records[5]["close"] = 10000.0

    report = run_validators(records=records, context=context, dataset_path="in-memory")

    outlier_result = next(
        result for result in report.results if result.validator == "outliers"
    )
    assert outlier_result.status == ValidationStatus.FAIL
    assert outlier_result.affected_row_count >= 1


def test_cli_non_zero_exit_on_fail_and_writes_report(tmp_path: Path) -> None:
    rows = _base_records()
    rows.append(rows[0].copy())
    input_path = tmp_path / "dupes.csv"
    report_path = tmp_path / "qa_report.json"
    _write_csv(input_path, rows)

    result = runner.invoke(
        app,
        [
            "run",
            "--input",
            str(input_path),
            "--config",
            "configs/validation.yaml",
            "--report",
            str(report_path),
        ],
    )

    assert result.exit_code == 1
    assert report_path.exists()

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["overall_status"] == "FAIL"
