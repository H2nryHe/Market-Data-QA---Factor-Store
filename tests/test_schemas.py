from __future__ import annotations

from datetime import datetime
from pathlib import Path

from schemas.market_ohlcv import SchemaValidationError, validate_market_ohlcv
from schemas.rules import load_schema_config


def _valid_records() -> list[dict[str, object]]:
    return [
        {
            "symbol": "AAPL",
            "timestamp": datetime(2026, 1, 2, 0, 0, 0),
            "open": 191.1,
            "high": 193.5,
            "low": 190.8,
            "close": 193.0,
            "volume": 45123000,
            "source": "sample_vendor",
            "ingested_at": datetime(2026, 1, 2, 8, 0, 0),
        }
    ]


def test_valid_fixture_passes() -> None:
    config = load_schema_config(Path("configs/schemas.yaml"))

    validate_market_ohlcv(records=_valid_records(), config=config)


def test_missing_required_column_fails() -> None:
    config = load_schema_config(Path("configs/schemas.yaml"))
    records = _valid_records()
    del records[0]["source"]

    try:
        validate_market_ohlcv(records=records, config=config)
        raise AssertionError(
            "Expected schema validation to fail for missing required column"
        )
    except SchemaValidationError as exc:
        message = str(exc)
        assert "check=required_columns" in message
        assert "field=source" in message


def test_wrong_dtype_fails() -> None:
    config = load_schema_config(Path("configs/schemas.yaml"))
    records = _valid_records()
    records[0]["volume"] = "45123000"

    try:
        validate_market_ohlcv(records=records, config=config)
        raise AssertionError("Expected schema validation to fail for wrong dtype")
    except SchemaValidationError as exc:
        message = str(exc)
        assert "check=dtype" in message
        assert "field=volume" in message


def test_rule_violation_fails_for_low_gt_high() -> None:
    config = load_schema_config(Path("configs/schemas.yaml"))
    records = _valid_records()
    records[0]["low"] = 200.0
    records[0]["high"] = 193.5

    try:
        validate_market_ohlcv(records=records, config=config)
        raise AssertionError("Expected schema validation to fail for low > high")
    except SchemaValidationError as exc:
        message = str(exc)
        assert "check=ohlc_consistency" in message
        assert "Rule 'low<=high' violated" in message
