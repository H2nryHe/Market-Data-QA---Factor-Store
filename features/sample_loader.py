"""Utilities for loading sample OHLCV data."""

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass

REQUIRED_COLUMNS = {"date", "symbol", "open", "high", "low", "close", "volume"}


def load_sample_ohlcv(path: str | Path) -> Any:
    """Load OHLCV sample data and enforce expected columns."""
    try:
        import polars as pl
    except ModuleNotFoundError as exc:  # pragma: no cover - environment-specific
        raise RuntimeError("Polars is required to load sample OHLCV data.") from exc

    frame = pl.read_csv(path, try_parse_dates=True)
    missing = REQUIRED_COLUMNS.difference(frame.columns)
    if missing:
        missing_cols = ", ".join(sorted(missing))
        raise ValueError(f"Missing required columns: {missing_cols}")
    return frame
