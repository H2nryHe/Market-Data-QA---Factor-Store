from pathlib import Path

import pytest


def test_load_sample_ohlcv() -> None:

    polars = pytest.importorskip("polars")
    from features.sample_loader import load_sample_ohlcv

    sample_path = Path("data/sample/ohlcv_sample.csv")
    df = load_sample_ohlcv(sample_path)

    assert isinstance(df, polars.DataFrame)
    assert df.height > 0
    assert {"date", "symbol", "open", "high", "low", "close", "volume"}.issubset(
        df.columns
    )
