"""20-day rolling volatility feature."""

from __future__ import annotations

from typing import Any

from features.base import Feature, as_float, grouped_indices, rolling_std


class Volatility20DFeature(Feature):
    """vol_20d: rolling std of 1-day returns over lookback window (default 20).

    Warmup/null policy:
    - Null until enough valid 1-day returns are available.
    - Null if any return in the window is missing.
    - Uses population std (`ddof=0`) for deterministic consistency.
    """

    name = "vol_20d"
    version = "1.0.0"
    required_columns = ("symbol", "close")

    def compute(
        self, rows: list[dict[str, Any]], params: dict[str, Any]
    ) -> list[float | None]:
        lookback = int(params.get("lookback", 20))
        output: list[float | None] = [None] * len(rows)

        for indexes in grouped_indices(rows).values():
            closes = [as_float(rows[idx].get("close")) for idx in indexes]
            ret_1d: list[float | None] = [None] * len(indexes)

            for pos in range(1, len(indexes)):
                curr = closes[pos]
                prev = closes[pos - 1]
                if curr is None or prev in (None, 0.0):
                    continue
                ret_1d[pos] = curr / prev - 1.0

            for pos, idx in enumerate(indexes):
                window_start = pos - lookback + 1
                if window_start < 1:
                    continue
                window = ret_1d[window_start : pos + 1]
                if any(v is None for v in window):
                    continue
                values = [float(v) for v in window if v is not None]
                output[idx] = rolling_std(values, ddof=0)

        return output
