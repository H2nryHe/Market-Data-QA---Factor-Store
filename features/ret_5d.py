"""5-day return feature."""

from __future__ import annotations

from typing import Any

from features.base import Feature, as_float, grouped_indices


class Return5DFeature(Feature):
    """ret_5d = close_t / close_{t-5} - 1 by default.

    Warmup/null policy:
    - Null for first lookback rows per symbol.
    - Null if current/lagged close is missing.
    - Null if lagged close is zero.
    """

    name = "ret_5d"
    version = "1.0.0"
    required_columns = ("symbol", "close")

    def compute(
        self, rows: list[dict[str, Any]], params: dict[str, Any]
    ) -> list[float | None]:
        lookback = int(params.get("lookback", 5))
        output: list[float | None] = [None] * len(rows)

        for indexes in grouped_indices(rows).values():
            closes = [as_float(rows[idx].get("close")) for idx in indexes]
            for pos, idx in enumerate(indexes):
                if pos < lookback:
                    continue
                curr = closes[pos]
                prev = closes[pos - lookback]
                if curr is None or prev in (None, 0.0):
                    continue
                output[idx] = curr / prev - 1.0
        return output
