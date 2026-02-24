"""Dollar-volume feature."""

from __future__ import annotations

from typing import Any

from features.base import Feature, as_float


class DollarVolumeFeature(Feature):
    """dollar_volume = close * volume.

    Warmup/null policy:
    - No warmup window.
    - Null if close or volume is missing/non-numeric.
    """

    name = "dollar_volume"
    version = "1.0.0"
    required_columns = ("close", "volume")

    def compute(
        self, rows: list[dict[str, Any]], params: dict[str, Any]
    ) -> list[float | None]:
        output: list[float | None] = [None] * len(rows)
        for idx, row in enumerate(rows):
            close = as_float(row.get("close"))
            volume = as_float(row.get("volume"))
            if close is None or volume is None:
                continue
            output[idx] = close * volume
        return output
