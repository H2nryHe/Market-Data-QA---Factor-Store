"""Feature interfaces and shared helpers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any


class Feature(ABC):
    """Base interface for deterministic feature computation."""

    name: str
    version: str
    required_columns: tuple[str, ...]

    @abstractmethod
    def compute(
        self, rows: list[dict[str, Any]], params: dict[str, Any]
    ) -> list[float | None]:
        """Compute feature values aligned to input row order."""


def grouped_indices(
    rows: list[dict[str, Any]], key: str = "symbol"
) -> dict[str, list[int]]:
    """Group row indexes by symbol while preserving row order."""
    groups: dict[str, list[int]] = defaultdict(list)
    for idx, row in enumerate(rows):
        groups[str(row.get(key))].append(idx)
    return groups


def as_float(value: Any) -> float | None:
    """Convert numeric-like values to float, else return None."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def rolling_std(values: list[float], ddof: int = 0) -> float:
    """Compute rolling standard deviation for non-empty values."""
    n = len(values)
    if n == 0 or n - ddof <= 0:
        return 0.0
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / (n - ddof)
    return var**0.5
