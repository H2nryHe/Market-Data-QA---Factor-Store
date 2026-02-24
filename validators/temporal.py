"""Temporal validator for ordering and gap checks."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any

from validators.base import ValidationContext, ValidationStatus, ValidatorResult


class TemporalValidator:
    """Checks reverse timestamps and excessive gaps per configured grouping."""

    name = "temporal"

    def validate(
        self, records: list[dict[str, Any]], context: ValidationContext
    ) -> ValidatorResult:
        group_by = context.validation_config.validators.temporal.group_by
        max_gap_days = context.validation_config.validators.temporal.max_gap_days
        groups: dict[tuple[Any, ...], list[tuple[int, datetime]]] = defaultdict(list)

        for row_index, row in enumerate(records):
            ts = row.get("timestamp")
            if isinstance(ts, datetime):
                group_key = tuple(row.get(field) for field in group_by)
                groups[group_key].append((row_index, ts))

        reverse_rows: set[int] = set()
        gap_rows: set[int] = set()
        samples: list[dict[str, Any]] = []

        for group_key, ordered in groups.items():
            previous: tuple[int, datetime] | None = None
            for row_index, current_ts in ordered:
                if previous is not None:
                    prev_index, prev_ts = previous
                    if current_ts < prev_ts:
                        reverse_rows.update([prev_index, row_index])
                        samples.append(
                            {
                                "group": dict(zip(group_by, group_key, strict=False)),
                                "previous_row": prev_index,
                                "current_row": row_index,
                                "previous_timestamp": prev_ts.isoformat(),
                                "current_timestamp": current_ts.isoformat(),
                                "check": "reverse_timestamp",
                            }
                        )
                    gap_days = (current_ts - prev_ts).days
                    if gap_days > max_gap_days:
                        gap_rows.update([prev_index, row_index])
                        samples.append(
                            {
                                "group": dict(zip(group_by, group_key, strict=False)),
                                "previous_row": prev_index,
                                "current_row": row_index,
                                "gap_days": gap_days,
                                "threshold_days": max_gap_days,
                                "check": "timestamp_gap",
                            }
                        )
                previous = (row_index, current_ts)

        affected = reverse_rows.union(gap_rows)
        if affected:
            return ValidatorResult(
                validator=self.name,
                status=ValidationStatus.FAIL,
                severity=context.validation_config.validators.temporal.severity,
                affected_row_count=len(affected),
                rule=f"temporal.group_by={group_by}, max_gap_days={max_gap_days}",
                message="Temporal ordering/gap violations detected.",
                sample_rows=samples[: context.validation_config.report_sample_limit],
            )

        return ValidatorResult(
            validator=self.name,
            status=ValidationStatus.PASS,
            severity=context.validation_config.validators.temporal.severity,
            affected_row_count=0,
            rule=f"temporal.group_by={group_by}, max_gap_days={max_gap_days}",
            message="Temporal checks passed.",
            sample_rows=[],
        )
