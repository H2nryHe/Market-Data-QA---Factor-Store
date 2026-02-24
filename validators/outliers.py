"""Outlier validator using modified z-score (MAD) by group."""

from __future__ import annotations

from collections import defaultdict
from statistics import median
from typing import Any

from validators.base import ValidationContext, ValidationStatus, ValidatorResult


class OutlierValidator:
    """Detect robust outliers using median absolute deviation."""

    name = "outliers"

    def validate(
        self, records: list[dict[str, Any]], context: ValidationContext
    ) -> ValidatorResult:
        settings = context.validation_config.validators.outliers
        if settings.method.lower() != "mad":
            return ValidatorResult(
                validator=self.name,
                status=ValidationStatus.WARN,
                severity=settings.severity,
                affected_row_count=0,
                rule=f"outliers.method={settings.method}",
                message="Unsupported outlier method configured; validator skipped.",
                sample_rows=[],
            )

        grouped: dict[tuple[Any, ...], list[tuple[int, dict[str, Any]]]] = defaultdict(
            list
        )
        for idx, row in enumerate(records):
            grouped[tuple(row.get(col) for col in settings.group_by)].append((idx, row))

        outlier_indexes: set[int] = set()
        samples: list[dict[str, Any]] = []

        for group_key, items in grouped.items():
            if len(items) < settings.min_group_size:
                continue

            for field in settings.fields:
                values: list[tuple[int, float]] = []
                for row_idx, row in items:
                    value = row.get(field)
                    if isinstance(value, (int, float)) and not isinstance(value, bool):
                        values.append((row_idx, float(value)))
                if len(values) < settings.min_group_size:
                    continue

                raw_values = [v for _, v in values]
                center = median(raw_values)
                abs_dev = [abs(v - center) for v in raw_values]
                mad = median(abs_dev)
                if mad == 0:
                    continue

                for row_idx, value in values:
                    modified_z = 0.6745 * (value - center) / mad
                    if abs(modified_z) > settings.z_threshold:
                        outlier_indexes.add(row_idx)
                        samples.append(
                            {
                                "group": dict(
                                    zip(settings.group_by, group_key, strict=False)
                                ),
                                "row": row_idx,
                                "field": field,
                                "value": value,
                                "median": center,
                                "mad": mad,
                                "modified_z": round(modified_z, 4),
                                "threshold": settings.z_threshold,
                            }
                        )

        if outlier_indexes:
            return ValidatorResult(
                validator=self.name,
                status=ValidationStatus.FAIL,
                severity=settings.severity,
                affected_row_count=len(outlier_indexes),
                rule=(
                    f"outliers.method=mad, z_threshold={settings.z_threshold}, "
                    f"min_group_size={settings.min_group_size}, fields={settings.fields}"
                ),
                message="Outlier rows detected.",
                sample_rows=samples[: context.validation_config.report_sample_limit],
            )

        return ValidatorResult(
            validator=self.name,
            status=ValidationStatus.PASS,
            severity=settings.severity,
            affected_row_count=0,
            rule=(
                f"outliers.method=mad, z_threshold={settings.z_threshold}, "
                f"min_group_size={settings.min_group_size}, fields={settings.fields}"
            ),
            message="No outliers detected by configured MAD policy.",
            sample_rows=[],
        )
