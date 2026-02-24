"""Duplicate key validator."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from validators.base import ValidationContext, ValidationStatus, ValidatorResult


class DuplicatesValidator:
    """Detect duplicate rows by configured uniqueness key."""

    name = "duplicates"

    def validate(
        self, records: list[dict[str, Any]], context: ValidationContext
    ) -> ValidatorResult:
        key_columns = context.validation_config.validators.duplicates.key_columns
        index_by_key: dict[tuple[Any, ...], list[int]] = defaultdict(list)

        for row_index, row in enumerate(records):
            key = tuple(row.get(col) for col in key_columns)
            index_by_key[key].append(row_index)

        duplicate_row_indexes: list[int] = []
        duplicate_examples: list[dict[str, Any]] = []
        for key, indexes in index_by_key.items():
            if len(indexes) > 1:
                duplicate_row_indexes.extend(indexes)
                duplicate_examples.append(
                    {"key": dict(zip(key_columns, key, strict=False)), "rows": indexes}
                )

        if duplicate_row_indexes:
            return ValidatorResult(
                validator=self.name,
                status=ValidationStatus.FAIL,
                severity=context.validation_config.validators.duplicates.severity,
                affected_row_count=len(set(duplicate_row_indexes)),
                rule=f"duplicates.key={key_columns}",
                message="Duplicate uniqueness-key rows detected.",
                sample_rows=duplicate_examples[
                    : context.validation_config.report_sample_limit
                ],
            )

        return ValidatorResult(
            validator=self.name,
            status=ValidationStatus.PASS,
            severity=context.validation_config.validators.duplicates.severity,
            affected_row_count=0,
            rule=f"duplicates.key={key_columns}",
            message="No duplicate uniqueness-key rows found.",
            sample_rows=[],
        )
