"""Canonical schema and business-rule validation for daily market OHLCV records."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from numbers import Real
from typing import Any

from schemas.rules import MarketOHLCVSchemaConfig

CANONICAL_REQUIRED_COLUMNS = [
    "symbol",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
    "ingested_at",
]


@dataclass(frozen=True)
class ValidationIssue:
    """A single schema validation issue with row/field context."""

    check: str
    message: str
    field: str | None = None
    row_index: int | None = None


class SchemaValidationError(ValueError):
    """Raised when structural or business-rule checks fail."""

    def __init__(self, issues: Sequence[ValidationIssue]) -> None:
        self.issues = list(issues)
        summary = "\n".join(format_issue(issue) for issue in self.issues)
        super().__init__(summary)


def format_issue(issue: ValidationIssue) -> str:
    """Format a single validation issue into a human-readable string."""
    parts = [f"check={issue.check}"]
    if issue.field is not None:
        parts.append(f"field={issue.field}")
    if issue.row_index is not None:
        parts.append(f"row={issue.row_index}")
    parts.append(f"message={issue.message}")
    return " | ".join(parts)


def validate_structural(
    records: Sequence[Mapping[str, Any]], config: MarketOHLCVSchemaConfig
) -> list[ValidationIssue]:
    """Run structural checks: columns, nullability, and dtypes."""
    issues: list[ValidationIssue] = []

    observed_columns = set()
    for row in records:
        observed_columns.update(row.keys())

    required = set(config.required_columns)
    missing = sorted(required.difference(observed_columns))
    for column in missing:
        issues.append(
            ValidationIssue(
                check="required_columns",
                field=column,
                message=f"Missing required column '{column}'.",
            )
        )

    if not config.validation_policy.allow_extra_columns:
        extras = sorted(observed_columns.difference(config.columns.keys()))
        for column in extras:
            issues.append(
                ValidationIssue(
                    check="extra_columns",
                    field=column,
                    message=f"Unexpected column '{column}' is not defined in schema.",
                )
            )

    for row_index, row in enumerate(records):
        for column_name, column_rule in config.columns.items():
            value = row.get(column_name)
            if value is None:
                if not column_rule.nullable:
                    issues.append(
                        ValidationIssue(
                            check="nullability",
                            field=column_name,
                            row_index=row_index,
                            message=f"Column '{column_name}' is non-nullable but value is null.",
                        )
                    )
                continue

            if config.validation_policy.strict_types and not _matches_dtype(
                value, column_rule.dtype
            ):
                issues.append(
                    ValidationIssue(
                        check="dtype",
                        field=column_name,
                        row_index=row_index,
                        message=(
                            f"Column '{column_name}' expected dtype '{column_rule.dtype}' "
                            f"but got '{type(value).__name__}'."
                        ),
                    )
                )

    return issues


def validate_business_rules(
    records: Sequence[Mapping[str, Any]], config: MarketOHLCVSchemaConfig
) -> list[ValidationIssue]:
    """Run business-rule checks: OHLC logical consistency and non-negative volume."""
    issues: list[ValidationIssue] = []

    if config.rules.enforce_ohlc_consistency:
        for row_index, row in enumerate(records):
            low = row.get("low")
            high = row.get("high")
            open_ = row.get("open")
            close = row.get("close")

            if _all_real(low, high):
                if low > high:
                    issues.append(
                        ValidationIssue(
                            check="ohlc_consistency",
                            field="low",
                            row_index=row_index,
                            message="Rule 'low<=high' violated: low is greater than high.",
                        )
                    )

            if _all_real(low, high, open_) and not (low <= open_ <= high):
                issues.append(
                    ValidationIssue(
                        check="ohlc_consistency",
                        field="open",
                        row_index=row_index,
                        message="Rule 'low<=open<=high' violated for open.",
                    )
                )

            if _all_real(low, high, close) and not (low <= close <= high):
                issues.append(
                    ValidationIssue(
                        check="ohlc_consistency",
                        field="close",
                        row_index=row_index,
                        message="Rule 'low<=close<=high' violated for close.",
                    )
                )

    if config.rules.enforce_non_negative_volume:
        for row_index, row in enumerate(records):
            volume = row.get("volume")
            if isinstance(volume, bool):
                continue
            if isinstance(volume, Real) and volume < 0:
                issues.append(
                    ValidationIssue(
                        check="non_negative_volume",
                        field="volume",
                        row_index=row_index,
                        message="Rule 'volume>=0' violated: volume is negative.",
                    )
                )

    return issues


def validate_market_ohlcv(
    records: Sequence[Mapping[str, Any]], config: MarketOHLCVSchemaConfig
) -> None:
    """Validate records and raise SchemaValidationError when violations are found."""
    structural_issues = validate_structural(records=records, config=config)
    business_issues = validate_business_rules(records=records, config=config)
    all_issues = structural_issues + business_issues
    if all_issues:
        raise SchemaValidationError(all_issues)


def _matches_dtype(value: Any, expected_dtype: str) -> bool:
    if expected_dtype == "str":
        return isinstance(value, str)
    if expected_dtype == "float":
        return isinstance(value, Real) and not isinstance(value, bool)
    if expected_dtype == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected_dtype == "datetime":
        return isinstance(value, datetime)
    return False


def _all_real(*values: Any) -> bool:
    return all(isinstance(v, Real) and not isinstance(v, bool) for v in values)
