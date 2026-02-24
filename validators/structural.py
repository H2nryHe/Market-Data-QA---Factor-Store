"""Structural validator wrapping schema-level structural and business checks."""

from __future__ import annotations

from typing import Any

from schemas.market_ohlcv import (
    format_issue,
    validate_business_rules,
    validate_structural,
)
from validators.base import ValidationContext, ValidationStatus, ValidatorResult


class StructuralValidator:
    """Runs schema structural checks and schema business rules."""

    name = "structural"

    def validate(
        self, records: list[dict[str, Any]], context: ValidationContext
    ) -> ValidatorResult:
        issues = validate_structural(records, context.schema_config)
        issues.extend(validate_business_rules(records, context.schema_config))

        if issues:
            sample_rows = [
                {"issue": format_issue(issue)}
                for issue in issues[: context.validation_config.report_sample_limit]
            ]
            return ValidatorResult(
                validator=self.name,
                status=ValidationStatus.FAIL,
                severity=context.validation_config.validators.structural.severity,
                affected_row_count=len(
                    {issue.row_index for issue in issues if issue.row_index is not None}
                ),
                rule="schema.structural+business",
                message=f"{len(issues)} schema issue(s) detected.",
                sample_rows=sample_rows,
            )

        return ValidatorResult(
            validator=self.name,
            status=ValidationStatus.PASS,
            severity=context.validation_config.validators.structural.severity,
            affected_row_count=0,
            rule="schema.structural+business",
            message="Structural schema and business-rule checks passed.",
            sample_rows=[],
        )
