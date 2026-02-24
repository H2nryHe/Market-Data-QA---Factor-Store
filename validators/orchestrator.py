"""Validator registry and orchestration helpers."""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Any

from validators.base import ValidationContext, ValidationStatus, Validator
from validators.duplicates import DuplicatesValidator
from validators.outliers import OutlierValidator
from validators.report import ValidationReport
from validators.structural import StructuralValidator
from validators.temporal import TemporalValidator


def build_registry(context: ValidationContext) -> list[Validator]:
    """Build ordered validator registry based on config-enabled validators."""
    registry: list[Validator] = []
    settings = context.validation_config.validators
    if settings.structural.enabled:
        registry.append(StructuralValidator())
    if settings.duplicates.enabled:
        registry.append(DuplicatesValidator())
    if settings.temporal.enabled:
        registry.append(TemporalValidator())
    if settings.outliers.enabled:
        registry.append(OutlierValidator())
    return registry


def run_validators(
    records: list[dict[str, Any]],
    context: ValidationContext,
    dataset_path: str | Path,
) -> ValidationReport:
    """Execute enabled validators and construct aggregate report."""
    results = [
        validator.validate(records=records, context=context)
        for validator in build_registry(context)
    ]
    return ValidationReport.from_results(dataset_path=dataset_path, results=results)


def load_records_from_csv(
    path: str | Path, context: ValidationContext
) -> list[dict[str, Any]]:
    """Load CSV records and apply schema-driven type casting."""
    csv_path = Path(path)
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    casted: list[dict[str, Any]] = []
    for row in rows:
        casted_row: dict[str, Any] = {}
        for column, value in row.items():
            casted_row[column] = _cast_value(
                value=value,
                expected_dtype=context.schema_config.columns.get(column).dtype
                if column in context.schema_config.columns
                else None,
            )
        casted.append(casted_row)
    return casted


def exit_code_for_report(report: ValidationReport) -> int:
    """Map report status to process exit code."""
    if report.overall_status == ValidationStatus.FAIL:
        return 1
    return 0


def _cast_value(value: str | None, expected_dtype: str | None) -> Any:
    if value is None or value == "":
        return None
    if expected_dtype is None:
        return value
    if expected_dtype == "str":
        return value
    if expected_dtype == "int":
        try:
            return int(value)
        except ValueError:
            return value
    if expected_dtype == "float":
        try:
            return float(value)
        except ValueError:
            return value
    if expected_dtype == "datetime":
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value
    return value
