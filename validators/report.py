"""Validation report model, JSON serialization, and console summaries."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, Field

from validators.base import ValidationStatus, ValidatorResult


class ValidationReport(BaseModel):
    """Aggregate report across all validators."""

    dataset_path: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    overall_status: ValidationStatus
    results: list[ValidatorResult]

    @classmethod
    def from_results(
        cls, dataset_path: str | Path, results: list[ValidatorResult]
    ) -> ValidationReport:
        overall_status = ValidationStatus.PASS
        if any(result.status == ValidationStatus.FAIL for result in results):
            overall_status = ValidationStatus.FAIL
        elif any(result.status == ValidationStatus.WARN for result in results):
            overall_status = ValidationStatus.WARN

        return cls(
            dataset_path=str(dataset_path),
            overall_status=overall_status,
            results=results,
        )

    def to_json_file(self, path: str | Path) -> Path:
        """Write report JSON to disk."""
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(self.model_dump_json(indent=2), encoding="utf-8")
        return output_path

    def console_summary(self) -> str:
        """Render compact human-readable report summary."""
        lines = [f"Validation overall_status={self.overall_status}"]
        for result in self.results:
            lines.append(
                " | ".join(
                    [
                        f"validator={result.validator}",
                        f"status={result.status}",
                        f"severity={result.severity}",
                        f"affected_rows={result.affected_row_count}",
                        f"rule={result.rule}",
                        f"message={result.message}",
                    ]
                )
            )
        return "\n".join(lines)


def report_to_pretty_json(report: ValidationReport) -> str:
    """Return pretty JSON for display/debugging."""
    payload = report.model_dump(mode="json")
    return json.dumps(payload, indent=2)
