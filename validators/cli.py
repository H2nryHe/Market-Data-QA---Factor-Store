"""CLI commands for market data QA validation workflows."""

from __future__ import annotations

from pathlib import Path

import typer

from validators.base import build_context
from validators.orchestrator import (
    exit_code_for_report,
    load_records_from_csv,
    run_validators,
)

app = typer.Typer(help="Market Data QA validator commands.")


@app.callback()
def main() -> None:
    """Validator CLI root."""


@app.command("run")
def run(
    input_path: Path = typer.Option(..., "--input", help="Input CSV path."),
    config_path: Path = typer.Option(Path("configs/validation.yaml"), "--config"),
    report_path: Path = typer.Option(
        Path("data/qa/validation_report.json"), "--report"
    ),
) -> None:
    """Run configured validators and emit JSON + console summary."""
    context = build_context(config_path)
    records = load_records_from_csv(input_path, context)
    report = run_validators(records=records, context=context, dataset_path=input_path)
    saved_path = report.to_json_file(report_path)
    typer.echo(report.console_summary())
    typer.echo(f"Saved report: {saved_path}")
    raise typer.Exit(code=exit_code_for_report(report))


if __name__ == "__main__":
    app()
