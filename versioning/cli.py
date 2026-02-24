"""CLI commands for snapshot creation and verification."""

from __future__ import annotations

from pathlib import Path

import typer

from versioning.snapshot import create_snapshot, verify_snapshot

app = typer.Typer(help="Dataset snapshot and verification commands.")


@app.callback()
def main() -> None:
    """Versioning CLI root."""


@app.command("snapshot")
def snapshot(
    input_path: Path = typer.Option(..., "--input", help="Input CSV path."),
    dataset_name: str = typer.Option(..., "--dataset", help="Dataset name."),
    output_root: Path = typer.Option(Path("data/snapshots"), "--output-root"),
    schema_version: str = typer.Option("1.0.0", "--schema-version"),
) -> None:
    """Create an immutable snapshot from input data."""
    snapshot_dir = create_snapshot(
        input_path=input_path,
        dataset_name=dataset_name,
        snapshots_root=output_root,
        schema_version=schema_version,
    )
    typer.echo(f"Snapshot created: {snapshot_dir}")


@app.command("verify")
def verify(snapshot_dir: Path = typer.Option(..., "--snapshot-dir")) -> None:
    """Verify snapshot integrity against manifest checksums."""
    result = verify_snapshot(snapshot_dir)
    typer.echo(result.message)
    raise typer.Exit(code=0 if result.ok else 1)


if __name__ == "__main__":
    app()
