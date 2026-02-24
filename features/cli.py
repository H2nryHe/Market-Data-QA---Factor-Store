"""CLI for snapshot-linked feature materialization."""

from __future__ import annotations

from pathlib import Path

import typer

from features.materialize import materialize_from_snapshot

app = typer.Typer(help="Feature materialization commands.")


@app.callback()
def main() -> None:
    """Feature CLI root."""


@app.command("materialize")
def materialize(
    snapshot_dir: Path = typer.Option(
        ..., "--snapshot-dir", help="Input snapshot directory."
    ),
    config_path: Path = typer.Option(Path("configs/features.yaml"), "--config"),
) -> None:
    """Materialize deterministic feature artifacts from a verified snapshot."""
    result = materialize_from_snapshot(
        snapshot_dir=snapshot_dir, config_path=config_path
    )
    typer.echo(f"cache_hit={result.cache_hit}")
    typer.echo(f"cache_key={result.cache_key}")
    typer.echo(f"artifact={result.artifact_path}")
    typer.echo(f"manifest={result.manifest_path}")


if __name__ == "__main__":
    app()
