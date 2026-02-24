"""Typed schema and rule configuration models for market OHLCV validation."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

DTypeName = Literal["str", "float", "int", "datetime"]


class ColumnRule(BaseModel):
    """Column-level structural expectations."""

    dtype: DTypeName
    nullable: bool = False


class StructuralPolicy(BaseModel):
    """Policy toggles for structural schema checks."""

    strict_types: bool = True
    allow_extra_columns: bool = False


class BusinessRules(BaseModel):
    """Policy toggles for business rule checks."""

    enforce_ohlc_consistency: bool = True
    enforce_non_negative_volume: bool = True
    uniqueness_key: list[str] = Field(
        default_factory=lambda: ["symbol", "timestamp", "source"],
        description="Placeholder uniqueness key definition; duplicate detection is stage 2.",
    )


class MarketOHLCVSchemaConfig(BaseModel):
    """Top-level validation config for canonical market OHLCV data."""

    required_columns: list[str]
    columns: dict[str, ColumnRule]
    validation_policy: StructuralPolicy = Field(default_factory=StructuralPolicy)
    rules: BusinessRules = Field(default_factory=BusinessRules)


def load_schema_config(path: str | Path) -> MarketOHLCVSchemaConfig:
    """Load schema config from a YAML file."""
    try:
        import yaml
    except ModuleNotFoundError as exc:  # pragma: no cover - environment-specific
        raise RuntimeError("PyYAML is required to load YAML schema configs.") from exc

    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as f:
        payload = yaml.safe_load(f)
    return MarketOHLCVSchemaConfig.model_validate(payload)
