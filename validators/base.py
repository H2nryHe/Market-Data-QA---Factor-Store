"""Core validator interfaces, configs, and result schema."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, Field

from schemas.rules import MarketOHLCVSchemaConfig, load_schema_config


class ValidationStatus(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


class Severity(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class ValidatorResult(BaseModel):
    """Outcome for a single validator run."""

    validator: str
    status: ValidationStatus
    severity: Severity
    affected_row_count: int
    rule: str
    message: str
    sample_rows: list[dict[str, Any]] = Field(default_factory=list)


class StructuralValidatorSettings(BaseModel):
    enabled: bool = True
    severity: Severity = Severity.HIGH


class DuplicatesValidatorSettings(BaseModel):
    enabled: bool = True
    severity: Severity = Severity.HIGH
    key_columns: list[str] = Field(
        default_factory=lambda: ["symbol", "timestamp", "source"]
    )


class TemporalValidatorSettings(BaseModel):
    enabled: bool = True
    severity: Severity = Severity.MEDIUM
    group_by: list[str] = Field(default_factory=lambda: ["symbol", "source"])
    max_gap_days: int = 3


class OutlierValidatorSettings(BaseModel):
    enabled: bool = True
    severity: Severity = Severity.MEDIUM
    method: str = "mad"
    fields: list[str] = Field(default_factory=lambda: ["open", "high", "low", "close"])
    z_threshold: float = 6.0
    min_group_size: int = 5
    group_by: list[str] = Field(default_factory=lambda: ["symbol"])


class ValidatorsConfig(BaseModel):
    structural: StructuralValidatorSettings = Field(
        default_factory=StructuralValidatorSettings
    )
    duplicates: DuplicatesValidatorSettings = Field(
        default_factory=DuplicatesValidatorSettings
    )
    temporal: TemporalValidatorSettings = Field(
        default_factory=TemporalValidatorSettings
    )
    outliers: OutlierValidatorSettings = Field(default_factory=OutlierValidatorSettings)


class ValidationConfig(BaseModel):
    """Top-level config for validator orchestration."""

    schema_config_path: str = "configs/schemas.yaml"
    report_sample_limit: int = 5
    validators: ValidatorsConfig = Field(default_factory=ValidatorsConfig)


class ValidationContext(BaseModel):
    """Runtime context shared across validators."""

    schema_config: MarketOHLCVSchemaConfig
    validation_config: ValidationConfig


class Validator(Protocol):
    """Composable validator interface."""

    name: str

    def validate(
        self, records: list[dict[str, Any]], context: ValidationContext
    ) -> ValidatorResult:
        """Validate records and return a structured result."""


def load_validation_config(path: str | Path) -> ValidationConfig:
    """Load validator config from YAML."""
    try:
        import yaml
    except ModuleNotFoundError as exc:  # pragma: no cover - environment-specific
        raise RuntimeError("PyYAML is required to load validation configs.") from exc

    config_path = Path(path)
    with config_path.open("r", encoding="utf-8") as f:
        payload = yaml.safe_load(f)
    return ValidationConfig.model_validate(payload)


def build_context(config_path: str | Path) -> ValidationContext:
    """Build validation context from top-level validation config path."""
    validation_config = load_validation_config(config_path)
    schema_config = load_schema_config(validation_config.schema_config_path)
    return ValidationContext(
        schema_config=schema_config, validation_config=validation_config
    )
