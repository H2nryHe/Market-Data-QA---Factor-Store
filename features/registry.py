"""Feature registry for deterministic materialization."""

from __future__ import annotations

from features.base import Feature
from features.dollar_volume import DollarVolumeFeature
from features.mom_20d import Momentum20DFeature
from features.ret_1d import Return1DFeature
from features.ret_5d import Return5DFeature
from features.vol_20d import Volatility20DFeature


def default_registry() -> dict[str, Feature]:
    """Return canonical feature registry."""
    features: list[Feature] = [
        Return1DFeature(),
        Return5DFeature(),
        Volatility20DFeature(),
        Momentum20DFeature(),
        DollarVolumeFeature(),
    ]
    return {feature.name: feature for feature in features}


def get_features(feature_names: list[str]) -> list[Feature]:
    """Resolve selected features from registry."""
    registry = default_registry()
    missing = [name for name in feature_names if name not in registry]
    if missing:
        raise ValueError(f"Unknown feature(s): {missing}")
    return [registry[name] for name in feature_names]
