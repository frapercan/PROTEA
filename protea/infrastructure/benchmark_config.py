"""Loader for ``protea/config/benchmark.yaml``.

Mirrors the ``load_settings`` pattern in ``settings.py`` but returns a typed
dataclass specific to the benchmark matrix view. Consumed by the
``/benchmark/matrix`` router to avoid hardcoding stage taxonomy, labels,
or GO-namespace constants.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class BenchmarkConfig:
    preferred_default_stages: tuple[str, ...]
    baseline_scoring_name: str | None
    hidden_stages: frozenset[str]
    stage_labels: dict[str, str]
    eval_set_labels: dict[str, str]
    categories: tuple[str, ...]
    aspects: tuple[str, ...]

    def label_for_stage(self, name: str) -> str:
        """Return the human-readable label for a stage.

        Falls back to a Title-Cased version of the raw name if the YAML
        does not define an explicit label.
        """
        if name in self.stage_labels:
            return self.stage_labels[name]
        return name.replace("_", " ").title()


_DEFAULT_CATEGORIES = ("NK", "LK", "PK")
_DEFAULT_ASPECTS = ("BPO", "MFO", "CCO")


def _as_str_tuple(val: Any, fallback: tuple[str, ...]) -> tuple[str, ...]:
    if not val:
        return fallback
    if isinstance(val, (list, tuple)):
        return tuple(str(v) for v in val)
    return fallback


def _as_str_dict(val: Any) -> dict[str, str]:
    if not val or not isinstance(val, dict):
        return {}
    return {str(k): str(v) for k, v in val.items()}


def load_benchmark_config(project_root: Path) -> BenchmarkConfig:
    """Load ``protea/config/benchmark.yaml`` into a :class:`BenchmarkConfig`.

    Missing file → sane defaults (no hidden stages, no custom labels,
    categories = NK/LK/PK, aspects = BPO/MFO/CCO). This keeps the API
    functional even in fresh checkouts without the YAML.
    """
    path = project_root / "protea" / "config" / "benchmark.yaml"
    raw: dict[str, Any] = {}
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

    stages_raw: dict[str, Any] = raw.get("stages") or {}

    preferred = _as_str_tuple(stages_raw.get("preferred_default"), ())
    baseline = stages_raw.get("baseline_scoring_name")
    baseline_name = str(baseline) if baseline else None
    hidden = frozenset(_as_str_tuple(stages_raw.get("hidden"), ()))
    labels = _as_str_dict(stages_raw.get("labels"))

    eval_labels = _as_str_dict(raw.get("eval_set_labels"))
    cats = _as_str_tuple(raw.get("categories"), _DEFAULT_CATEGORIES)
    asps = _as_str_tuple(raw.get("aspects"), _DEFAULT_ASPECTS)

    return BenchmarkConfig(
        preferred_default_stages=preferred,
        baseline_scoring_name=baseline_name,
        hidden_stages=hidden,
        stage_labels=labels,
        eval_set_labels=eval_labels,
        categories=cats,
        aspects=asps,
    )
