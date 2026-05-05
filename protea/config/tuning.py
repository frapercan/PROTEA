"""Runtime tuning settings (T-CONF.2).

Externalises hardcoded module-level constants from ``protea/`` so an
operator can tune throughput, retry policy and timeouts per
deployment target (dev, prod-cloud, hpc-bsc, hpc-airgap) without
touching code.

Hierarchy (lowest to highest priority):

  1. Defaults baked into the pydantic models below.
  2. ``tuning:`` section in ``protea/config/system.yaml``.
  3. Environment variables of the form ``PROTEA_TUNING__<group>__<field>``.

Currently scoped to the ``QueueTuning`` group as a proof of concept.
The remaining categories from ``docs/CONFIG_INVENTORY.md``
(WorkerTuning, OperationTuning, APILimits, ResearchKnobs) follow the
same pattern and will be added incrementally.

Example::

    from protea.config.tuning import get_tuning

    settings = get_tuning()
    for attempt in range(settings.queue.publisher_max_attempts):
        ...
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

ENV_PREFIX = "PROTEA_TUNING__"


class QueueTuning(BaseModel):
    """RabbitMQ publisher / consumer retry and dispatch knobs.

    Sources: ``infrastructure/queue/publisher.py`` and
    ``infrastructure/queue/consumer.py`` (ver
    ``docs/CONFIG_INVENTORY.md`` §A).
    """

    publisher_max_attempts: int = Field(
        default=12,
        ge=1,
        description=(
            "Reintentos máximos al publicar a RabbitMQ. 12 attempts cubren "
            "~4 min de broker downtime con backoff exponencial cap a 30s."
        ),
    )
    publisher_base_delay: float = Field(
        default=1.0,
        ge=0.0,
        description=(
            "Backoff inicial publisher en segundos. Multiplica x2 por "
            "intento hasta el cap interno de 30s."
        ),
    )
    oom_max_retries: int = Field(
        default=5,
        ge=0,
        description="Reintentos al hit CUDA OOM en GPU worker.",
    )
    oom_base_delay: int = Field(
        default=5,
        ge=0,
        description="Backoff inicial OOM en segundos.",
    )
    oom_max_delay: int = Field(
        default=300,
        ge=1,
        description="Cap del backoff OOM en segundos (5 min default).",
    )


class TuningSettings(BaseModel):
    """Root tuning model that composes per-category sub-models."""

    queue: QueueTuning = Field(default_factory=QueueTuning)


def _load_yaml_tuning(project_root: Path) -> dict[str, Any]:
    path = project_root / "protea" / "config" / "system.yaml"
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    raw = data.get("tuning") or {}
    return raw if isinstance(raw, dict) else {}


def _apply_env_overrides(merged: dict[str, Any]) -> dict[str, Any]:
    """Merge env vars of the form PROTEA_TUNING__<group>__<field>=<value>.

    The double underscore is the conventional path separator (matches
    pydantic-settings env_nested_delimiter) so we don't collide with
    legitimate single underscores inside field names like
    ``publisher_max_attempts``.
    """
    for key, value in os.environ.items():
        if not key.startswith(ENV_PREFIX):
            continue
        path = key[len(ENV_PREFIX):].split("__")
        if len(path) < 2:
            continue
        group, field = path[0].lower(), "__".join(path[1:]).lower()
        merged.setdefault(group, {})[field] = _coerce(value)
    return merged


def _coerce(value: str) -> Any:
    """Best-effort string -> int/float/bool coercion for env values."""
    lo = value.strip().lower()
    if lo in {"true", "false"}:
        return lo == "true"
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def _resolve_project_root() -> Path:
    """Resolve the project root from this file's location.

    ``protea/config/tuning.py`` -> parents[2] = project root.
    """
    return Path(__file__).resolve().parents[2]


@lru_cache(maxsize=1)
def get_tuning() -> TuningSettings:
    """Load and cache the tuning settings.

    Cache reset (mostly for tests):
        ``get_tuning.cache_clear()``
    """
    raw = _load_yaml_tuning(_resolve_project_root())
    raw = _apply_env_overrides(raw)
    return TuningSettings.model_validate(raw)
