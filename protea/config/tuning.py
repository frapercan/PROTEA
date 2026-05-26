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
    amqp_heartbeat: int = Field(
        default=600,
        ge=0,
        description=(
            "Heartbeat AMQP en segundos para BlockingConnection (consumer "
            "y publisher). Pika usa I/O bloqueante; durante un cómputo "
            "largo el worker no cede al loop select() y el broker cierra "
            "la conexión con el default de 60s. 600s da un margen 10x "
            "manteniendo detección de peers muertos en pocos minutos. "
            "0 desactiva heartbeats. Override por env: "
            "PROTEA_AMQP_HEARTBEAT o PROTEA_TUNING__queue__amqp_heartbeat."
        ),
    )


class WorkerTuning(BaseModel):
    """Pool sizes, in-process caches and reaper timeouts.

    Sources: ``infrastructure/database/engine.py``,
    ``infrastructure/operations/{compute_embeddings,predict_go_terms}.py``,
    ``workers/stale_job_reaper.py``, ``api/cache.py`` (ver
    ``docs/CONFIG_INVENTORY.md`` §B).
    """

    db_pool_size: int = Field(
        default=20,
        ge=1,
        description="SQLAlchemy connection pool size. Tunear según concurrencia esperada.",
    )
    db_pool_max_overflow: int = Field(
        default=40,
        ge=0,
        description="Conexiones extra permitidas sobre el pool size cuando hay pico.",
    )
    db_pool_recycle_seconds: int = Field(
        default=3600,
        ge=60,
        description=(
            "Reciclar conexiones tras N segundos para evitar idle-timeout silencioso del DB."
        ),
    )
    model_cache_max: int = Field(
        default=1,
        ge=1,
        description=(
            "Modelos PLM en cache por proceso de embeddings. >1 acumula GB en GPU."
        ),
    )
    ref_cache_max: int = Field(
        default=1,
        ge=1,
        description="Reference data sets en cache por proceso predict.",
    )
    reaper_main_timeout_seconds: int = Field(
        default=21600,
        ge=300,
        description=(
            "Timeout duro antes de marcar jobs FAILED en producción (default 6h). "
            "Coordinator jobs como compute_embeddings pueden correr 2-3h en datasets "
            "grandes con 100% headroom; este es el corte global para capturar "
            "jobs stalled dentro de una jornada laboral (replaces 24h backstop)."
        ),
    )
    reaper_default_timeout_seconds: int = Field(
        default=3600,
        ge=300,
        description="Default constructor de StaleJobReaper (sobrescrito por main).",
    )
    reaper_stall_seconds: int = Field(
        default=1800,
        ge=60,
        description=(
            "Tiempo sin JobEvent antes de considerar un job stalled candidato a reapear."
        ),
    )
    worker_shutdown_grace_seconds: int = Field(
        default=30,
        ge=1,
        description=(
            "Ventana en segundos que el QueueConsumer concede a un job en vuelo "
            "tras recibir SIGTERM/SIGINT antes de marcarlo FAILED con "
            "error_code=WorkerShutdown via fallback session. 30s permite que "
            "callbacks cortos terminen naturalmente; jobs largos quedan "
            "registrados como FAILED en vez de quedarse colgados en RUNNING "
            "tras un redeploy."
        ),
    )
    job_heartbeat_interval_seconds: int = Field(
        default=30,
        ge=5,
        description=(
            "Intervalo en segundos entre heartbeats de lease para jobs en RUNNING "
            "(F-OPS-JOBS.1). El worker renueva leased_until cada N segundos; "
            "el reaper sólo mata jobs cuyo leased_until ha expirado. "
            "Override: PROTEA_JOB_HEARTBEAT_INTERVAL_SECONDS o "
            "PROTEA_TUNING__worker__job_heartbeat_interval_seconds."
        ),
    )
    api_cache_default_ttl_seconds: float = Field(
        default=300.0,
        ge=1.0,
        description="TTL default cache HTTP (api/cache.py). 5 min por defecto.",
    )


class OperationTuning(BaseModel):
    """Module-level chunk and batch sizes used inside operations.

    HTTP retry policy and per-source timeouts live inside their
    respective pydantic payloads (``InsertProteinsPayload``,
    ``LoadGoaAnnotationsPayload``, etc.) because the caller picks
    them per-job. The values here are infra-level: how to slice
    work between memory and broker pressure constraints.

    Sources: ``core/feature_enricher.py``, ``core/knn_search.py``,
    ``core/operations/{predict_go_terms,training_dump_helpers}.py``
    (ver ``docs/CONFIG_INVENTORY.md`` §C).
    """

    annotation_chunk_size: int = Field(
        default=10_000,
        ge=100,
        description=(
            "Filas por chunk al cargar/iterar anotaciones. Tunear "
            "según RAM disponible: 1k-100k razonable."
        ),
    )
    stream_chunk_size: int = Field(
        default=2_000,
        ge=100,
        description=(
            "Chunk size streaming PyArrow / SQLAlchemy yield_per. "
            "Más bajo reduce pico Python-object; más alto reduce "
            "round-trips. 500-10k razonable."
        ),
    )
    store_chunk_size: int = Field(
        default=10_000,
        ge=500,
        description=(
            "Filas por chunk al publicar predictions a la cola "
            "store. RabbitMQ cap 128 MB; 10k filas serializan "
            "~20-25 MB. 5k-50k según mensaje promedio."
        ),
    )
    numpy_query_chunk: int = Field(
        default=500,
        ge=10,
        description=(
            "Query chunk size para KNN numpy backend. Multiplicado "
            "por n_refs determina el pico de la matriz de "
            "distancias (500 x 500k x 4B ~ 1 GB)."
        ),
    )
    ref_cache_freshness_seconds: int = Field(
        default=300,
        ge=0,
        description=(
            "Ventana de frescura del disco cache de reference pool en segundos. "
            "Si los archivos .npy existen y su mtime es menor a este umbral, "
            "se salta la COUNT(*) de validación (consulta cara sobre JOIN de "
            "500k+ filas). 0 desactiva el skip y siempre ejecuta COUNT(*). "
            "Override: PROTEA_TUNING__operation__ref_cache_freshness_seconds."
        ),
    )
    aspect_knn_workers: int = Field(
        default=3,
        ge=1,
        description=(
            "Hilos del ThreadPoolExecutor para el KNN por aspecto cuando "
            "aspect_separated_knn=true. 3 = un hilo por aspecto (MF/BP/CC) "
            "en paralelo. numpy libera el GIL en operaciones matriciales "
            "por lo que la ganancia es real. 1 desactiva la paralelización."
        ),
    )


class APILimits(BaseModel):
    """HTTP boundary limits enforced at the FastAPI router layer.

    Sources: ``api/routers/{annotate,query_sets,support}.py`` (ver
    ``docs/CONFIG_INVENTORY.md`` §D).
    """

    max_fasta_bytes: int = Field(
        default=50 * 1024 * 1024,
        ge=1024,
        description=(
            "Tope upload FASTA en bytes. 50 MB cubre la mayoría de "
            "submissions; subir si el caso de uso lo justifica. "
            "Hardcodeado antes en dos routers; este campo dedupica."
        ),
    )
    max_comment_length: int = Field(
        default=500,
        ge=1,
        description="Caracteres máximos por comentario en /support.",
    )
    recent_limit: int = Field(
        default=20,
        ge=1,
        description="Items devueltos por defecto en /support/recent.",
    )
    page_limit: int = Field(
        default=100,
        ge=1,
        description="Page size hard cap para list endpoints de soporte.",
    )


class TuningSettings(BaseModel):
    """Root tuning model that composes per-category sub-models."""

    queue: QueueTuning = Field(default_factory=QueueTuning)
    worker: WorkerTuning = Field(default_factory=WorkerTuning)
    operation: OperationTuning = Field(default_factory=OperationTuning)
    api: APILimits = Field(default_factory=APILimits)


def _load_yaml_tuning(project_root: Path) -> dict[str, Any]:
    path = project_root / "protea" / "config" / "system.yaml"
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    raw = data.get("tuning") or {}
    return raw if isinstance(raw, dict) else {}


_SHORT_ALIASES: dict[str, tuple[str, str]] = {
    # Short alias -> (group, field). Kept for high-traffic ops knobs
    # that deserve a one-liner env var instead of the full
    # PROTEA_TUNING__group__field path.
    "PROTEA_AMQP_HEARTBEAT": ("queue", "amqp_heartbeat"),
}


def _apply_env_overrides(merged: dict[str, Any]) -> dict[str, Any]:
    """Merge env vars of the form PROTEA_TUNING__<group>__<field>=<value>.

    The double underscore is the conventional path separator (matches
    pydantic-settings env_nested_delimiter) so we don't collide with
    legitimate single underscores inside field names like
    ``publisher_max_attempts``.

    Also honours a small set of short aliases in :data:`_SHORT_ALIASES`
    so high-traffic knobs (heartbeat, pool sizes) can be tuned with a
    one-liner env var instead of the full nested path.
    """
    for key, value in os.environ.items():
        if not key.startswith(ENV_PREFIX):
            continue
        path = key[len(ENV_PREFIX):].split("__")
        if len(path) < 2:
            continue
        group, field = path[0].lower(), "__".join(path[1:]).lower()
        merged.setdefault(group, {})[field] = _coerce(value)
    for alias, (group, field) in _SHORT_ALIASES.items():
        raw = os.environ.get(alias)
        if raw is None:
            continue
        # Nested path wins over short alias when both are set so the
        # canonical form remains authoritative.
        if field in merged.get(group, {}):
            continue
        merged.setdefault(group, {})[field] = _coerce(raw)
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
