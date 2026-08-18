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
        description=("Modelos PLM en cache por proceso de embeddings. >1 acumula GB en GPU."),
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
        description=("Tiempo sin JobEvent antes de considerar un job stalled candidato a reapear."),
    )
    reaper_event_grace_seconds: int = Field(
        default=2700,
        ge=60,
        description=(
            "Ventana de liveness por evento (C4 / NFR-INFRA). Un job RUNNING "
            "con lease expirado se considera VIVO (nunca se re-enquea ni se "
            "marca FAILED) si emitio un JobEvent dentro de esta ventana. El "
            "hilo heartbeat del lease puede starve bajo contienda del GIL en "
            "splits largos single-threaded (export_research_dataset / "
            "predict_go_terms emiten eventos cada 30-40 min), asi que un evento "
            "reciente es prueba fiable de que la operacion sigue trabajando. "
            "Default 2700s (45 min). Override: PROTEA_REAPER_EVENT_GRACE_SECONDS "
            "o PROTEA_TUNING__worker__reaper_event_grace_seconds."
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
    max_lease_requeues: int = Field(
        default=3,
        ge=0,
        description=(
            "Número máximo de re-enqueues que el StaleJobReaper concede a "
            "un job cuyo leased_until ha expirado antes de marcarlo FAILED "
            "con error_code=lease_expired (F-OPS-JOBS.1). 0 desactiva el "
            "re-enqueue (comportamiento legacy: directamente FAILED)."
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
    max_implicit_query_population: int = Field(
        default=50_000,
        ge=0,
        description=(
            "Largest query population a predict dispatch may select WITHOUT naming "
            "it. Giving neither query_set_id nor query_accessions selects every "
            "protein with an embedding, which reads as a filter that matched "
            "everything rather than as a filter that was never applied. On "
            "2026-08-18 that silently ran twelve jobs over 616,846 proteins when "
            "6,216 were intended, discarding 3.3 million rows. Above this count "
            "the coordinator refuses and names both numbers. A run that genuinely "
            "wants the corpus states it by passing the accessions. 0 disables the "
            "guard. Override: "
            "PROTEA_TUNING__operation__max_implicit_query_population."
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
    gpu_busy_retry_seconds: int = Field(
        default=8,
        ge=1,
        description=(
            "Backoff en segundos cuando el coordinador compute_embeddings "
            "encuentra la GPU ocupada por otro job y se re-encola. Bajo = la "
            "cola de embeddings drena rápido cuando varios usuarios envían a la "
            "vez (la GPU se serializa igual, pero el relevo es casi inmediato). "
            "Antes estaba hardcodeado en 60s, que dejaba a los que esperan "
            "parados hasta un minuto tras liberarse la GPU. "
            "Override: PROTEA_TUNING__operation__gpu_busy_retry_seconds."
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


class ServeTuning(BaseModel):
    """Live serving-path knobs for the one-click ``/annotate`` endpoint.

    These pin the validated offline pipeline onto the serving path without
    hardcoding ids or flipping behaviour. Every default reproduces the
    pre-existing serve behaviour exactly, so a deployment that sets none of
    these env vars serves identically to before. To activate the validated
    pipeline at deploy time, set:

    * ``default_embedding_config_id`` to the learned k-WTA retrieval config,
    * ``compute_v6_features`` and ``compute_lineage_features`` to ``True`` so
      the predict payload matches the validated reranker feature schema,
    * ``interpro_bp_graft`` to ``True`` for the InterPro2GO BP enrichment.

    Sources: ``api/routers/annotate.py`` and the predict post-KNN pipeline.
    """

    default_embedding_config_id: str | None = Field(
        default=None,
        description=(
            "Pinned EmbeddingConfig UUID used for retrieval by /annotate. When "
            "set AND the config already has embeddings, it overrides the "
            "smallest-param auto-pick. Empty/None (default) keeps the legacy "
            "smallest-param logic. Override: PROTEA_DEFAULT_EMBEDDING_CONFIG_ID "
            "o PROTEA_TUNING__serve__default_embedding_config_id."
        ),
    )
    compute_alignments: bool = Field(
        default=True,
        description=(
            "Serve-time compute_alignments flag in the predict payload. Default "
            "True preserves current behaviour. Override: "
            "PROTEA_TUNING__serve__compute_alignments."
        ),
    )
    compute_taxonomy: bool = Field(
        default=True,
        description=(
            "Serve-time compute_taxonomy flag in the predict payload. Default "
            "True preserves current behaviour. Override: "
            "PROTEA_TUNING__serve__compute_taxonomy."
        ),
    )
    compute_v6_features: bool = Field(
        default=False,
        description=(
            "Serve-time compute_v6_features flag. Default False preserves "
            "current behaviour. Set True to match the validated reranker "
            "feature schema (851849df). Override: "
            "PROTEA_TUNING__serve__compute_v6_features."
        ),
    )
    compute_lineage_features: bool = Field(
        default=False,
        description=(
            "Serve-time compute_lineage_features flag. Default False preserves "
            "current behaviour. Set True together with compute_v6_features to "
            "match the validated reranker feature schema (851849df). Override: "
            "PROTEA_TUNING__serve__compute_lineage_features."
        ),
    )
    interpro_bp_graft: bool = Field(
        default=False,
        description=(
            "Gate the optional InterPro2GO BP noisy-OR graft post-step in the "
            "predict pipeline. Default False = no graft (behaviour unchanged). "
            "Override: PROTEA_TUNING__serve__interpro_bp_graft."
        ),
    )
    interpro_bp_graft_weight: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description=(
            "Noisy-OR weight w in 1-(1-base)(1-w*interpro) for the InterPro2GO "
            "BP graft. Only consulted when interpro_bp_graft is True. Offline "
            "tuned per-category BP weights (NK 0.05 / LK 0.4 / PK 0.5); serve "
            "uses one weight because /annotate does not pre-categorise a query, "
            "defaulting to the LK/PK BP band where the lift concentrates. "
            "Override: PROTEA_TUNING__serve__interpro_bp_graft_weight."
        ),
    )
    interpro_bp_graft_source_version: str | None = Field(
        default=None,
        description=(
            "Pinned InterPro2GO mapping source_version for the BP graft. None "
            "(default) auto-picks the most recently loaded InterProGoMapping "
            "release. Only consulted when interpro_bp_graft is True. Override: "
            "PROTEA_TUNING__serve__interpro_bp_graft_source_version."
        ),
    )
    classifier_impl_by_category: bool = Field(
        default=False,
        description=(
            "Route the classifier candidate generator per CAFA category on the "
            "predict path so serve reproduces the composite champion pool: NK "
            "and LK cells use the M2 anc2vec head, PK cells use the two-tower "
            "sparse head (d8979601). Default False keeps the single global "
            "PROTEA_CLASSIFIER_IMPL selection so behaviour is unchanged. The "
            "category is derived per (protein, candidate aspect) from the same "
            "leakage-clean pre-cutoff experimental annotations the reranker "
            "uses, so a K protein takes its known-aspect candidates from the "
            "two-tower head and its other-aspect candidates from M2. Enabling "
            "this requires BOTH heads configured (the M2 checkpoint and the "
            "two-tower PROTEA_TWO_TOWER_* artifacts). Override: "
            "PROTEA_TUNING__serve__classifier_impl_by_category."
        ),
    )


class TuningSettings(BaseModel):
    """Root tuning model that composes per-category sub-models."""

    queue: QueueTuning = Field(default_factory=QueueTuning)
    worker: WorkerTuning = Field(default_factory=WorkerTuning)
    operation: OperationTuning = Field(default_factory=OperationTuning)
    api: APILimits = Field(default_factory=APILimits)
    serve: ServeTuning = Field(default_factory=ServeTuning)


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
    "PROTEA_REAPER_EVENT_GRACE_SECONDS": ("worker", "reaper_event_grace_seconds"),
    "PROTEA_DEFAULT_EMBEDDING_CONFIG_ID": ("serve", "default_embedding_config_id"),
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
        path = key[len(ENV_PREFIX) :].split("__")
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
