# Hardcoded parameters inventory (T-CONF.1)

Snapshot 2026-05-05 tras un grep sistemático sobre `protea/` buscando constantes módulo-level (`^_?[A-Z][A-Z_]+ = <num>`), defaults hardcodeados en signatures de pydantic payloads, y valores literales obvios. Output: tabla nombre → fichero:línea → categoría → propósito → rango sugerido → exempt? si aplica.

Este inventario es la base de **T-CONF.2** (externalización a `protea_core.config.Settings` con jerarquía defaults < yaml < env < flags) y **T-CONF.3** (doc viva autogenerada).

## Política de externalización

Cada parámetro identificado abajo:

- Si **influye en throughput, memoria, latencia, o robustez** ante cargas variables: candidato directo a `Settings`. Operador de plataforma debe poder ajustarlo en `config/{env}.yaml` o env var sin tocar código.
- Si es **estructural / físico** (longitud de hash MD5, índices posicionales en formato GAF, dimensión de un space PCA fija por modelo): `# config-exempt: <razón>`. Estas no se mueven a `Settings` pero quedan documentadas.
- Si es **límite de seguridad / contractual** (max FASTA upload size, max comment length): a `Settings` con sub-modelo `APILimits` para que infosec pueda revisarlo en config.

## Inventario

### A. QueueTuning (RabbitMQ + worker dispatch)

| Constante | Fichero:línea | Valor | Propósito | Rango sugerido | Exempt |
|-----------|---------------|------:|-----------|----------------|--------|
| `_MAX_ATTEMPTS` | `infrastructure/queue/publisher.py:14` | 12 | Reintentos máximos al publicar a RabbitMQ. Cubre ~4 min de broker down. | 5-20; tunear según SLA broker | no |
| `_BASE_DELAY` | `infrastructure/queue/publisher.py:15` | 1 | Backoff inicial publisher (seg). Multiplica x2 hasta cap 30s. | 0.5-5 | no |
| `_OOM_MAX_RETRIES` | `infrastructure/queue/consumer.py:28` | 5 | Reintentos al hit CUDA OOM en GPU worker. | 3-10 | no |
| `_OOM_BASE_DELAY` | `infrastructure/queue/consumer.py:29` | 5 | Backoff inicial OOM (seg). | 1-30 | no |
| `_OOM_MAX_DELAY` | `infrastructure/queue/consumer.py:30` | 300 | Cap del backoff OOM (5 min). | 60-900 | no |
| `prefetch_count` | `infrastructure/queue/consumer.py:62, 189` | 1 | Prefetch RabbitMQ por consumer. 1 = strict serialization. | 1-10 según operación | no |

### B. WorkerTuning (pools, caches, reapers)

| Constante | Fichero:línea | Valor | Propósito | Rango sugerido | Exempt |
|-----------|---------------|------:|-----------|----------------|--------|
| `pool_size` | `infrastructure/database/engine.py:12` | 20 | Connection pool size SQLAlchemy. | 5-50 según carga concurrent | no |
| `_MODEL_CACHE_MAX` | `core/operations/compute_embeddings.py:609` | 1 | Modelos PLM en cache por proceso. >1 acumula GB en GPU. | 1-2 (GPU memory hard limit) | no |
| `_REF_CACHE_MAX` | `core/operations/predict_go_terms.py:83` | 1 | Reference data en cache por proceso predict. | 1-2 | no |
| `timeout_seconds` (reaper main) | `workers/stale_job_reaper.py:26` | 21600 | 6h timeout antes de marcar jobs FAILED. | 1800-43200 según SLA | no |
| `timeout_seconds` (reaper default) | `workers/stale_job_reaper.py:50` | 3600 | Default constructor; main usa 21600. | 1800-43200 | no |
| `stall_seconds` | `workers/stale_job_reaper.py:52` | 1800 | Tiempo sin JobEvent antes de considerar stalled. | 600-3600 | no |
| `_DEFAULT_TTL` | `api/cache.py:18` | 300.0 | TTL default cache HTTP (5 min). | 60-3600 según endpoint | no |

### C. OperationTuning (chunks, batches, HTTP)

| Constante | Fichero:línea | Valor | Propósito | Rango sugerido | Exempt |
|-----------|---------------|------:|-----------|----------------|--------|
| `_ANNOTATION_CHUNK_SIZE` | `core/feature_enricher.py:42`, `core/operations/{train_reranker,predict_go_terms}.py` | 10_000 | Filas por chunk al cargar anotaciones. | 1k-100k según RAM | no |
| `_STREAM_CHUNK_SIZE` | `core/operations/{train_reranker,predict_go_terms}.py` | 2_000 | Chunk size streaming PyArrow. | 500-10k | no |
| `_STORE_CHUNK_SIZE` | `core/operations/predict_go_terms.py:872` | 10_000 | Filas por chunk al publicar a `protea.predictions.write`. ~20-25 MB serializado. RabbitMQ cap 128 MB. | 5k-50k según mensaje promedio | no |
| `_NUMPY_QUERY_CHUNK` | `core/knn_search.py:135` | 500 | Query chunk size para KNN numpy. | 100-2000 según RAM | no |
| `_N_THRESHOLDS` | `core/metrics.py:34` | 101 | Threshold sweep [0.0, 0.01, ..., 1.0] para Fmax. | 51, 101, 201 | no |
| `batch_size` (compute_embeddings payload) | `core/operations/compute_embeddings.py:90, 108` | 1 | Sequences por batch GPU. 1 evita OOM en proteínas largas. | 1-32 según PLM | no |
| `batch_size` (predict_go_terms payload) | `core/operations/predict_go_terms.py:171` | 1024 | Queries por batch KNN. | 256-4096 según vector dim | no |
| `batch_size` (parquet read) | `core/operations/train_reranker.py:1822` | 200_000 | Filas por batch al leer parquet eval. | 50k-500k según RAM | no |
| `gene_product_batch_size` (QuickGO) | `core/operations/load_quickgo_annotations.py:46` | 200 | Batch QuickGO API. Sus límites internos. | 100-500 (revisar API spec) | no |
| `timeout_seconds` (UniProt insert) | `core/operations/insert_proteins.py:30` | 60 | HTTP timeout por request UniProt. | 30-300 | no |
| `timeout_seconds` (UniProt metadata) | `core/operations/fetch_uniprot_metadata.py:29` | 60 | Idem | 30-300 | no |
| `timeout_seconds` (GOA load) | `core/operations/load_goa_annotations.py:34` | 300 | Timeout ftp.ebi GOA descarga (5 min). | 120-900 | no |
| `timeout_seconds` (ontology snapshot) | `core/operations/load_ontology_snapshot.py:19` | 120 | Timeout descarga OBO. | 60-300 | no |
| `timeout_seconds` (QuickGO) | `core/operations/load_quickgo_annotations.py:43` | 300 | Timeout QuickGO API. | 120-900 | no |
| `max_retries` (UniProt) | `core/operations/insert_proteins.py:33`, `fetch_uniprot_metadata.py` | 6 | Reintentos HTTP. | 3-10 | no |
| `backoff_base_seconds` | `core/operations/{insert,fetch}_uniprot*.py` | 0.8 | Backoff inicial UniProt. | 0.5-2 | no |
| `backoff_max_seconds` | `core/operations/{insert,fetch}_uniprot*.py` | 20.0 | Cap backoff UniProt. | 10-60 | no |
| `jitter_seconds` | `core/operations/{insert,fetch}_uniprot*.py` | 0.4 | Jitter agregado al sleep. | 0-1 | no |

### D. APILimits (HTTP boundaries)

| Constante | Fichero:línea | Valor | Propósito | Rango sugerido | Exempt |
|-----------|---------------|------:|-----------|----------------|--------|
| `_MAX_FASTA_BYTES` | `api/routers/annotate.py:95`, `query_sets.py:112` | 50 MB | Tope upload FASTA. Hardcodeado en dos sitios. | 10-200 MB; **dedupe a Settings** | no |
| `_MAX_COMMENT_LENGTH` | `api/routers/support.py:14` | 500 | Max chars comentario soporte. | 200-2000 | no |
| `_RECENT_LIMIT` | `api/routers/support.py:15` | 20 | Items en /support/recent. | 10-100 | no |
| `_PAGE_LIMIT` | `api/routers/support.py:16` | 100 | Page size hard cap. | 50-500 | no |

### E. ResearchKnobs (modelado, no infraestructura)

| Constante | Fichero:línea | Valor | Propósito | Notas |
|-----------|---------------|------:|-----------|-------|
| `EMBEDDING_PCA_DIM` | `core/reranker.py:102` | 16 | Dim PCA reducido para feature engineering. **CONTRATO con `protea-contracts.feature_schema`**. No mover a Settings (es parte del schema canónico). | exempt: contrato con `protea-contracts` |
| `N_THRESHOLDS` (CAFA sweep) | `core/metrics.py:34` | 101 | Granularidad sweep para Fmax. Cambiar afecta números canónicos. | exempt: parte de la metodología CAFA |

### F. Estructurales (config-exempt)

GAF column indices (`load_goa_annotations.py:90-97`): `_IDX_ACCESSION=1`, `_IDX_QUALIFIER=3`, etc. Son posiciones físicas del formato GAF 2.x; cambiar significaría no leer GAF. **exempt: format spec**.

Cualquier `min_length=1` o `max_length=255` en `Field(...)` de pydantic payloads en `api/routers/`: longitudes de validación de strings (UUIDs, names, paths). **exempt: shape de payloads** (revisable junto con `protea-contracts` si se mueve a paquete).

## Resumen cuantitativo

- **Total entradas**: 31 constantes con candidate to externalize.
- **Estructurales exempt**: ~10 (GAF indices, hash lengths, payload shape constraints).
- **Research knobs exempt**: 2 (PCA dim, threshold sweep).
- **A externalizar a Settings (T-CONF.2)**: **31** parámetros, 5 categorías (`QueueTuning`, `WorkerTuning`, `OperationTuning`, `APILimits`, `ResearchKnobs`).
- **Duplicación detectada**: `_ANNOTATION_CHUNK_SIZE` aparece en 3 ficheros (`feature_enricher`, `train_reranker`, `predict_go_terms`); `_STREAM_CHUNK_SIZE` en 2; `_MAX_FASTA_BYTES` en 2 routers. Externalizar **dedupica por construcción** (un solo Settings).

## Próximos pasos (T-CONF.2 + T-CONF.3)

T-CONF.2: crear `protea_core.config` con sub-modelos `QueueTuning`, `WorkerTuning`, `OperationTuning`, `APILimits`. `Settings` raíz que los compone. `protea/config/{dev,prod,hpc-bsc,hpc-airgap}.yaml` con valores per target. Ruta canónica de carga: defaults < yaml < env vars (`PROTEA__QUEUE__MAX_ATTEMPTS=15` etc.) < flags CLI. Sustituir las 31 referencias en código por `settings.X.Y`.

T-CONF.3: autogenerar `docs/source/appendix/configuration.rst` desde el modelo pydantic con docstrings + rangos del inventario. Test CI que parsea cada env yaml y confirma schema válido.

**AC final** (definido en master plan v3 §5 T-CONF.2): `grep -rE "^_?[A-Z][A-Z_]+\s*=\s*[0-9]" protea-core/` solo devuelve constantes con `# config-exempt: <razón>` (los 12 estructurales / research knobs documentados aquí).
