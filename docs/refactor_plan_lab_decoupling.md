# PROTEA ↔ protea-reranker-lab: plan de desacoplamiento

**Fecha:** 2026-05-04
**Estado:** propuesta, sin aprobar
**Autor:** discusión con Claude (sesión guru/PROTEA-audit)

Plan completo para limpiar el contrato entre PROTEA y `protea-reranker-lab`. La fricción central es que las definiciones de columnas viven duplicadas en tres sitios y el cómputo está disperso entre tres archivos de PROTEA. Esto crea un canal silencioso de schema-drift que solo se detecta en runtime de inferencia.

---

## 1. Diagnóstico del acoplamiento actual

El contrato declarado es el **parquet + manifest**. Las definiciones de columnas viven duplicadas en tres sitios:

| Dónde | Contiene | Quién lo usa |
|---|---|---|
| `protea-reranker-lab/src/.../reranker.py` (L22-63) | `NUMERIC_FEATURES`, `CATEGORICAL_FEATURES`, `ALL_FEATURES`, `FEATURE_FAMILIES` | Lab: training + filtering por familia |
| `protea-reranker-lab/.../contracts.py` | Re-exporta lo anterior + `ManifestV1`, `compute_*_sha` | **Boundary oficial** |
| `PROTEA/protea/core/reranker.py` (L39-111) | `NUMERIC_FEATURES`, `CATEGORICAL_FEATURES`, `ALL_FEATURES` — idénticos | PROTEA: `predict`, `apply_reranker`, `prepare_dataset`, **export** |

Puntos de drift detectados:

1. `PROTEA/core/parquet_export.py:30` importa `ALL_FEATURES` de PROTEA, no del lab. El `schema_sha` que escribe en el manifest se computa sobre esa lista (L192-194). Si la lista de PROTEA y la del lab divergen, el dump tiene unas columnas y el manifest declara las otras — y nadie se entera hasta inferencia.
2. `PROTEA/core/reranker.py:395-420` define `infer_active_feature_families` con un comentario literal: *"Keep this in sync with `protea_reranker_lab.contracts.FEATURE_FAMILIES`"*. La sincronía es manual.
3. `predict_go_terms.py:937-957` valida `feature_schema_sha` en runtime y rechaza si difiere — pero es **defensa reactiva**: el booster ya está entrenado, el dump ya está hecho.
4. **Bug latente:** `parquet_export.py:192-194` calcula sha como `hashlib.sha256(json.dumps(ALL_FEATURES, sort_keys=True).encode()).hexdigest()[:12]`, mientras que `lab.contracts.compute_schema_sha` usa `"|".join(sorted(...))`. Los dos shas **no coinciden** hoy. Verificarlo es el primer test del refactor.

**El verdadero problema:** se mezclan dos cosas que deberían vivir separadas:

- **Definición** de features (qué columnas, qué familia, qué dtype) — *datos*.
- **Cómputo** de features (cómo se calculan a partir de KNN + secuencias + taxonomía + anc2vec) — *código de PROTEA*.

Hoy la definición vive en dos sitios y el cómputo está repartido entre `feature_engineering.py`, `feature_enricher.py` y los `_load_*` de `predict_go_terms.py`.

---

## 2. Opciones de diseño

| Opción | Idea | Coste | Cleanness |
|---|---|---|---|
| **A. Lab canónico, PROTEA importa** | `protea-reranker-lab.contracts` única fuente. PROTEA borra su copia. Lab se promociona de dev-dep a runtime. | Bajo | Alto |
| **B. PROTEA canónico, lab importa** | Inverso. Lab pierde su independencia (hoy es pydantic-only). | Bajo | Bajo |
| **C. Tercer paquete `protea-features`** | Lo mínimo: column lists + family map + sha. Ambos dependen. | Medio | Muy alto |
| **D. Vendor + CI assert** | Dos copias, fallar el CI si divergen. | Mínimo | Bajo |
| **E. Schema declarativo (YAML)** | Features en un YAML, ambos lo leen. | Alto | Máximo |

**Recomendación: A**, con un partido pequeño dentro del lab (ver fase 1).

Razones:

- El lab ya está diseñado para ser el contrato — ya re-exporta desde `contracts.py`. Solo falta que PROTEA lo respete.
- C es más limpio teóricamente pero supone un repo nuevo, otro release, otro pyproject. No merece para una tesis.
- E es donde uno acaba si lo lleva al extremo. Para 52 features con un orden conocido y una tesis con fecha, no compensa.

---

## 3. El plan, en fases

### Fase 0 — desbroce (sin cambiar contrato)

Sin estos pasos, las fases siguientes mueven código muerto y duplican trabajo.

- **0.1** Borrar `protea/core/operations/train_reranker.py` (1825 líneas, no registrado, ver CLAUDE.md L107-108). Mover los helpers que `ExportResearchDatasetOperation` consume a un módulo neutro `protea/core/training_dump_helpers.py`.
- **0.2** Deduplicar `_update_parent_progress` (idéntico en `compute_embeddings.py:590` y `predict_go_terms.py:2016`) → `protea/core/contracts/parent_progress.py`.
- **0.3** *(Opcional)* Convertir `UniProtHttpMixin` a delegación. No desbloquea el plan pero quita ruido.

**Coste:** 1-2 días. **Riesgo:** muy bajo (dead code y duplicado).

### Fase 1 — partido del lab + dependencia limpia

El lab hoy hace `contracts.py` → `from .reranker import ALL_FEATURES, FEATURE_FAMILIES`, y `reranker.py` importa LightGBM. Si PROTEA hace `from protea_reranker_lab.contracts import ALL_FEATURES`, arrastra LightGBM. PROTEA ya lo tiene en deps, pero **el contrato no debería arrastrar el motor**.

- **1.1** Dentro del lab, partir `reranker.py`:
  ```
  src/protea_reranker_lab/
    schema.py       # NUEVO: solo datos — ALL_FEATURES, FEATURE_FAMILIES,
                    #        RESERVED_COLUMNS, NUMERIC_FEATURES,
                    #        CATEGORICAL_FEATURES, EMBEDDING_PCA_DIM
    contracts.py    # imports de schema, ManifestV1, DatasetSpec, compute_*_sha
    reranker.py     # solo el motor LightGBM (TrainConfig, fit, predict_streaming)
  ```
  `schema.py` no depende de nada que no sea stdlib. `contracts.py` depende de pydantic + schema. `reranker.py` depende de lightgbm + numpy.

- **1.2** Borrar de `PROTEA/protea/core/reranker.py` las constantes `NUMERIC_FEATURES`, `CATEGORICAL_FEATURES`, `ALL_FEATURES`, `EMBEDDING_PCA_DIM` (L39-102). Sustituir por imports del lab.

- **1.3** En `PROTEA/protea/core/parquet_export.py`, sustituir el cálculo manual de sha por `compute_schema_sha` del lab. Esperar que aparezcan los manifests con sha distinto del legacy — script de migración o flag de compat.

- **1.4** Promocionar el lab de dev-dep a runtime-dep en `pyproject.toml`. Para tesis, el path-dep en runtime vale; para producción, publicar en index privado.

**Coste:** 1 día. **Riesgo medio:** el sha-bug latente puede salir, hay que recalcular shas de manifests existentes.

**Criterio de éxito:** `grep ALL_FEATURES protea/core/` da cero resultados con definición; solo imports.

### Fase 2 — registry de cómputo

La definición está unificada (fase 1). El cómputo sigue disperso. Aquí el corazón del problema.

- **2.1** Crear `protea/core/features/`:
  ```
  protea/core/features/
    __init__.py       # re-exporta REGISTRY y register
    registry.py       # Feature, FeatureRegistry, register
    knn.py            # @register: distance, k_position, vote_count, ...
    alignment.py      # identity_nw, similarity_nw, ...
    taxonomy.py       # taxonomic_*, tax_voters_*
    anc2vec.py        # anc2vec_*
    emb_pca.py        # emb_pca_query_*
    annotation_meta.py # qualifier, evidence_code, aspect
  ```

  ```python
  # registry.py
  from dataclasses import dataclass
  from typing import Callable, Literal

  @dataclass(frozen=True)
  class Feature:
      name: str
      family: str
      dtype: Literal["numeric", "categorical"]
      compute: Callable[..., dict]   # firma exacta a definir según contexto

  class FeatureRegistry:
      def __init__(self):
          self._features: dict[str, Feature] = {}
      def register(self, f: Feature) -> None: ...
      def get(self, name: str) -> Feature: ...
      def families(self) -> dict[str, list[str]]: ...
      def selected(self, families: list[str], drop: list[str]) -> list[str]: ...

  REGISTRY = FeatureRegistry()
  ```

- **2.2** Mover el cómputo actual en bloques:
  - `feature_engineering.py` (alignment NW/SW, taxonomy pair) → `alignment.py`, `taxonomy.py`.
  - `feature_enricher.py` (las 25 features de la familia `v6_features`) → repartir entre `taxonomy.py` (tax_voters_*), `anc2vec.py`, `emb_pca.py`.
  - `predict_go_terms.py:_load_*` que carga datos de soporte → no tocar la **carga**, solo el cálculo de la columna.

- **2.3** En `parquet_export.py` y en `predict_go_terms._predict_batch`, usar el registry:
  ```python
  ctx = PredictionContext(query, refs, knn_result, anc2vec, ...)
  for feat in registry.selected(active_families, drop=p.drop_features):
      feat.compute(ctx, predictions)
  ```

- **2.4** Borrar `infer_active_feature_families` (PROTEA/reranker.py:395). El registry da la respuesta directamente.

**Coste:** 4-7 días. Es el grueso. **Riesgo medio-alto** porque toca el camino caliente del modelo. Necesitas tests de regresión que comparen un dump pre-refactor vs post-refactor para una entrada fija.

**Criterio de éxito:** añadir una feature nueva = (a) añadir nombre a `lab/schema.py`, (b) registrar `Feature` en el archivo de su familia. Cero cambios en `parquet_export.py` ni en `_predict_batch`.

### Fase 3 — guardrails

- **3.1** Test de invariante en `tests/test_feature_contract.py`:
  ```python
  def test_protea_registry_covers_lab_contract():
      from protea_reranker_lab.schema import ALL_FEATURES
      from protea.core.features import REGISTRY
      missing = set(ALL_FEATURES) - set(REGISTRY.names())
      extra   = set(REGISTRY.names()) - set(ALL_FEATURES)
      assert not missing, f"PROTEA no computa: {missing}"
      assert not extra,   f"PROTEA computa columnas no declaradas: {extra}"

  def test_feature_families_match():
      from protea_reranker_lab.schema import FEATURE_FAMILIES
      assert REGISTRY.families() == FEATURE_FAMILIES
  ```

- **3.2** Validación al exportar:
  ```python
  expected = compute_schema_sha(ALL_FEATURES)
  written  = compute_schema_sha([c for c in train_df.columns if c in ALL_FEATURES])
  assert written == expected
  ```

- **3.3** Validación al inferir simplificada: `_apply_reranker_if_aligned` deja de "inferir" familias y compara el sha del registry con el del booster.

**Coste:** 1 día.

### Fase 4 — opcional, alto valor: cómputo unificado export-vs-inferencia

Hoy la misma feature se calcula con código distinto en export-time vs inference-time. Eso es la causa profunda del schema-drift.

Con la fase 2 hecha, ambos usan el `Feature.compute(ctx, ...)`. La diferencia entre export e inferencia se reduce a **qué `ctx` se construye** (export tiene ground-truth label; inferencia no). El cómputo de cada columna es literalmente el mismo objeto `Feature`.

Es la victoria conceptual: un único pipeline de features, dos consumidores. Cierra la prevención del drift "porque por construcción no puede haber dos definiciones del mismo número".

---

## 4. Decisiones que necesitan tomarse antes de empezar

1. **¿Lab como runtime-dep de PROTEA?** Si sí, fase 1.4 procede. Si prefieres mantenerlo dev-only, hay que ir a opción C (paquete `protea-features` separado).
2. **¿Romper schema_sha existentes?** El cálculo actual de `parquet_export.py` no coincide con `lab.contracts.compute_schema_sha`. Unificar invalida los manifests ya escritos. Opciones:
   - Script de migración que reescribe manifests.
   - Mantener compat retroactiva computando ambas y validando contra cualquiera.
   - Versionar el sha (`schema_sha_v2`).
3. **¿Hasta dónde llegar?** Para tesis, **fases 0-3 son suficientes y dejan el sistema en estado defendible**. La fase 4 es la elegante pero arriesga calendario.
4. **Tests de regresión bit-exactos.** Antes de la fase 2, fijar un `tests/data/golden_dump_v9_pk_bpo.parquet` y un test que `assert_frame_equal` post-refactor. Sin esto, la fase 2 va a ciegas.

---

## 5. Roadmap resumen

| Fase | Tiempo | Bloquea a | Reversible | Valor |
|---|---|---|---|---|
| 0 — desbroce | 1-2d | nada | sí | medio (calidad) |
| 1 — fuente única | 1d | 2, 3 | sí | alto (elimina drift de definición) |
| 2 — registry de cómputo | 4-7d | 4 | parcial | alto (one-place add feature) |
| 3 — guardrails | 1d | nada | sí | alto (preventivo) |
| 4 — cómputo unificado | 3-5d | nada | parcial | muy alto (elegancia) |

---

## 6. Lo que NO se recomienda

- Mover `predict_go_terms.py` a la jerarquía de Operations (refactor grande señalado en auditoría previa) **al mismo tiempo**. Ortogonal pero se solapa en archivos. Hazlo después de la fase 2 — el registry deja `_predict_batch` mucho más manejable.
- YAML de schema (opción E). La indirección "string en YAML → función en Python" se paga siempre y solo se cobra si el equipo crece o si pasa a un lenguaje distinto.
- Reescribir `feature_enricher.py` antes de la fase 2. Sus 611 líneas son densas con orden de pasos delicado. El registry obliga a desentrañarlo, mejor todo de golpe.

---

## 7. Notas de la auditoría previa (contexto)

Antes de este plan, una auditoría sobre PROTEA identificó cinco refactors candidatos. Tres de ellos tocan archivos que esta planificación también modifica:

- **#1** `UniProtHttpMixin` → delegación. **Ortogonal**, fase 0.3.
- **#2** `BaseWorker.handle_job` long method. **Ortogonal**, no afectado por este plan.
- **#3** `_update_parent_progress` duplicado. **Incluido**, fase 0.2.
- **#4** `PredictGOTermsBatchOperation` (1400 líneas). **Solapado** con fase 2 — el registry simplifica `_predict_batch` y los `_load_*` siguen como cargadores de soporte. La extracción `KnnRunner` / `RerankerApplier` cae natural después de la fase 2.
- **#5** `train_reranker.py` 1825 líneas no registrado. **Incluido**, fase 0.1.
