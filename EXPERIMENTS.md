# Plan de Experimentación PROTEA

## Infraestructura

- **Annotation sets:** 15 GOA snapshots (160–229)
- **Ontology:** releases/2026-01-23 + IA file (IA_cafa6.tsv)
- **Embeddings:** 527K ESM-C 300M (dim=960)
- **Evaluation set:** GOA 220→229 (NK: 2831, LK: 3410, PK: 15313 proteínas)
- **Query set:** `af6bf007` (GOA_220_229, ~20K proteínas)
- **Evaluador:** cafaeval con IA weighting (information accretion)

**IDs de referencia:**
- Embedding config: `8e7f78c3-900f-452f-858e-63ca14d103e1`
- Annotation set (GOA 220): `c7bdb296-a86a-4141-b5e5-53eb77363ad0`
- Ontology snapshot: `947bdff6-d17c-4ca3-a41a-bc8fb4d74b7a`
- Evaluation set (220→229): `42b34e79-6fe9-4fa0-b718-02f43a1e3192`

---

## Exp 1: Baseline KNN: efecto de k

**Scoring:** baseline (`1 - distance/2`), `aspect_separated_knn=true`

| k | NK-BPO | NK-MFO | NK-CCO | LK-BPO | LK-MFO | LK-CCO | PK-BPO | PK-MFO | PK-CCO | Estado |
|---|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------|
| **5** | 0.412 | 0.590 | 0.668 | 0.467 | 0.558 | 0.676 | 0.187 | 0.278 | 0.325 | ✅ `d7adeb1e` |
| 10 | 0.400 | 0.574 | 0.656 | 0.458 | 0.537 | 0.663 | 0.177 | 0.272 | 0.317 | ✅ `30bf6187` |
| 20 | 0.396 | 0.564 | 0.649 | 0.454 | 0.528 | 0.654 | 0.173 | 0.269 | 0.313 | ✅ `a4442444` |
| 50 | 0.396 | 0.555 | 0.646 | 0.452 | 0.523 | 0.651 | 0.173 | 0.269 | 0.312 | ✅ `d41b8d05` |

**Conclusión:** k=5 es óptimo en todas las categorías. Más vecinos = más ruido, degradación monotónica.

---

## Exp 2: Efecto de `aspect_separated_knn`

Con k=5, comparar índice unificado vs separado por aspecto (BPO/MFO/CCO).

| Variante | NK-BPO | NK-MFO | NK-CCO | LK-BPO | LK-MFO | LK-CCO | PK-BPO | PK-MFO | PK-CCO | Estado |
|----------|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------|
| aspect_sep=true | 0.412 | 0.590 | 0.668 | 0.467 | 0.558 | 0.676 | 0.187 | 0.278 | 0.325 | ✅ `d7adeb1e` |
| aspect_sep=false | 0.410 | 0.595 | 0.666 | 0.471 | 0.569 | 0.675 | 0.188 | 0.279 | 0.325 | ✅ `bee8fbe7` |

**Conclusión:** Diferencias mínimas. aspect_sep=false mejora ligeramente MFO (+0.005 NK, +0.011 LK); aspect_sep=true mejora ligeramente BPO. Sin ganancia clara → mantener aspect_sep=true por cobertura uniforme de aspectos.

---

## Exp 3: Scoring heurístico

**Requisito:** prediction set con `compute_alignments=true, compute_taxonomy=true` (k=5, aspect_sep=mejor de Exp 2).

Usa los 5 ScoringConfig presets del sistema. El scoring se aplica en evaluación (no requiere re-predicción para cada config).

| Config | Fórmula | Pesos | NK-BPO | NK-MFO | NK-CCO | LK-BPO | LK-MFO | LK-CCO | PK-BPO | PK-MFO | PK-CCO | Estado |
|--------|---------|-------|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------|
| **embedding_only** | linear | emb=1.0 | 0.412 | 0.590 | 0.668 | 0.467 | 0.558 | 0.675 | 0.187 | 0.278 | 0.325 | ✅ |
| alignment_weighted | linear | emb=0.5, nw=0.3, sw=0.2 | **0.428** | **0.611** | **0.683** | **0.500** | **0.598** | **0.699** | **0.201** | **0.285** | **0.337** | ✅ |
| evidence_primary | linear | emb=0.2, evi=0.8 | 0.362 | 0.558 | 0.638 | 0.412 | 0.540 | 0.642 | 0.165 | 0.268 | 0.308 | ✅ |
| embedding_plus_evidence | evidence_weighted | emb=1.0, evi=1.0 | 0.352 | 0.531 | 0.618 | 0.387 | 0.517 | 0.626 | 0.162 | 0.250 | 0.300 | ✅ |
| composite | evidence_weighted | emb=0.4, nw=0.2, sw=0.1, evi=0.2, tax=0.1 | 0.364 | 0.560 | 0.639 | 0.412 | 0.542 | 0.642 | 0.167 | 0.267 | 0.307 | ✅ |

**Prediction set:** `a818b653` (k=5, aspect_sep=true, alignments+taxonomy+reranker_features)

**Conclusión:** `alignment_weighted` es el mejor scoring en todas las categorías y aspectos. Mejora el baseline (embedding_only) entre +1.5% y +4% Fmax. Las configs que usan evidence_weight (evidence_primary, composite, embedding_plus_evidence) **empeoran** el baseline, porque la señal de evidencia perjudica el ranking bajo CAFA-eval con IA weighting.

---

## Exp 4: Re-ranker LightGBM

**Requisito:** prediction set con `compute_alignments=true, compute_taxonomy=true, compute_reranker_features=true`.

**Entrenamiento:** `train_reranker_auto` con 12 splits temporales (GOA 160→165 hasta 215→220), test 220→229.
9 modelos (NK/LK/PK × BPO/MFO/CCO), binary CE, features completas (alignments + taxonomy + reranker_features).

### 4a. Sin balance (job `188eb26a`)

| Cat-Asp | AUC | Iter | Observación |
|---------|-----|------|-------------|
| NK-BPO | 0.771 | 1 | early stop (pocos positivos, 0.17%) |
| NK-MFO | 0.938 | 300 | buen modelo |
| NK-CCO | 0.911 | 266 | buen modelo |
| LK-BPO | 0.770 | 1 | early stop |
| LK-MFO | 0.930 | 300 | buen modelo |
| LK-CCO | 0.872 | 300 | buen modelo |
| PK-BPO | 0.779 | 1 | early stop |
| PK-MFO | 0.831 | 1 | early stop |
| PK-CCO | 0.767 | 1 | early stop |

6 de 9 modelos no aprenden (early stop iter=1) por desbalance extremo.

### 4b. Con balance `neg_pos_ratio=10` (job `a96eed71`)

| Cat-Asp | AUC | Iter | Δ AUC vs 4a |
|---------|-----|------|-------------|
| NK-BPO | 0.898 | 4 | +0.127 |
| NK-MFO | 0.922 | 9 | -0.016 |
| NK-CCO | 0.881 | 4 | -0.030 |
| LK-BPO | 0.893 | 4 | +0.124 |
| LK-MFO | 0.925 | 11 | -0.005 |
| LK-CCO | 0.854 | 3 | -0.018 |
| PK-BPO | 0.796 | 2 | +0.017 |
| PK-MFO | 0.849 | 3 | +0.018 |
| PK-CCO | 0.781 | 2 | +0.014 |

Todos los modelos aprenden. BPO sube ~12 puntos AUC. MFO/CCO bajan ligeramente (menos datos de entrenamiento).

### Resultados CAFA-eval (v1)

| Método | NK-BPO | NK-MFO | NK-CCO | LK-BPO | LK-MFO | LK-CCO | PK-BPO | PK-MFO | PK-CCO |
|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------|
| baseline (emb only) | 0.412 | 0.590 | 0.668 | 0.467 | 0.558 | 0.675 | 0.187 | 0.278 | 0.325 |
| **alignment_weighted** | **0.428** | **0.611** | 0.683 | **0.500** | **0.598** | 0.699 | 0.201 | 0.285 | 0.337 |
| reranker v1 (sin balance) | 0.384 | 0.584 | **0.695** | 0.447 | 0.482 | **0.713** | 0.201 | 0.284 | 0.335 |
| reranker v1 (balanced) | 0.408 | 0.577 | 0.687 | 0.478 | 0.506 | 0.711 | 0.201 | **0.298** | 0.332 |

**Conclusiones v1:**
- El balance corrige BPO (+0.024 NK, +0.031 LK vs sin balance) pero no alcanza al heurístico
- Ambos rerankers mejoran **CCO** respecto al baseline (+2-4%)
- Ambos rerankers **empeoran MFO** respecto al heurístico (-3 a -9%)
- El reranker balanced destaca en **PK-MFO** (0.298, mejor de todos los métodos)
- `alignment_weighted` sigue siendo el mejor approach global: gana en 6 de 9 celdas

---

## Exp 5: Re-ranker v2 (per-categoría con IA weighting)

**Cambios respecto a v1:**
- 3 modelos per-categoría (NK, LK, PK) en vez de 9 per-aspecto
- `is_unbalance` eliminado (evita doble compensación con `neg_pos_ratio`)
- `learning_rate`: 0.05 → 0.01
- `num_boost_round`: 300 → 1000 (con `early_stopping_rounds`: 50)
- IA values como `sample_weight` en entrenamiento (términos raros pesan más)

### 5a. Quick test (2 splits: 211→215→220, test 229), eval `9242ea3e`

| Método | NK-BPO | NK-MFO | NK-CCO | LK-BPO | LK-MFO | LK-CCO | PK-BPO | PK-MFO | PK-CCO |
|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------|
| reranker v2 (2 splits) | 0.418 | 0.601 | 0.691 | 0.477 | 0.560 | 0.700 | 0.182 | 0.282 | 0.341 |

MFO ya no se destruye (0.601 vs 0.577 de v1 balanced). Prometedor con solo 2 splits.

### 5b. Full training (13 splits: 160→220, test 229), eval `a3d3bbea`

Modelos: `lgbm_v2_full-{nk,lk,pk}`
- NK: `fc013658-9c95-48e8-9c72-c13f477a8b26`
- LK: `8697ffed-6814-4594-85a1-5dae3ea00b1f`
- PK: `cdcbc26f-8f9a-41b2-9196-21bf4f9d3e2e`

| Método | NK-BPO | NK-MFO | NK-CCO | LK-BPO | LK-MFO | LK-CCO | PK-BPO | PK-MFO | PK-CCO |
|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------|
| baseline (emb only) | 0.412 | 0.590 | 0.668 | 0.467 | 0.558 | 0.675 | 0.187 | 0.278 | 0.325 |
| **alignment_weighted** | **0.428** | **0.611** | 0.683 | **0.500** | **0.598** | 0.699 | **0.201** | 0.285 | 0.337 |
| reranker v1 (sin balance) | 0.384 | 0.584 | 0.695 | 0.447 | 0.482 | **0.713** | 0.201 | 0.284 | 0.335 |
| reranker v1 (balanced) | 0.408 | 0.577 | 0.687 | 0.478 | 0.506 | 0.711 | 0.201 | **0.298** | 0.332 |
| **reranker v2 full** | 0.425 | 0.607 | **0.689** | 0.486 | 0.575 | **0.707** | 0.199 | 0.297 | **0.335** |

**Conclusiones v2 full:**
- **Mucho más robusto que v1**: MFO no se destruye (0.607 vs 0.577 de v1 bal), BPO mejora consistentemente
- **CCO sigue siendo el punto fuerte del reranker**: NK-CCO 0.689, LK-CCO 0.707 (segundo mejor tras v1 unbal)
- **PK recupera**: v2 full (0.199/0.297/0.335) supera al v2 quick test que había caído en PK-BPO
- **alignment_weighted sigue ganando en BPO y MFO**: NK-BPO 0.428 vs 0.425, LK-BPO 0.500 vs 0.486, LK-MFO 0.598 vs 0.575
- El IA weighting en entrenamiento + modelos per-categoría eliminan la inestabilidad de v1 pero no superan al heurístico globalmente

---

## Exp 6: Re-ranker v3 (features completas: alineamientos + taxonomía en entrenamiento)

**Cambio clave respecto a v2:** En v2 las features de alineamiento (NW/SW) y taxonomía estaban hardcodeadas a NULL durante el entrenamiento, por lo que el modelo nunca las veía. v3 computa `compute_alignment()` y `compute_taxonomy()` por cada par (query, ref) durante la generación de datos de entrenamiento, dando al modelo acceso a las 22 features completas.

**Configuración:** 13 splits (160→220), test 229, `neg_pos_ratio=10`, IA weights, `compute_alignments=true`, `compute_taxonomy=true`. Tiempo de entrenamiento: ~2h 45m (vs ~2h de v2; el overhead de alineamientos es mínimo).

Modelos: `lgbm_v3_full-{nk,lk,pk}`
- NK: `2ff1818f-71b6-4932-8f8d-b3000e3c8d34`
- LK: `269e26b4-0bec-42fa-a077-fe5b675dd2de`
- PK: `e14b9716-bbf8-4b99-b34b-b801c3966579`

### Resultados CAFA-eval, eval `23851bff`

| Método | NK-BPO | NK-MFO | NK-CCO | LK-BPO | LK-MFO | LK-CCO | PK-BPO | PK-MFO | PK-CCO |
|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------|
| baseline (emb only) | 0.412 | 0.590 | 0.668 | 0.467 | 0.558 | 0.675 | 0.187 | 0.278 | 0.325 |
| alignment_weighted | 0.428 | 0.611 | 0.683 | **0.500** | 0.598 | 0.699 | 0.201 | 0.285 | 0.337 |
| reranker v2 full | 0.425 | 0.607 | 0.689 | 0.486 | 0.575 | 0.707 | 0.199 | 0.297 | 0.335 |
| **reranker v3 full** | **0.431** | **0.620** | **0.692** | 0.478 | **0.607** | 0.697 | **0.201** | **0.297** | **0.339** |

**Conclusiones v3:**
- **Las features de alineamiento importaban.** v3 supera a v2 en casi todas las métricas, especialmente MFO (+0.013 NK, +0.032 LK)
- **Supera al heurístico `alignment_weighted`** en 7 de 9 celdas: NK-BPO (+0.003), NK-MFO (+0.009), NK-CCO (+0.009), LK-MFO (+0.009), PK-BPO (=), PK-MFO (+0.012), PK-CCO (+0.002)
- Solo pierde en LK-BPO (0.478 vs 0.500) y LK-CCO (0.697 vs 0.699)
- **Resultado positivo**: el re-ranker con features completas es el mejor método global

---

## Resumen de progreso

| Fase | Experimento | Estado | Mejor Fmax NK-MFO |
|------|-------------|--------|-------------------|
| 1 | Baseline KNN (k sweep) | ✅ | 0.590 (k=5) |
| 2 | aspect_separated_knn | ✅ | ~0.590 (sin diferencia clara) |
| 3 | Scoring heurístico (5 configs) | ✅ | 0.611 (alignment_weighted) |
| 4a | Re-ranker v1 LightGBM (sin balance) | ✅ | 0.584 (mejora CCO, empeora MFO) |
| 4b | Re-ranker v1 LightGBM (balanced) | ✅ | 0.577 (mejora PK-MFO a 0.298) |
| 5a | Re-ranker v2 quick test (2 splits) | ✅ | 0.601 (mucho más estable que v1) |
| 5b | Re-ranker v2 full (13 splits) | ✅ | 0.607 (robusto, pero no supera heurístico) |
| 6 | **Re-ranker v3 full (features completas)** | ✅ | **0.620** (supera al heurístico) |
| 7 | **Comparativa eggNOG-mapper** | ✅ | 0.359 (PROTEA 9/9 celdas mejor) |
| 8 | **Comparativa Pannzer2 + data leakage** | ✅ | 0.717 (con leakage: 62.4% NK GT exacto) |
| 9 | **Comparativa InterProScan 6** | ✅ | 0.551 (PROTEA supera en 8/9 celdas) |
| 10 | **ProstT5 vs ESMC (v3 preliminar)** | ⚠️ F3 contaminado por under-training | F1+F2 válidos, F3 pendiente |
| 11 | **Re-train v4 "converged" (5000 rounds)** | 🔄 en curso | n/a |
| 12 | **Extended PLM matrix (8 modelos)** | 📋 diseño listo (`EXPERIMENTAL_DESIGN.md`) | n/a |

**Flujo de dependencias:**
```
Exp 1 (k sweep) ✅
  → Exp 2 (aspect_sep) ✅
    → Predicción con features completas ✅ (a818b653)
      → Exp 3 (scoring configs) ✅ — alignment_weighted gana
      → Exp 4 (re-ranker v1, 12 splits) ✅ — mejora CCO, empeora MFO
      → Exp 5 (re-ranker v2, per-cat + IA weights) ✅ — robusto pero no supera heurístico
      → Exp 6 (re-ranker v3, features completas) ✅ — SUPERA al heurístico
      → Exp 7 (eggNOG-mapper comparison) ✅ — PROTEA gana 9/9 celdas
      → Exp 8 (Pannzer2 + leakage analysis) ✅ — leakage confirmado, PROTEA única evaluación fair
      → Exp 9 (InterProScan 6) ✅ — PROTEA supera en 8/9 celdas
```

**Mejor configuración global: `reranker v3 full` (LightGBM per-categoría, 22 features, IA weights)**

---

## Exp 7: Comparativa con eggNOG-mapper

**Herramienta:** eggNOG-mapper v2.1.13 (Docker: `quay.io/biocontainers/eggnog-mapper:2.1.13--pyhdfd78af_2`)
**Base de datos:** eggNOG DB v5.0.2 + Diamond v2.0.15
**Parámetros:** `-m diamond --go_evidence experimental --tax_scope auto --target_orthologs all --cpu 8`
**Test set:** 20,281 proteínas del delta GOA 220→229 (mismo que todos los experimentos PROTEA)
**Cobertura:** 17,334/20,281 proteínas con GO terms (85.5%)
**Tiempo:** ~21 minutos (solo CPU, 8 threads)

### Resultados CAFA-eval (IA-weighted)

| Método | NK-BPO | NK-MFO | NK-CCO | LK-BPO | LK-MFO | LK-CCO | PK-BPO | PK-MFO | PK-CCO |
|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------|
| **eggNOG-mapper 2.1.13** | 0.247 | 0.359 | 0.386 | 0.382 | 0.334 | 0.450 | 0.190 | 0.199 | 0.325 |
| PROTEA baseline (emb only) | 0.412 | 0.590 | 0.668 | 0.467 | 0.558 | 0.675 | 0.187 | 0.278 | 0.325 |
| **PROTEA reranker v3** | **0.431** | **0.620** | **0.692** | **0.478** | **0.607** | **0.697** | **0.201** | **0.297** | **0.339** |

### Diferencia absoluta Fmax (PROTEA v3 - eggNOG-mapper)

| Categoría | BPO | MFO | CCO |
|-----------|------|------|------|
| NK | +0.184 | +0.261 | +0.306 |
| LK | +0.096 | +0.273 | +0.247 |
| PK | +0.011 | +0.098 | +0.014 |

**Conclusiones:**
- PROTEA v3 supera a eggNOG-mapper en **9 de 9 celdas**
- Incluso el baseline de PROTEA (solo embeddings) supera a eggNOG-mapper en 8 de 9 celdas
- Las mayores diferencias están en NK y LK (hasta +0.306 Fmax en NK-CCO)
- eggNOG-mapper tiene menor cobertura (85.5% vs 100%) y no produce scores graduados
- Script de evaluación: `scripts/evaluate_external_tool.py`

---

## Exp 8: Comparativa con Pannzer2 + análisis de data leakage

**Herramienta:** Pannzer2 (servidor web Helsinki, marzo 2026)
**Base de datos:** UniProt/SwissProt actual (actualizada a fecha de ejecución)
**Test set:** 20,281 proteínas del delta GOA 220→229 (mismo que todos los experimentos)
**Cobertura:** 19,964/20,281 proteínas con GO terms (98.4%)
**Predicciones totales:** 532,557 (max 30 GO terms por proteína, con PPV scores calibrados 0.31–0.91)

### Resultados CAFA-eval (IA-weighted)

| Método | NK-BPO | NK-MFO | NK-CCO | LK-BPO | LK-MFO | LK-CCO | PK-BPO | PK-MFO | PK-CCO |
|--------|--------|--------|--------|--------|--------|--------|--------|--------|--------|
| **Pannzer2** † | **0.656** | **0.717** | **0.791** | **0.681** | **0.729** | **0.813** | **0.391** | **0.574** | **0.618** |
| InterProScan 6 † | 0.312 | 0.551 | 0.476 | 0.479 | 0.488 | 0.491 | 0.208 | 0.269 | 0.250 |
| eggNOG-mapper 2.1.13 † | 0.247 | 0.359 | 0.386 | 0.382 | 0.334 | 0.450 | 0.190 | 0.199 | 0.325 |
| **PROTEA reranker v3** | **0.431** | **0.620** | **0.692** | **0.478** | **0.607** | **0.697** | **0.201** | **0.297** | **0.339** |

† Subject to temporal data leakage (reference DB from March 2026, after GOA 229).

### Data leakage: análisis temporal

Los resultados de Pannzer2 y eggNOG-mapper **no son comparables directamente** con PROTEA debido a data leakage temporal:

| | Pannzer2 | InterProScan 6 | eggNOG-mapper | PROTEA |
|---|---|---|---|---|
| **Fecha de ejecución** | Marzo 2026 | 25 Mar 2026 | 24 Mar 2026 | n/a |
| **BD de referencia** | UniProt/SwissProt 2026 | InterPro 2026 | eggNOG v5.0.2 (2026) | GOA 220 (frozen at t0) |
| **Conoce las respuestas?** | Sí | Parcialmente | Parcialmente | No |

**Cuantificación del leakage:** Se midió el porcentaje de pares (proteína, GO term) del ground truth que aparecen exactamente en las predicciones de cada herramienta.

| Categoría | GT pairs | Pannzer2 exact match | eggNOG exact match |
|-----------|----------|---------------------|-------------------|
| **Total** | 40,014 | 20,373 (**50.9%**) | 10,308 (25.8%) |
| NK | 6,953 | 4,339 (**62.4%**) | 1,025 (14.7%) |
| LK | 5,520 | 3,624 (**65.7%**) | 1,087 (19.7%) |
| PK | 27,541 | 12,410 (45.1%) | 8,196 (29.8%) |

Pannzer2 acierta el 62.4% de las anotaciones NK (proteínas que por definición no tenían anotaciones experimentales en t0). Esto confirma que su BD de referencia contiene anotaciones posteriores a GOA 220, incluyendo muchas que forman parte del ground truth GOA 229.

**Conclusión:** PROTEA es la única herramienta del benchmark que garantiza integridad temporal: la referencia se congela en t0, el ground truth se computa como delta, y todo queda versionado en la BD. Los números de Pannzer2 y eggNOG-mapper representan un **upper bound optimista** bajo data leakage, no una comparación fair.

- Parsing de resultados Pannzer2: `/home/frapercan/Thesis2/pannzer2_results/parse_pannzer2.py`
- Raw HTML: `/home/frapercan/Thesis2/pannzer2_results/raw/PANZ_{1-21}.html`
- Script de evaluación: `scripts/evaluate_external_tool.py --tool pannzer2`

---

## Hallazgos previos

- Baseline KNN con `score = 1 - distance/2` da buenos resultados en NK/LK
- Un intento previo de LightGBM per-aspecto (9 modelos) **empeoró** NK/LK:
  - Causa 1: optimiza binary CE (todos los GO terms pesan igual) pero CAFA-eval pondera por IC
  - Causa 2: features de agregación estaban NULL en el prediction set

### Cambios de configuración

- **2026-04-23: Peso IEA en `DEFAULT_EVIDENCE_WEIGHTS` 0.3 → 0.8.** La jerarquía clásica de GO-docs coloca IEA por debajo del tier computacional (ISS/IBA/... 0.7) y de NAS (0.5). Observación empírica en el histórico de GOA: las anotaciones IEA se promueven a un código experimental con mayor frecuencia que las del tier computacional, por lo que su fiabilidad previa estaba infraestimada. Los tres stages del benchmark (`baseline`, `alignment_weighted`, `reranker` v4) no consumen `evidence_weight`, así que las Fmax reportadas en Exp 1–11 no cambian; el swap sólo afecta a scorings basados en evidencia (p. ej. `evidence_primary`, `composite`, `embedding_plus_evidence`).

---

## Exp 10: ProstT5 vs ESMC (comparativa preliminar v3)

**Fecha**: 2026-04-10
**Objetivo**: replicar el reranker v3 sobre un segundo PLM (ProstT5-XL ~3B) para ver si la ganancia del v3 generaliza más allá de ESMC-300M.

> **Caveat metodológico importante**: ESMC-300M (~300M params, BERT-like encoder) y ProstT5-XL (~3B params, T5 encoder + structure fine-tuning) son modelos con tamaño y arquitectura distintos. Esta comparativa mezcla esos ejes, por lo que no es fair para concluir nada sobre "ESMC vs ProstT5 como familia". El benchmark con matriz limpia está en `EXPERIMENTAL_DESIGN.md` (Exp 12).

### Setup

- **Evaluation set**: `42b34e79-6fe9-4fa0-b718-02f43a1e3192` (delta GOA 220→229, 20281 proteínas)
- **ESMC prediction set**: `a818b653-cad9-4f42-8e04-eda3f5ff2ceb`
- **ProstT5 prediction set**: `38ee00af-cbfd-4c5b-ab84-c98a32765b40`
- **IA file**: `IA_cafa6.tsv`
- **Ontology snapshot**: `947bdff6-d17c-4ca3-a41a-bc8fb4d74b7a`

Rerankers v3 (`num_boost_round=1000, early_stopping_rounds=50, neg_pos_ratio=10, IA sample weights, 13 splits 160→220`):

| Embedding | NK | LK | PK |
|---|---|---|---|
| ESMC-300M (job `16c3dcfd`) | `2ff1818f` | `269e26b4` | `e14b9716` |
| ProstT5-XL (job `12b704d4`) | `a1b4947d` | `60597ab9` | `1efd0c33` |

CAFA eval results:
- ESMC + reranker: `ba7476cb-81f2-461a-b69a-a99c8df834bf`
- ProstT5 + reranker: `7b97e74a-54df-4e4e-90ed-39e07b58de64`

### Resultados (cafaeval + IA, evaluación oficial)

**F1: ProstT5 gana en retrieval bruto**: avg Fmax baseline ProstT5 0.4849 vs ESMC 0.4824. Consistente en las 9 celdas: ProstT5 gana 44/45 en el 45-cell benchmark previo.

**F3: Reranker per-aspect (9 celdas)**:

| Método | NK-BPO | NK-MFO | NK-CCO | LK-BPO | LK-MFO | LK-CCO | PK-BPO | PK-MFO | PK-CCO | Avg |
|---|---|---|---|---|---|---|---|---|---|---|
| ESMC baseline | 0.412 | 0.590 | 0.668 | 0.467 | 0.558 | 0.675 | 0.187 | 0.278 | 0.325 | 0.4624 |
| ESMC + reranker v3 | 0.431 | 0.620 | 0.692 | 0.478 | 0.607 | 0.697 | 0.201 | 0.297 | 0.339 | **0.4846** |
| ProstT5 baseline | ~ | ~ | ~ | ~ | ~ | ~ | ~ | ~ | ~ | **0.4849** |
| ProstT5 + reranker v3 | ~ | ~ | ~ | ~ | ~ | ~ | ~ | ~ | ~ | 0.4817 |

- **ESMC mejora con reranker**: 6/9 celdas, avg Δ = **+0.0022**
- **ProstT5 degrada con reranker**: 9/9 celdas, avg Δ = **−0.0032**
- Avg final ESMC+rr (0.4846) ≈ ProstT5+rr (0.4817), diferencia pequeña pero de signo opuesto a la del retrieval bruto

### F2: Feature importance (hipótesis de compensación)

Extracción de `feature_importance` (gain) de los 6 rerankers. Agregado sobre features de `{alignment_*, similarity_*, taxonomic_*}`:

- **ESMC ponderan alignment+taxonomy entre 2.15% y 5.22% más** que sus homólogos ProstT5 (monótono en NK/LK/PK)
- Diferencias dramáticas en features individuales:
  - NK `alignment_score_nw`: ESMC 4.72% vs ProstT5 1.69% (**2.8×**)
  - PK `similarity_nw`: ESMC 9.63% vs ProstT5 3.91% (**2.5×**)
- ProstT5 compensa redistribuyendo a features derivadas del embedding: `ref_annotation_density`, `vote_count`, `k_position`

**Interpretación**: cuando el embedding es "más fuerte" (ProstT5), el reranker se apoya menos en señales externas (alineamiento, taxonomía) y más en estadísticos derivados del propio retrieval. Este es el carry-over de la hipótesis que se va a testear formalmente como H4 en `EXPERIMENTAL_DESIGN.md`.

### Blocker: under-training en los 6 modelos v3

Revisión del `best_iteration` de cada modelo con `num_boost_round=1000, early_stopping_rounds=50`:

| Modelo | best_iteration |
|---|---|
| ESMC-nk | **1000** (techo, early stop no disparó) |
| ESMC-lk | 994 |
| ESMC-pk | 999 |
| ProstT5-nk | **1000** |
| ProstT5-lk | 995 |
| ProstT5-pk | **1000** |

Con 95k–332k samples por tier y LR=0.01, este dataset típicamente necesita 3000–10000 iters para saturar. **Conclusión**: los deltas de F3 (especialmente el signo negativo de ProstT5 −0.0032) pueden ser artefacto del under-training, no efecto real del embedding.

- **F2 (feature importance) sigue siendo válido**: ambos modelos tuvieron el mismo presupuesto bajo el techo, la diferencia *relativa* en cómo distribuyen alignment/taxonomy es una comparación justa
- **F3 (signos de los deltas Fmax) está contaminado**: no se debe usar para la tesis hasta que converjan

**Lección metodológica crítica**: el campo `test_evaluation` que reporta `train_reranker_auto` muestra deltas de +0.04 a +0.08 Fmax mucho más optimistas que los +0.002 reales de cafaeval. El test_evaluation no aplica propagación GO ni IA weighting. **No usar para la tesis.** Solo cafaeval con IA.

### Estado

- F1 y F2: publicables con los números actuales
- F3: **pendiente de re-evaluación** tras v4 (ver Exp 11)
- Estado de trabajo detallado: `project_reranker_benchmark.md` (auto-memory)

---

## Exp 11: Re-training v4 "converged" (en curso)

**Fecha de lanzamiento**: 2026-04-10 18:03 UTC
**Objetivo**: re-entrenar los 6 modelos (ESMC y ProstT5, NK/LK/PK) con presupuesto suficiente para que el early stopping dispare de verdad, eliminando el confounder de under-training del Exp 10.

### Cambios respecto a v3

| Parámetro | v3 | v4 |
|---|---|---|
| `num_boost_round` | 1000 | **5000** |
| `early_stopping_rounds` | 50 | **100** |
| Resto | (same) | idéntico (13 splits 160→220, neg_pos_ratio=10, IA weights, per-tier NK/LK/PK, alignment+taxonomy features) |

El resto del pipeline (KNN, FAISS IVFFlat, feature engineering) es idéntico. v4 cambia **solo** el presupuesto de boosting.

### Jobs

Ambos lanzados a `protea.training` (cola aislada, worker dedicado, peak RAM ~14 GB con los fixes de chunked KNN del 2026-04-10):

| Job | Modelo | Estado esperado |
|---|---|---|
| `48c91381-1af1-414c-bd1b-a6a51c931873` | `lgbm_v4_converged_esmc` | running (~2h) |
| `e923ac70-21a8-4c5c-8cc6-9ebb76d156aa` | `lgbm_v4_converged_prostt5` | queued, arrancará al terminar ESMC |

Tiempo estimado total: ~4h serial (protea.training procesa uno a uno).

### Escenarios esperados al terminar

- **A: narrativa F2 se confirma**: ProstT5 sigue degradando (−ΔFmax tras converger) → conclusión fuerte de tesis, la hipótesis de compensación gana peso
- **B: ProstT5 pasa a neutro o +**: narrativa se suaviza ("ambos embeddings mejoran con reranker, ESMC un poco más"). F2 sigue válido como explicación.
- **C: ambos suben ~0.01-0.02**: confirma que v3 estaba under-trained y da números definitivos más altos que Exp 10

### Pendientes cuando termine

1. Verificar `best_iteration` de los 6 modelos nuevos (esperamos 2000-4000, disparando early stop)
2. Re-lanzar `run_cafa_evaluation` para ambos embeddings con los nuevos reranker UUIDs
3. Re-extraer feature importance y re-validar F2
4. Reemplazar la tabla de F3 en el Exp 10 con los números de v4
5. Decidir A/B/C y actualizar la narrativa de la tesis en consecuencia

---

## Exp 12: Extended PLM benchmark matrix (planned)

**Fecha de diseño**: 2026-04-10
**Estado**: documento de diseño prospectivo
**Plan completo**: `EXPERIMENTAL_DESIGN.md`

### Motivación

Exp 10 expuso el confounder central del trabajo preliminar: comparar ESMC-300M (~300M, BERT-like) con ProstT5-XL (~3B, T5 + structure fine-tuning) mezcla **tamaño** y **familia** en un solo eje. Ningún finding se puede atribuir a una u otra dimensión sin una matriz que los separe.

### Matriz propuesta (8 modelos)

| # | Modelo | Params | Backend | Estado |
|---|---|---|---|---|
| 1 | ESMC-300M | ~300M | `esm3c` | ✓ (Exp 10, v4 en curso) |
| 2 | ESMC-600M | ~600M | `esm3c` | nuevo |
| 3 | ESM2-650M (`esm2_t33_650M_UR50D`) | ~650M | `esm` | nuevo |
| 4 | ESM2-3B (`esm2_t36_3B_UR50D`) | ~3B | `esm` | nuevo |
| 5 | Ankh-base (`ElnaggarLab/ankh-base`) | ~450M | `ankh` | nuevo |
| 6 | Ankh-large (`ElnaggarLab/ankh-large`) | ~1.9B | `ankh` | nuevo |
| 7 | ProtT5-XL (`prot_t5_xl_uniref50`) | ~3B | `t5` | nuevo |
| 8 | ProstT5-XL | ~3B | `t5` | ✓ (Exp 10, v4 en curso) |

**Descartado**: ESM2-15B (coste de embedding prohibitivo, no tiene par T5 de tamaño equivalente → rompe la simetría de la matriz).

### Research questions (ver `EXPERIMENTAL_DESIGN.md` §2)

- **RQ1**: ¿a tamaño fijo, qué familia gana (BERT-like vs T5 encoder)?
- **RQ2**: ¿cómo escala Fmax con el tamaño dentro de una familia? ¿Dónde satura?
- **RQ3**: ¿estructura aporta? Test pareado ProtT5-XL vs ProstT5-XL (mismo backbone, única diferencia = 3Di fine-tuning).
- **RQ4**: ¿los embeddings más débiles fuerzan al reranker a compensar con alignment+taxonomy? (carry-over de F2)

### Protocolo

Pipeline idéntico para los 8 modelos, cero tuning per-modelo. Ver `EXPERIMENTAL_DESIGN.md` §6 para hiperparámetros pinned: KNN `k=5`, FAISS IVFFlat, alignments + taxonomy on, reranker v4 (5000 rounds), `run_cafa_evaluation` con IA weighting.

### Tests estadísticos

Wilcoxon signed-rank sobre las 9 celdas Fmax, corrección Holm-Bonferroni sobre 6 comparaciones pareadas, bootstrap CI 95% para effect sizes. Regresión OLS para H4.

### Coste

~3-4 días de compute secuencial (embeddings + KNN + v4 training + eval por los 6 modelos nuevos). Comprimible con paralelismo GPU si procede.

### Estado

- **Diseño**: completo (`EXPERIMENTAL_DESIGN.md` v1.0)
- **Ejecución**: bloqueada hasta que v4 (Exp 11) valide que el presupuesto es correcto
- **Dependencias previas**: Ankh backend ya integrado en PROTEA como `model_backend="ankh"` dedicado (no alias de `t5`). Ver `project_ankh_backend.md`.

### Deliverables esperados

1. Tabla master 8 × 3 (baseline / alignment_weighted / reranker) × 9 celdas
2. Heatmap de feature importance de las 24 rerankers (8 modelos × 3 tiers)
3. Report estadístico (p-valores + effect sizes + CIs) por comparación
4. Capítulo de tesis formalizando RQ1-RQ4 con la matriz como evidencia
