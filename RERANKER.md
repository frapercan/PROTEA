# Temporal Holdout Re-Ranker for GO Term Prediction

## Motivación

El pipeline actual de PROTEA transfiere anotaciones GO mediante KNN sobre embeddings ESM, usando un scoring heurístico que combina distancia de embedding y pesos de evidencia. Este scoring no está optimizado para la métrica objetivo (Fmax) ni para el comportamiento real de las anotaciones GO a lo largo del tiempo.

La hipótesis central es que existe una señal aprendible: **dado el contexto de una predicción KNN, ¿acabará este GO term apareciendo en el siguiente release de GOA para esta proteína?** Esta señal puede extraerse directamente del mecanismo de holdout temporal que ya implementa PROTEA.

---

## Formulación del Problema

Sea $\mathcal{G}_N$ el conjunto de anotaciones GO en el release $N$ de GOA (Swiss-Prot reviewed). Para cada par consecutivo $(G_N, G_{N+1})$, el delta temporal es:

$$\Delta_{N \to N+1} = \{(p, t) \mid (p, t) \in \mathcal{G}_{N+1} \setminus \mathcal{G}_N\}$$

El re-ranker aprende una función:

$$f(q, t, \mathcal{N}_K(q)) \to \hat{y} \in [0, 1]$$

donde:
- $q$ es la proteína query (representada por su embedding ESM)
- $t$ es el GO term candidato
- $\mathcal{N}_K(q)$ es el conjunto de $K$ vecinos más cercanos en el espacio de embeddings con referencia $\mathcal{G}_N$
- $\hat{y}$ es la probabilidad de que $(q, t) \in \Delta_{N \to N+1}$

---

## Protocolo de Entrenamiento

Se utiliza validación cruzada temporal con múltiples splits históricos de GOA:

```
Training splits:
  GOA_190 → GOA_195
  GOA_195 → GOA_200
  GOA_200 → GOA_205
  GOA_205 → GOA_211
  GOA_211 → GOA_215
  GOA_215 → GOA_220

Test split (holdout estricto, nunca visto durante training):
  GOA_220 → GOA_229
```

Para cada split se generan ejemplos etiquetados: positivos $(y=1)$ si el par (proteína, GO term) aparece en el delta, negativos $(y=0)$ en caso contrario. El desbalanceo esperado es aproximadamente 1:10, manejable con técnicas estándar.

---

## Arquitectura: Cross-Attention Re-Ranker

El modelo procesa cada par (query, GO term) usando el contexto completo de los vecinos KNN que contribuyeron a esa predicción.

```
Inputs por predicción (query_protein, go_term):
  query_embedding       float32[D]       ESM embedding del query (D=480 para esmc_300m)
  neighbor_embeddings   float32[K × D]   ESM embeddings de los K vecinos contribuyentes
  tabular_features      float32[K × F]   distancia, evidencia, alineamiento, taxonomía...
  go_term_embedding     float32[G]       embedding semántico del GO term (G=64)

Arquitectura:
  1. query_proj(query_embedding)          →  q        [H=256]
  2. ref_proj(neighbor_embeddings)        →  tokens   [K × H]
  3. feature_encoder(tabular_features)   →  (sumado a tokens)
  4. CrossAttention(q, tokens, tokens)   →  context  [H]
  5. MLP([q ‖ context ‖ go_emb ‖ agg_features])  →  score  [1]
```

La atención cruzada permite al modelo aprender **qué vecinos son más informativos para este query concreto**, en lugar de agregar los scores de forma heurística.

### GO Term Embeddings

Los embeddings de los GO terms se aprenden a partir de la estructura del DAG de GO (relaciones `is_a` / `part_of`) mediante Node2Vec o TransE, de forma que términos semánticamente relacionados (padre-hijo) tengan representaciones similares. El DAG ya está disponible en PROTEA a través de los modelos `GOTerm` y `GOTermRelationship`.

---

## Feature Vector

Cada predicción (query, GO term) se caracteriza por las siguientes features tabulares, computadas por vecino que contribuyó a la predicción:

| Feature | Descripción | Estado |
|---|---|---|
| `distance` | Distancia coseno en espacio de embeddings | Existente |
| `evidence_weight` | Peso del código de evidencia (IDA > IEA) | Existente |
| `identity_nw / sw` | Identidad de secuencia (alineamiento NW/SW) | Existente (opcional) |
| `similarity_nw / sw` | Similaridad de secuencia | Existente (opcional) |
| `taxonomic_distance` | Distancia taxonómica entre query y referencia | Existente (opcional) |
| `vote_count` | Número de vecinos que coinciden en este GO term | **Nuevo** |
| `k_position` | Posición del vecino más cercano que predijo este término | **Nuevo** |
| `go_term_frequency` | Frecuencia del término en el annotation set de referencia | **Nuevo** |
| `ref_annotation_density` | Número de GO terms de la proteína de referencia | **Nuevo** |
| `neighbor_distance_std` | Varianza de distancias a los K vecinos | **Nuevo** |

---

## Función de Pérdida

Se utiliza **LambdaRank** en lugar de binary cross-entropy, ya que optimiza directamente el orden de las predicciones (proxy de NDCG / Fmax) en lugar de la calibración de probabilidades.

Para cada proteína query, las predicciones GO se rankean conjuntamente:
- Positivos: GO terms en $\Delta_{N \to N+1}$
- Negativos: GO terms predichos pero no en el delta

---

## Pipeline de Datos: WebDataset

El volumen de datos (múltiples splits × ~1.35M predicciones por split × embeddings de 480 dim) requiere un pipeline de datos eficiente. Se propone almacenar los ejemplos de entrenamiento en formato **WebDataset** (shards tar), con un shard por split GOA:

```
reranker_data/
  splits/
    goa190_to_195.tar       # ~2GB por shard
    goa195_to_200.tar
    ...
    goa220_to_229.tar       # test split — no tocar durante training
  models/
    reranker_v1.pt
    reranker_v1_config.json
```

Cada muestra en el WebDataset es **una proteína query** con todas sus predicciones GO para ese split:

```python
{
    "query_accession": "P12345",
    "query_embedding": float32[480],
    "go_term_ids": ["GO:0006915", "GO:0005737", ...],   # N_preds
    "neighbor_embeddings": float32[N_preds, K, 480],
    "tabular_features": float32[N_preds, K, F],
    "labels": int8[N_preds],                             # 1 si en delta, 0 si no
}
```

El streaming de WebDataset permite entrenar sin cargar todo en RAM.

---

## Stack Tecnológico

| Componente | Tecnología |
|---|---|
| Modelo | PyTorch |
| Data pipeline | WebDataset + torch.utils.data |
| Baseline comparación | LightGBM (binary + LambdaRank) |
| GO embeddings | Node2Vec / PyTorch Geometric |
| Seguimiento experimentos | wandb |
| Embeddings proteína | ESM2 / ESMC (ya en PROTEA) |

---

## Integración en PROTEA

Una vez entrenado, el re-ranker se integra en el pipeline existente:

1. Nuevo modelo ORM `RerankingModel`: almacena pesos serializados y metadata de entrenamiento
2. Campo `reranker_id` (nullable) en `PredictionSet`
3. Si `reranker_id` presente: `store_predictions` aplica el modelo y sobreescribe `score` con $\hat{y}$
4. El threshold de Fmax se calcula igual que ahora sobre los nuevos scores
5. UI: selector de re-ranker en la pantalla de predicción

---

## Experimentos y Ablaciones

El diseño permite comparar directamente:

| Configuración | Descripción |
|---|---|
| **Baseline** | KNN + scoring heurístico actual |
| **LightGBM tabular** | Re-ranker con features tabulares sin embeddings |
| **LightGBM + derived** | Features tabulares + features derivadas del embedding (density, std) |
| **MLP cross-encoder** | Arquitectura completa sin cross-attention |
| **Cross-attention (propuesto)** | Arquitectura completa |
| **+ GO DAG embeddings** | Ablación: ¿aportan los go_term_emb? |
| **+ temporal CV** | Ablación: ¿mejora añadir más splits históricos? |

La métrica principal es **Fmax promedio sobre los 9 settings** (NK/LK/PK × BPO/MFO/CCO) en el test split GOA220→229.

---

## Valor para la Tesis

1. **Científicamente honesto**: el mismo mecanismo temporal que se usa para evaluar se usa para entrenar. No hay data leakage.
2. **Comprobable y cuantificable**: Fmax(baseline KNN) vs Fmax(re-ranker) en benchmark idéntico.
3. **Interpretable**: las feature importances (LightGBM) o los pesos de atención (cross-attention) revelan qué aspectos de una predicción KNN son más predictivos de anotaciones futuras.
4. **Generalizable**: el re-ranker aprende sobre distribuciones temporales de anotaciones GO, no sobre una proteína concreta — debería generalizar a proteínas no vistas.
5. **Extensible**: la arquitectura admite incorporar embeddings de secuencia de mayor calidad (ESM3, ProstT5) sin cambiar el pipeline.
