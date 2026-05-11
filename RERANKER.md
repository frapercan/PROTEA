# PROTEA Re-Ranker: Design and Rationale

**Status**: implemented (v3 shipped, v4 training in progress)
**Location in code**: `protea/core/reranker.py`, `protea/core/operations/train_reranker.py`
**Version**: 2.0 (2026-04-10, rewrite)

> This document describes **the re-ranker as it exists in PROTEA today**. An earlier version of this file proposed a PyTorch cross-attention architecture with WebDataset shards; that proposal was explored on paper but **never implemented**. The system converged on a simpler LightGBM design for the reasons documented in §3 ("Why LightGBM and not a neural cross-encoder"). The experiment log showing the evolution across versions lives in `EXPERIMENTS.md`; the forward-looking PLM benchmark plan that uses this re-ranker as a fixed downstream stage lives in `EXPERIMENTAL_DESIGN.md`.

---

## 1. Problem statement

PROTEA predicts GO terms by transferring annotations from the $k$ nearest reference proteins in an embedding space. The raw retrieval score is a distance-based heuristic (e.g. `1 - cosine_distance / 2`) optionally combined with alignment identity and evidence weights. This heuristic is:

- **Not optimised for F<sub>max</sub> with IA weighting** (the metric CAFA actually uses)
- **Not calibrated across tiers**: No-Knowledge, Limited-Knowledge and Previously-Known proteins behave very differently and benefit from different signal combinations
- **Not able to use all available features**: sequence alignments, taxonomy, neighbour statistics, and evidence codes are either ignored or combined by hand with arbitrary weights

The re-ranker replaces this heuristic with a **learned function** that, for each candidate GO term, produces a probability score used to reorder the top-$k$ retrieval list:

$$f(q, t, \mathcal{N}_K(q)) \to \hat{y} \in [0, 1]$$

where $q$ is the query protein, $t$ is a candidate GO term, and $\mathcal{N}_K(q)$ is the set of $K$ nearest neighbours that voted for $t$.

The training signal is derived from the **temporal structure of GOA releases**: a GO term that first appears for a protein in a later release (and was missing from an earlier one) defines a positive example; any term predicted but absent from the future release is a negative. See §4.

---

## 2. Scope of this document

| Covered | Not covered |
|---|---|
| Model architecture and feature set | Downstream CAFA evaluation protocol (→ `EXPERIMENTAL_DESIGN.md` §7) |
| Training protocol and hyperparameters | PLM comparison across ESMC/ESM2/ProstT5/Ankh (→ `EXPERIMENTAL_DESIGN.md`) |
| Version history and key design decisions | Historical result tables per experiment (→ `EXPERIMENTS.md`) |
| Integration with the PROTEA pipeline | Alternative rankers (cross-attention, ListNet, ProT5 rerankers…) |
| Known limitations | External tool baselines (eggNOG, Pannzer2, InterProScan) |

---

## 3. Why LightGBM and not a neural cross-encoder

The original design (see §11 for the earlier version's record) proposed a cross-attention neural re-ranker in PyTorch, with learned GO term embeddings from the GO DAG and a WebDataset sharded data pipeline. That proposal was abandoned in favour of a LightGBM gradient-boosted tree model for four concrete reasons:

1. **Data volume is moderate, not huge.** Each temporal split yields 80k–330k training rows after negative subsampling. Gradient boosted trees are the sample-efficient sweet spot for this regime; a cross-attention transformer would either overfit or need heavy regularisation and we would then be tuning architecture choices instead of studying the actual research question.
2. **Feature heterogeneity is the bottleneck, not representation.** The informative features are already engineered (alignment scores, taxonomy distance, neighbour statistics). A model whose job is to combine 23 tabular features non-linearly across categorical and numeric axes is exactly what GBDT excels at. A neural cross-encoder would need to learn an equivalent combination from scratch.
3. **Interpretability is a thesis requirement.** The F2 finding (that smaller PLMs force the re-ranker to rely more on alignment/taxonomy) can only be measured through gain-based feature importance. LightGBM exposes this directly; extracting equivalent attributions from a cross-attention model requires additional machinery (integrated gradients, attention rollout) that adds failure modes.
4. **Training cost was a hard constraint.** Each re-ranker (per-tier × per-embedding) trains in 2–4 hours on CPU. The same pipeline under a neural cross-encoder with the same budget would train a single model for similar time on a GPU while blocking the embedding worker. Since the PLM benchmark (`EXPERIMENTAL_DESIGN.md`) multiplies compute cost by 8, the LightGBM choice is what makes the study feasible on a single workstation.

The cross-attention design was not a wrong idea, only a wrong fit for this problem at this scale. Revisiting it remains an option if a later phase of the work finds a measurable ceiling on LightGBM.

---

## 4. Temporal holdout training signal

Let $\mathcal{G}_N$ denote the set of GO annotations present in GOA release $N$ (Swiss-Prot reviewed, evidence-filtered to exclude IEA if so configured). For any ordered pair of releases $(N, N+1)$, the **annotation delta** is

$$\Delta_{N \to N+1} = \{(p, t) \mid (p, t) \in \mathcal{G}_{N+1} \setminus \mathcal{G}_N\}$$

For a training pair $(N, N+1)$:

1. All proteins in $\mathcal{G}_{N+1}$ are used as queries.
2. KNN retrieval is performed using **only** the reference set derived from $\mathcal{G}_N$ (no leakage from the future).
3. For each candidate $(q, t)$ in the retrieval output:
   - **Positive** ($y = 1$) if $(q, t) \in \Delta_{N \to N+1}$ (the annotation materialised between $N$ and $N+1$)
   - **Negative** ($y = 0$) if the model predicted $t$ but $(q, t) \notin \mathcal{G}_{N+1}$

This definition ensures the training labels are **causally prior** to the prediction: at time $N$ the system does not know what $N+1$ will contain, and neither does the re-ranker while scoring.

The test split $(220 \to 229)$ is never seen during training and produces the F<sub>max</sub> numbers that are reported for the thesis.

---

## 5. Feature set (implementation: `protea/core/reranker.py`)

Each (query, candidate GO term, contributing neighbour) triple is characterised by **23 features** (20 numeric and 3 categorical), computed at KNN time and persisted on `GOPrediction` rows.

### 5.1 Numeric features (20)

| Group | Feature | Origin |
|---|---|---|
| **Embedding retrieval** | `distance` | cosine distance between query and the contributing neighbour |
| **NW alignment** | `identity_nw`, `similarity_nw`, `alignment_score_nw`, `gaps_pct_nw`, `alignment_length_nw` | Needleman–Wunsch via parasail (BLOSUM62), computed per (query, neighbour) pair when `compute_alignments=True` |
| **SW alignment** | `identity_sw`, `similarity_sw`, `alignment_score_sw`, `gaps_pct_sw`, `alignment_length_sw` | Smith–Waterman via parasail (BLOSUM62), same condition |
| **Sequence length** | `length_query`, `length_ref` | Raw sequence lengths |
| **Taxonomy** | `taxonomic_distance`, `taxonomic_common_ancestors` | NCBI taxonomy LCA via ete3 when `compute_taxonomy=True` |
| **Neighbour aggregation** | `vote_count` | Number of neighbours in the top-$k$ that voted for the same GO term |
| | `k_position` | Rank (0-indexed) of the closest neighbour that supported the term |
| | `go_term_frequency` | Global frequency of the term in the reference annotation set |
| | `ref_annotation_density` | Number of distinct GO terms annotating the reference protein |
| | `neighbor_distance_std` | Standard deviation of distances across the $k$ neighbours of the query |

### 5.2 Categorical features (3)

| Feature | Meaning |
|---|---|
| `qualifier` | GAF qualifier of the source annotation (`enables`, `involved_in`, etc.) |
| `evidence_code` | GAF evidence code of the source annotation (`EXP`, `IDA`, `IEA`, …) |
| `taxonomic_relation` | Discrete label derived from the LCA (`same_species`, `same_genus`, `same_family`, `distant`) |

Categoricals are passed to LightGBM via its native `categorical_feature` handling (no one-hot encoding; LightGBM partitions on category sets directly).

### 5.3 Missing-value convention

- Numeric missing values are left as `NaN` and handled natively by LightGBM's missing-value-aware splits.
- Categorical missing values are coerced to `NA` and treated as a distinct bin.
- Alignment and taxonomy columns are only populated when `compute_alignments=True` / `compute_taxonomy=True` at prediction time. If either flag is off, those columns are all-NaN for the run and the re-ranker still trains but with a degraded feature set.

---

## 6. Model and training protocol

### 6.1 Model

- **Library**: LightGBM (`lightgbm.Booster`)
- **Objective**: `binary` (binary cross-entropy / log loss)
- **Validation metric**: `binary_logloss` and `auc` (early stopping is tracked on AUC)
- **Boosting**: `gbdt` with `num_leaves=31`, `learning_rate=0.01`, `feature_fraction=0.8`, `bagging_fraction=0.8`, `bagging_freq=5`, `seed=42`
- **Early stopping**: disabled via callback only if `early_stopping_rounds=0`; otherwise stops when validation AUC does not improve for the configured number of rounds

> **Note on the objective.** Earlier drafts of this document (and informal notes) described the loss as **LambdaRank**. The implementation is actually **binary cross-entropy**. Switching to a pairwise/listwise rank loss is a known avenue for future work; it was deferred because (a) binary CE is the simpler baseline and has already matched or beaten the heuristic `alignment_weighted` scoring and (b) LambdaRank would require restructuring the training data into query groups, which complicates the per-split sampling pipeline.

### 6.2 Split strategy

- **Stratified train/val split** at `val_fraction=0.2`, stratified on the label (the positive rate is 0.17%–5% depending on tier × aspect; naive random splits would under-represent positives in the validation set).
- **Negative subsampling** via `neg_pos_ratio=10`: after splitting, each of the train and val sets is independently subsampled so that `|negatives| ≤ 10 × |positives|`. Without this step, 6 of 9 per-(tier, aspect) models in v1 failed to learn at all, because the positive rate was too low for gradient boosted trees to see a signal.
- **IA sample weighting**: when an information accretion file is provided, each row's `sample_weight` is set to `IA(go_term)`. This makes the model focus on informative (rare, specific) GO terms, the same aspect of the term that CAFA evaluation rewards via IA-weighted F<sub>max</sub>.

### 6.3 Per-tier, not per-aspect

One model is trained **per tier** (`NK`, `LK`, `PK`), not per (tier × aspect). This was an explicit change in v2 after v1 trained 9 models (one per cell) and 6 of them either never converged or overfit on the smaller aspect slices. Aspect identity is not currently used as a feature; this is a known simplification (see §9).

### 6.4 Temporal splits

- **Training pairs**: 13 consecutive deltas from GOA 160 through GOA 220: `[(160,165), (165,170), (170,175), (175,180), (180,185), (185,190), (190,195), (195,200), (200,205), (205,211), (211,215), (215,220)]`. The training rows from all pairs are concatenated and passed to LightGBM as a single dataset. Pair identity is not used as a feature.
- **Test pair**: `(220, 229)`, never seen during training. The test set is passed through the trained reranker and fed to `run_cafa_evaluation` alongside the baseline to measure the lift.

### 6.5 Budget

| Version | `num_boost_round` | `early_stopping_rounds` | Comment |
|---|---|---|---|
| v1 | 300 | 50 | 6/9 models hit iter=1 (early stop on first round): under-trained, unbalanced |
| v2 | 1000 | 50 | Stable; per-tier models; IA weighting introduced |
| v3 | 1000 | 50 | Same budget; alignment + taxonomy features fully populated in training (were NULL in v2) |
| v4 | **5000** | **100** | In progress 2026-04-10: all 6 v3 models hit `best_iteration ≈ 1000`, implying they never converged under the previous budget. v4 restores early stopping as a convergence criterion, not a time-out. |

---

## 7. Integration with the PROTEA pipeline

### 7.1 ORM and persistence

- **`Reranker` row** (table: `rerankers`): stores the trained LightGBM booster serialised as bytes alongside training metadata (`feature_importance`, `val_auc`, `best_iteration`, `train_samples`, hyperparameters, parent `job_id`).
- **`RerankerTrainingJob`** row captures the auto-pipeline metadata (splits used, features computed, per-tier model IDs).

### 7.2 Scoring router

The `scoring` router exposes endpoints to list and inspect rerankers:
- `GET /scoring/rerankers`: list trained rerankers
- `GET /scoring/rerankers/{id}`: metadata + feature importance

### 7.3 Applying the re-ranker at evaluation time

At evaluation time (`run_cafa_evaluation`), the caller supplies a `rerankers` mapping that selects a re-ranker per tier:

```json
{
  "rerankers": {
    "nk": {"reranker_id": "2ff1818f-71b6-4932-8f8d-b3000e3c8d34"},
    "lk": {"reranker_id": "269e26b4-0bec-42fa-a077-fe5b675dd2de"},
    "pk": {"reranker_id": "e14b9716-bbf8-4b99-b34b-b801c3966579"}
  }
}
```

The evaluation operation:
1. Streams predictions from the target `PredictionSet` tier by tier.
2. For each tier, loads the corresponding booster, applies it to the feature matrix, and overrides the original `score` with the re-ranked probability.
3. Feeds the re-ranked predictions to `cafaeval` with IA weighting and emits per-cell F<sub>max</sub>.

The raw `PredictionSet` is never mutated: the re-ranker only changes the `score` column as the rows pass through evaluation. This means a single prediction set can be evaluated under multiple re-rankers (ESMC, ProstT5, v3, v4, ...) without duplicating storage.

### 7.4 `train_reranker_auto` operation

The operation `train_reranker_auto` orchestrates the full pipeline end-to-end:

1. For each training pair, runs KNN retrieval (FAISS IVFFlat by default) with `compute_alignments=True`, `compute_taxonomy=True`.
2. Writes per-pair parquet files into a temporary directory.
3. Loads the concatenation into memory, applies per-tier splits, trains three LightGBM boosters.
4. Persists the three boosters as `Reranker` rows under a common base name.
5. Optionally runs a self-evaluation on the held-out test split (see warning in §8).
6. **Cleans up the temporary parquet files** on exit (`shutil.rmtree(tmp_dir)` at `train_reranker.py:1480`).

The cleanup in step 6 has an important consequence: **re-training only the LightGBM stage is not possible** after a pipeline run. A re-train requires re-executing the full KNN + feature engineering path. This is why each v-version re-train takes hours, not minutes.

---

## 8. Known limitations and caveats

1. **`test_evaluation` is not comparable to `cafaeval`.** The operation optionally runs an internal test evaluator against the held-out split. That evaluator does not apply GO propagation, does not apply IA weighting, and uses a naive macro-Fmax that inflates improvements by +0.04 to +0.08 over what `cafaeval` actually reports. **It must not be used in thesis claims.** Only `run_cafa_evaluation` with IA and GO propagation produces numbers that belong in the thesis.
2. **Binary objective is a proxy for ranking.** Binary cross-entropy optimises pointwise calibration, not ranking quality. This is the single largest known gap between the current implementation and the ideal model for F<sub>max</sub>. Replacing it with LambdaRank (or a listwise objective) is the first item on the "future work" list.
3. **Parquet staging files are ephemeral.** The KNN + feature engineering output is thrown away at the end of a training run, so the LightGBM stage cannot be iterated independently. Persisting the staging parquet (behind a flag) would allow rapid hyperparameter sweeps. Open question: is the additional disk cost (10–20 GB per run) worth it?
4. **No aspect feature.** Aspect is not used as a feature, even though BPO/MFO/CCO have very different annotation densities and the same term can behave differently across aspects. A per-tier model averages across aspects and may under-perform in MFO vs BPO.
5. **No uncertainty output.** The re-ranker emits a point probability. Downstream evaluation is sensitive to calibration, but calibration is not currently measured. A reliability diagram per tier would help diagnose whether the probabilities are meaningful or only usable for ranking.
6. **Under-training of v1–v3.** All six v3 models (ESMC and ProstT5, NK/LK/PK) hit `best_iteration ≈ 1000` at the previous budget, which indicates the models never satisfied the early stopping criterion. The F<sub>max</sub> deltas derived from v3 must be treated as provisional until v4 completes. See `project_reranker_benchmark.md` for the full story.
7. **Temporal label noise.** Some annotations in $\Delta_{N \to N+1}$ are not genuinely "new biology"; they are curation catch-ups. There is no filter for this, so the training label includes noise. Evidence code filtering removes the worst offenders (IEA) but not all.
8. **Single embedding at a time.** The re-ranker is trained on features derived from one embedding configuration. There is no multi-embedding ensemble; comparing ESMC, ProstT5 and Ankh means training three independent re-rankers, which is exactly what the benchmark in `EXPERIMENTAL_DESIGN.md` does.

---

## 9. Version history

| Version | Date | Change | Outcome |
|---|---|---|---|
| v1 (unbalanced) | 2026-03-22 | First working pipeline: 9 per-(tier, aspect) models, binary CE, 300 rounds, no sample weights, no negative subsampling | 6/9 models never learned (positive rate too low); CCO/MFO noisy |
| v1 (balanced) | 2026-03-22 | Added `neg_pos_ratio=10`; same 9 models | All models learned; BPO recovered; MFO degraded vs heuristic |
| v2 | 2026-03-23 | Collapsed to 3 per-tier models (NK/LK/PK); added IA sample weighting; raised `num_boost_round` to 1000 | Robust; matched the heuristic `alignment_weighted` in most cells but did not beat it |
| v3 | 2026-03-23 | Populated alignment + taxonomy features during training (were NULL in v2) | First version to beat `alignment_weighted` in 7/9 cells for ESMC-300M |
| v3 ProstT5 | 2026-04-10 | Same v3 protocol, run on ProstT5-XL embeddings for cross-embedding comparison | Yielded the F1/F2/F3 findings in `project_reranker_benchmark.md`; exposed the under-training in v3 |
| v4 (in progress) | 2026-04-10 | Raised `num_boost_round` to 5000 and `early_stopping_rounds` to 100; same features, same splits | In training for both ESMC-300M and ProstT5-XL (jobs `48c91381`, `e923ac70`); meant to provide the converged reference numbers |

Concrete reranker UUIDs for the v3 and v4 runs live in `project_reranker_benchmark.md` and will be mirrored into `EXPERIMENTS.md` once v4 completes.

---

## 10. Forward pointers

- **`EXPERIMENTS.md`**: per-experiment tables, external tool comparisons, day-to-day lab notebook.
- **`EXPERIMENTAL_DESIGN.md`**: the prospective 8-model PLM comparison that uses this re-ranker as a fixed downstream stage.
- **`project_reranker_benchmark.md`** (in auto-memory): volatile working state for the ongoing benchmark.
- **Code**: `protea/core/reranker.py` (feature definitions, `train`, `predict_scores`), `protea/core/operations/train_reranker.py` (both `TrainRerankerPayload` and `TrainRerankerAutoPayload`, the full pipeline).

---

## 11. Historical note: why this file was rewritten

The previous version of `RERANKER.md` (removed 2026-04-10) proposed a PyTorch cross-attention re-ranker over ESM embeddings with WebDataset sharded I/O, Node2Vec GO term embeddings, wandb tracking, and a nine-cell (tier × aspect) ablation matrix. That design was never built. The system that actually exists and produces the benchmark numbers in `EXPERIMENTS.md` is the LightGBM pipeline documented above. Keeping the two in sync was causing confusion when referring back to the design doc during thesis writing, so the document was rewritten from the current source of truth (`protea/core/reranker.py`) rather than from the original proposal. The historical proposal is preserved in git history for reference.
