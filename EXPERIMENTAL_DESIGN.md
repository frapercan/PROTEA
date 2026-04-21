# PROTEA — Experimental Design

**Version**: 1.0 — 2026-04-10
**Status**: Active
**Scope**: Protein language model (PLM) benchmark for GO term prediction via KNN + learned reranking

> This document is **prospective**: it formalises the protocol, hypotheses, and execution plan for the extended PLM comparison. Retrospective results (finished experiments, ablations, external tool comparisons) live in `EXPERIMENTS.md`. The reranker design rationale lives in `RERANKER.md`.

---

## 1. Motivation

The preliminary comparison in `EXPERIMENTS.md` (ESMC-300M vs ProstT5-XL) **confounds two independent variables**: model family and parameter count. ESMC-300M is a ~300M-parameter BERT-like encoder; ProstT5-XL is a ~3B-parameter T5 encoder with structural fine-tuning. Any observed difference in downstream F<sub>max</sub> cannot be attributed to either axis unambiguously.

This document defines the extended benchmark that disentangles those factors and integrates additional PLMs (Ankh, ESM2, ESMC-600M, ProtT5-XL) into a single, statistically comparable grid under an identical downstream pipeline.

---

## 2. Research questions

| ID | Question |
|---|---|
| **RQ1** | At matched parameter count, does a BERT-like encoder (ESM2, ESMC) outperform a T5 encoder (ProtT5, Ankh) for GO term transfer via KNN? |
| **RQ2** | Holding model family fixed, how does F<sub>max</sub> scale with parameter count? Where does the curve saturate? |
| **RQ3** | Does structure-aware fine-tuning (ProstT5) yield a measurable F<sub>max</sub> improvement over its pure-sequence parent (ProtT5-XL) at identical size? |
| **RQ4** | Does the learned reranker compensate for weaker embeddings by placing more weight on alignment and taxonomy features? Is there a systematic inverse relationship between embedding quality and reranker feature-importance on these compensatory signals? |

---

## 3. Hypotheses (pre-registered)

| # | Hypothesis | Primary test |
|---|---|---|
| **H1** | At small scale (~300–650M), family effect dominates scale effect (ΔF<sub>max</sub> across families ≥ ΔF<sub>max</sub> across sizes within a family) | Wilcoxon signed-rank across 9-cell F<sub>max</sub> vectors, pairwise within the small tier |
| **H2** | Scale gains within a single family saturate in the 1–3B range | Monotonicity of F<sub>max</sub> across {ESM2-650M, ESM2-3B} and {Ankh-base, Ankh-large, ProtT5-XL} |
| **H3** | Structure awareness provides a positive but modest gain (+1–3 F<sub>max</sub> points averaged across cells) | Pairwise matched test ProtT5-XL vs ProstT5-XL (same backbone, same size, only fine-tuning differs) |
| **H4** | Reranker gain-based importance on `{alignment_*, similarity_*, taxonomic_*}` features is inversely correlated with the baseline F<sub>max</sub> of the underlying embedding | Linear regression across the 8 models: `weight_on_compensatory` ~ `baseline_Fmax` |

H1–H3 are confirmatory; H4 is exploratory and carries forward the **F2 finding** from the ESMC vs ProstT5 analysis in `project_reranker_benchmark.md`.

---

## 4. Model matrix

**8 models total** (2 already computed, 6 new).

| # | Model | Backbone | Params | PROTEA backend | Status |
|---|---|---|---|---|---|
| 1 | **ESMC-300M** | ESM3c (EvolutionaryScale) | ~300M | `esm3c` | ✓ computed; reranker v4 in progress (`48c91381`) |
| 2 | **ESMC-600M** | ESM3c (EvolutionaryScale) | ~600M | `esm3c` | new |
| 3 | **ESM2-650M** | ESM2 `esm2_t33_650M_UR50D` (Meta) | ~650M | `esm` | new |
| 4 | **ESM2-3B** | ESM2 `esm2_t36_3B_UR50D` (Meta) | ~3B | `esm` | new |
| 5 | **Ankh-base** | Ankh `ElnaggarLab/ankh-base` | ~450M | `ankh` | new |
| 6 | **Ankh-large** | Ankh `ElnaggarLab/ankh-large` | ~1.9B | `ankh` | new |
| 7 | **ProtT5-XL** | ProtT5 `prot_t5_xl_uniref50` (Rostlab) | ~3B | `t5` | new |
| 8 | **ProstT5-XL** | ProstT5 structure-fine-tuned (Rostlab) | ~3B | `t5` | ✓ computed; reranker v4 in progress (`e923ac70`) |

**Discarded**: ESM2-15B (prohibitive embedding cost over 527k sequences; no matched-size T5 counterpart → breaks symmetry of the grid).

### Explanatory grid (for RQ1 / RQ2 / RQ3)

| Scale | BERT-like encoder | T5 encoder (sequence-only) | T5 encoder (structure-aware) |
|---|---|---|---|
| **Small (~300–650M)** | ESMC-300M, ESMC-600M, ESM2-650M | Ankh-base (~450M) | — |
| **Medium (~1–2B)** | — | Ankh-large (~1.9B) | — |
| **Large (~3B)** | ESM2-3B | ProtT5-XL | ProstT5-XL |

### Planned pairwise comparisons

| Pair | Isolates | RQ |
|---|---|---|
| ESMC-300M ↔ Ankh-base | architecture (BERT vs T5), ~matched size | RQ1 |
| ESM2-650M ↔ Ankh-base | architecture, ~matched size | RQ1 |
| ESMC-300M ↔ ESMC-600M | scale, family fixed | RQ2 |
| ESM2-650M ↔ ESM2-3B | scale, family fixed | RQ2 |
| Ankh-base ↔ Ankh-large ↔ ProtT5-XL | scale ladder within T5 encoder family | RQ2 |
| **ProtT5-XL ↔ ProstT5-XL** | structure fine-tuning (cleanest test) | **RQ3** |

---

## 5. Data and splits (fixed across all 8 runs)

Identical to the ESMC/ProstT5 preliminary experiments in `EXPERIMENTS.md` to preserve backward comparability with established findings.

| Item | Value |
|---|---|
| Reference annotation sets | GOA releases 160 → 220 (13 temporal splits for reranker training) |
| Evaluation set | `42b34e79-6fe9-4fa0-b718-02f43a1e3192` (GOA 220 → 229 delta) |
| Evaluation size | 20,281 proteins (NK=2,831; LK=3,410; PK=15,313) |
| Ontology snapshot | `947bdff6-d17c-4ca3-a41a-bc8fb4d74b7a` (GO release 2026-01-23) |
| IA file | `data/benchmarks/IA_cafa6.tsv` (CAFA6 information accretion) |

---

## 6. Pipeline protocol — pinned hyperparameters

Every model is put through the same three-stage pipeline with **identical hyperparameters**. No per-model tuning. Fair comparison requires this invariance.

### 6.1 Embeddings — `compute_embeddings`
- Pooling: `mean` over residue representations
- Precision: fp32 at storage (cast to fp16 at KNN load time via `_REF_CACHE`)
- Storage: pgvector `VECTOR(dim)` per `(sequence, config, chunk)`
- Full reference set (~527k sequences) + evaluation set query embeddings

### 6.2 KNN retrieval — `predict_go_terms`
- `k = 5`
- `metric = cosine`
- `backend = faiss`, `faiss_index_type = IVFFlat`, `nlist = 256`, `nprobe = 32`
- `aspect_separated_knn = true`
- `compute_alignments = true` (NW + SW via parasail/BLOSUM62)
- `compute_taxonomy = true` (NCBI taxonomy LCA via ete3)

### 6.3 Reranker training — `train_reranker_auto` (v4 budget)
- `num_boost_round = 5000`
- `early_stopping_rounds = 100`
- `val_fraction = 0.2`
- `neg_pos_ratio = 10`
- `train_versions = [160, 165, 170, 175, 180, 185, 190, 195, 200, 205, 211, 215, 220]` (13 splits)
- `test_versions = [229]`
- `compute_alignments = true`, `compute_taxonomy = true`
- `ia_file = data/benchmarks/IA_cafa6.tsv` (IA-weighted sample weighting: `sample_weight = IA(go_term)`)
- **3 models per embedding (NK / LK / PK)** — per-category, not per-aspect (justified in `RERANKER.md` §6.3)
- Objective: **binary cross-entropy (LightGBM `objective=binary`)**, early stopping on validation AUC. IA weights enter through `sample_weight`, not through the objective. See `RERANKER.md` §6.1 for rationale and the known limitation that a pairwise/listwise rank loss is future work.
- Name convention: `lgbm_v4_converged_<model_slug>-{nk,lk,pk}`

### 6.4 Evaluation — `run_cafa_evaluation`
- Library: `cafaeval` (integrated via the `run_cafa_evaluation` operation)
- Metric: **F<sub>max</sub> with IA weighting**, computed per (tier × aspect) cell → 9-dimensional output vector per model×pipeline-stage
- Pipeline stages reported: `baseline` (embedding only), `alignment_weighted` (best heuristic from Exp 3), `reranker` (v4 LightGBM)

---

## 7. Statistical protocol

Pre-registered to prevent post-hoc test-shopping.

| Aspect | Method |
|---|---|
| **Primary outcome** | 9-cell F<sub>max</sub> vector per (model, pipeline-stage) |
| **Pairwise test** | Wilcoxon signed-rank over the 9 matched cells |
| **Multiple comparisons** | Holm–Bonferroni correction across the planned comparisons in §4 (6 RQ1/RQ2/RQ3 tests) |
| **Effect size** | Mean F<sub>max</sub> delta ± 95% bootstrap CI (1000 resamples over cells) |
| **H4 regression** | For each (model, tier): `weight_compensatory = Σ importance(feature)` over features in `{alignment_score_*, similarity_*, identity_*, gaps_pct_*, alignment_length_*, taxonomic_*}`. Fit `weight_compensatory ~ baseline_Fmax` across the 8 models via OLS; report slope, p-value, R² |
| **Reporting convention** | All numbers from `cafaeval` with IA weighting. **Never** use the internal `test_evaluation` field from `train_reranker_auto` for thesis claims — it is unweighted and biased (see `project_reranker_benchmark.md`) |

---

## 8. Execution plan

Ordered so each stage produces usable partial results; no stage blocks on the next.

| Step | Action | Depends on | Compute estimate |
|---|---|---|---|
| 1 | Wait for v4 rerankers (ESMC-300M, ProstT5-XL) to finish | running | ~4h total (sequential) |
| 2 | Create 6 `EmbeddingConfig` rows with pinned pooling/precision | — | minutes |
| 3 | Run `compute_embeddings` for the 6 new models over ref+eval sets | step 2 | 2–10h per model; ~1.5–2 days total sequential |
| 4 | Run `predict_go_terms` (with alignments + taxonomy) for the 6 new models | step 3 | 1–2h per model |
| 5 | Run `train_reranker_auto` v4 for the 6 new models in `protea.training` queue | step 4 | 2–4h per model; ~1 day total sequential |
| 6 | Run `run_cafa_evaluation` for all 8 models × 3 stages = 24 evals | step 5 + existing | ~10 min per eval; ~4h total |
| 7 | Extract feature importance from all 24 (model × tier) rerankers | step 5 | minutes (script) |
| 8 | Apply the statistical protocol in §7 to the aggregated results | steps 6–7 | — |
| 9 | Update `EXPERIMENTS.md` with the per-model result tables | step 8 | — |
| 10 | Compile results into thesis chapter / appendix | step 9 | — |

**Total wall-clock (pessimistic, fully serial):** ~3–4 days of compute. Can be compressed with overlapping embedding/training workers if GPU capacity allows.

---

## 9. Deliverables

- `EmbeddingConfig` rows for the 6 new models, committed to the DB.
- Per-model entries in `EXPERIMENTS.md` mirroring the existing table format (Exp 1 / Exp 3 / Exp 4+ rows).
- **Master results table**: 8 rows × (baseline F<sub>max</sub> | alignment_weighted F<sub>max</sub> | reranker F<sub>max</sub>) × 9 cells each.
- **Feature importance heatmap**: 24 (model × tier) rerankers × top-N features, colour-coded by gain.
- Statistical test report (Wilcoxon p-values + effect sizes + CIs) as a standalone markdown section.
- Thesis chapter / appendix formalising the grid as evidence for RQ1–RQ4.

---

## 10. Known limitations (honest reporting)

1. **Not training-data matched.** Each PLM was pretrained on different corpora (UniRef50 subsets at different points in time, sometimes Big Fantastic Database for ProtT5, etc.). Perfect controlled comparison is impossible without re-pretraining, which is out of scope.
2. **Architecture is not a clean isolated variable.** T5 encoders and BERT-style encoders differ in depth, attention masking, objective (span corruption vs MLM), and training data. RQ1's conclusion will be **correlational**, not causal.
3. **Scale is coarse.** Three tiers (~300M / ~1.5B / ~3B) is the maximum granularity this compute budget allows. Smooth scaling curves are out of reach.
4. **Ankh backend.** Ankh is exposed in PROTEA as a **dedicated backend** (`model_backend = "ankh"`), not as an alias of `t5`. Internally it reuses the T5 batched pipeline via `_embed_t5(..., use_aa2fold=False)` but uses `AutoTokenizer` instead of `T5Tokenizer` and never injects the `<AA2fold>` prefix — ensuring clean separation in the benchmark tables. The distinction matters for RQ1: Ankh results are reported under their own family row, not merged into "T5 encoder".
5. **ESMC-600M availability.** EvolutionaryScale's public ESMC release must be confirmed to include the 600M variant at time of execution. If unavailable at that scale, substitute with the closest public ESMC size and document the deviation in step 2.
6. **No seed-variance analysis.** LightGBM training (with fixed seed), KNN retrieval, and embeddings are all deterministic under PROTEA's default config. Variance across re-runs for the same config should be zero; we do not budget compute for confirming this.
7. **Single evaluation delta.** Only the GOA 220 → 229 delta is used. A multi-delta sensitivity analysis (e.g. 215 → 229, 220 → 225) is a candidate for future work but not planned here.
8. **ProstT5 inference requires 3Di tokens**, which PROTEA currently provides via sequence-only input using the AA2fold branch (`use_aa2fold = "prostt5" in model_name.lower()` at `compute_embeddings.py:715`). This means PROTEA's ProstT5 embeddings are generated **without** real 3Di tokens from a structure; the model internally translates sequence to predicted 3Di. This is the setup the Rostlab release supports but is distinct from "true structure-aware" inference with Foldseek-derived 3Di tokens. Document this explicitly in the thesis when discussing RQ3.

---

## 11. Change log

| Date | Change |
|---|---|
| 2026-04-10 | Initial draft: 8-model matrix, RQ1–RQ4, hypotheses H1–H4, pinned pipeline, statistical protocol. ESMC-600M confirmed. ESM2-15B discarded. |
