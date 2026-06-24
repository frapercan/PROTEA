# protea-knn-v1 (LAFA Method Card)

LAFA v2 submission #1 from PROTEA. KNN baseline: ProtT5 PLM, cosine
distance, per-aspect K-nearest-neighbour transfer of GO annotations,
plus a self-prior (each target's own t0 non-experimental annotations
injected as a prior), no learned reranker. Companion submissions `protea-knn-8plm`
(ensemble of eight PLMs) and `protea-v18` (full pipeline with v6
features and per-aspect LightGBM rerank) follow in the same v2 cycle.

Draft for the LAFA evaluator (recipient: An Phan,
`anphan0828/LAFA_container_guide`). The author will deliver this card
together with the container reference; this file is the source of
truth in-tree.

## Method overview

For each query protein the container does the following:

1. Embed the query with **ProtT5-XL UniRef50** (mean pool over the
   residue axis, single ~1024-dim vector).
2. Partition the GO ontology by aspect (MFO, BPO, CCO).
3. For each aspect, run an independent cosine-KNN search (K=5 by
   default) against the frozen reference embeddings. Aspect-separated
   transfer recovers the BPO recall ceiling that a unified search
   produces on the same bank.
4. Transfer the GO annotations of the K neighbours to the query.
   Each candidate GO term inherits a vote count plus minimum and
   mean cosine distance to its contributing neighbours.
5. Score each candidate as `max(0, 1 - min_distance)` and emit
   `(query_accession, go_id, score)` rows.

The v1 baseline omits the v6 feature pass (Anc2Vec centroids, PCA
projection, taxonomy voters) and the LightGBM reranker. These layers
ship in `protea-v18`, the third F-LAFA v2 submission.

## Training data

| Source | Cutoff | Volume |
|---|---|---|
| UniProt sequences | release `2025_02` | ~574k canonical accessions in the reference bank |
| GO annotations | GOA UniProt v226 (2025-05-03) | annotations strictly before LAFA t0 (v227, 2025-09-04) |
| GO ontology | OBO release 2026-01-23 | superset of v226's GO terms |

Training cutoff aligned with LAFA t0: the GAF used for reference
annotations is the latest release strictly before the evaluation
window opens.

## External resources at inference

- **ProtT5-XL UniRef50** (`Rostlab/prot_t5_xl_uniref50`): pulled from
  HuggingFace Hub on first use, cached on a host-mounted HF cache
  directory (`/hf-cache`). No model weights are baked into the image.
- **No internet access** required at inference time once the model
  weights and the frozen-data bundle are present on disk.

## Container

| Field | Value |
|---|---|
| Image | `ghcr.io/frapercan/protea/knn-v1:<release tag>` |
| Base image | `ghcr.io/frapercan/protea/method-runtime:<release tag>` (ADR-D15) |
| Source | <https://github.com/frapercan/PROTEA>, path `apps/lafa_knn_v1/` |
| License | MIT |

The image follows the LAFA container guide bind-mount layout:

```
/bundle               frozen reference bundle (read-only)
/input/queries.fasta  FASTA of query proteins (read-only)
/output/              writable dir for predictions.tsv
/hf-cache             HuggingFace cache for ProtT5 weights
```

Default entrypoint runs `protea-predict --aspect_separated --no_v6
--no_reranker --self_prior` against the bind-mounted bundle (this is the
no-booster path; when a universal booster ships in the bundle the
entrypoint runs `--universal_reranker --self_prior` instead). Extra
positional args are forwarded to `protea-predict`, so the evaluator can
sweep K or distance metrics without rebuilding (the baseline flags are
pinned and cannot be undone from the command line).

## Frozen-data bundle

The container expects a bind-mounted directory at `/bundle`. Layout
matches `apps/method_runtime/README.md` ("Bundle layout") and the v1
LAFA wrapper. For protea-knn-v1 only the following files are
consulted:

```
<bundle>/
├── manifest.json                 # cutoff version + schema sha
├── reference_embeddings.parquet  # 574k canonical accessions x ProtT5 dim
├── reference_annotations.parquet # GOA v226 rows rolled up to canonical
└── go_term_metadata.parquet      # OBO 2026-01-23 (id, go_id, aspect)
```

`pca_state.npz`, `anc2vec.npz`, and `reranker/` are ignored by the
v1 baseline (the entrypoint pins `--no_v6 --no_reranker`); the
`--self_prior` term draws only on `reference_annotations.parquet`.

## Output format

Tab-separated, one row per `(query, GO term)` candidate:

```
<query_accession>\t<go_id>\t<score>
```

Score is `max(0, 1 - min_distance)` where `min_distance` is the
cosine distance to the closest neighbour that voted for the term in
the candidate's aspect. Scores are clamped to `[0, 1]`.

## Approach details

- **PLM**: ProtT5-XL UniRef50, mean-pooled over the residue
  dimension (single ~1024-dim vector per protein).
- **KNN**: cosine distance, K=5 by default. Three independent
  searches partitioned by GO aspect (P/F/C).
- **GO transfer**: each neighbour's GO annotations are voted across
  candidates. The v1 baseline emits one candidate per (query, GO
  term) that received at least one vote.
- **No learned reranker**: candidate ordering follows
  `1 - min_distance` only.

## Reproducibility

- **Inference container**:
  `ghcr.io/frapercan/protea/knn-v1:v<release>` built from
  `apps/lafa_knn_v1/Dockerfile`. Source on GitHub
  (<https://github.com/frapercan/PROTEA>).
- **Inference library**: `protea-method`
  (<https://github.com/frapercan/protea-method>). All KNN, feature,
  and reranker code lives here; the container is a thin shell that
  loads the bundle and calls `protea_method.pipeline.predict` with
  the v1 flags pinned.
- **Base image**: `protea-method-runtime` (ADR-D15) at the same
  release tag. The base image owns the heavy dependency graph
  (torch, transformers, lightgbm, faiss, pyarrow) so all three v2
  submissions share a single rebuild path.
- **Bundle exporter**: `scripts/export_lafa_bundle.py` materialises
  the frozen bundle from PROTEA's database for any
  `(embedding_config, annotation_set, reranker)` triple.

## Per-cutoff revisions

Per the 2026-05-06 contract with An Phan, PROTEA can publish a new
`protea-frozen-v<N>-<YYYY-MM-DD>` revision when LAFA extends its
evaluation window. The container image is reusable across cutoffs;
only the bind-mounted bundle changes.

## Limitations

- Mean-pool ProtT5 collapses sequence-position information; methods
  using residue-level attention can outperform on local-feature
  tasks (binding sites, signal peptides).
- The v1 baseline has no learned reranker, so candidate ordering is
  bounded by KNN distance quality. The full pipeline
  (`protea-v18`) adds per-aspect LightGBM scoring on top of v6
  feature enrichment.
- Predictions are emitted only for GO terms that received at least
  one neighbour vote. Coverage is therefore bounded by the reference
  bank's annotation density; sparsely-annotated aspects (e.g. CCO
  for fungal proteins) will see fewer candidates than the full
  pipeline produces via Anc2Vec propagation.

## Authors

- **Francisco Miguel Pérez Canales** (author and sole maintainer of
  PROTEA): pipeline architecture, KNN + v6 feature design, LAFA
  containerisation.
- **PhD co-supervisors**: David Orellana-Martín (Universidad de
  Sevilla), Ana M. Rojas (CABD Sevilla).

## Contact

`frapercan1@gmail.com` (primary), `frapercan1@alum.us.es` (academic).
