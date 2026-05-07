# PROTEA — LAFA Method Card

Draft for the LAFA submission. Will be revised once An Phan shares
the official template.

## Method overview

PROTEA is a KNN-based protein function prediction system that
augments nearest-neighbour retrieval with a per-aspect LightGBM
re-ranker. For each query protein it (1) computes a single
mean-pooled ProtT5 embedding, (2) finds the top-K most similar
proteins in a frozen reference bank, (3) transfers GO term votes
from those neighbours, (4) enriches each candidate term with a
"v6" feature family (Anc2Vec ontology embeddings, neighbour
centroids, taxonomy voters, query-PCA projection), and (5) scores
each candidate with an aspect-specific LightGBM booster trained
offline by `protea-reranker-lab`. When no booster is available for
a given aspect the candidates fall back to KNN distance ordering.

## Training data

| Source | Cutoff | Volume |
|---|---|---|
| UniProt sequences | release ≤ `2025_02` | ~574k canonical accessions in the reference bank |
| GO annotations | GOA UniProt **v226 (2025-05-03)** | annotations strictly before LAFA's earliest t0 (v227, 2025-09-04) |
| GO ontology | OBO release `2026-01-23` | superset of v226's GO terms (no annotation drops at load time) |

Training cutoff aligned with LAFA's t0: the GAF used for reference
annotations is the latest release strictly before the evaluation
window opens, and the OBO release is the earliest one fully covering
the GO terms referenced in that GAF.

## External resources at inference

- **ProtT5-XL embeddings** (`Rostlab/prot_t5_xl_uniref50`):
  pulled from HuggingFace Hub on first use, cached on a host-mounted
  HF cache (no model weights baked into the image).
- **No internet access** required at inference time once the model
  weights and the frozen-data bundle are present on disk.

## Frozen-data bundle layout

The container expects a bind-mounted directory at `--frozen_data_dir`:

```
protea-frozen-v226-2025-05-03/
├── manifest.json                 # cutoff version + schema sha
├── reference_embeddings.parquet  # 574k canonical accessions × ProtT5 dim
├── reference_annotations.parquet # GOA v226 rows rolled up to canonical
├── go_term_metadata.parquet      # OBO 2026-01-23 (id, go_id, aspect, name)
├── pca_state.npz                 # mean + components for query PCA
├── anc2vec.npz                   # GO-term Anc2Vec dictionary (2020-10)
└── reranker/                     # per-aspect LightGBM boosters
    ├── F.txt                     # MFO booster
    ├── P.txt                     # BPO booster
    ├── C.txt                     # CCO booster
    └── routing.json              # aspect → booster filename + provenance
```

Per-cutoff bundles live in their own directory. Future training
cutoffs (LAFA window growing) ship as
`protea-frozen-v<N>-<YYYY-MM-DD>` revisions; a single container
image is reusable across them.

## Approach details

- **PLM**: ProtT5-XL UniRef50, mean-pooled over the residue dimension
  (single ~1024-dim vector per protein).
- **KNN**: cosine distance, K=5 by default, three independent
  searches partitioned by GO aspect (P/F/C) so the BPO recall ceiling
  observed with unified-index transfer disappears.
- **GO transfer**: each neighbour's GO annotations are voted across
  candidates; vote count + min/mean distance are the base features.
- **v6 features**: Anc2Vec centroids over neighbour-side annotations,
  per-aspect candidate vector cosine, query embedding projected onto
  16 PCA components, optional taxonomy-pair voters (off by default
  in the LAFA bundle since taxonomy lookups are not part of LAFA's
  query-time inputs).
- **Re-ranker**: LightGBM (binary classifier with sigmoid
  calibration to [0, 1]). Trained per (tier, aspect) cell on a frozen
  PROTEA dataset; the LAFA submission ships the highest-Fmax
  per-aspect winner from the v19 family, falling back to KNN distance
  when no booster covers an aspect.
- **Output**: one row per `(query, GO term)` candidate; score is the
  reranker output when a booster ships for the candidate's aspect,
  else `1 - distance` of the closest contributing neighbour.

## Reproducibility

- **Inference container**:
  `ghcr.io/frapercan/protea/lafa:v<release>` built from `apps/lafa_container/Dockerfile`.
  Source: <https://github.com/frapercan/PROTEA>.
- **Inference library**:
  `protea-method` (<https://github.com/frapercan/protea-method>).
  All KNN, feature enrichment, and reranker scoring code lives here;
  the container is a thin shell that loads the bundle and calls
  `protea_method.pipeline.predict`.
- **Bundle exporter**:
  `scripts/export_lafa_bundle.py` materialises the frozen bundle
  from PROTEA's database for any (embedding_config, annotation_set,
  reranker) triple. Per-cutoff submissions are produced by re-running
  this script with a fresh `source_version` annotation set.

## Per-cutoff revisions

Per the 2026-05-06 contract with An Phan: when LAFA extends its
evaluation window past `Mar_2026`, PROTEA can publish a new
`protea-frozen-v<N>-<YYYY-MM-DD>` revision (re-running the GOA loader
+ exporter for the new cutoff) and notify An; the same container
image accepts the new bundle without code changes. This lets LAFA
test the recency effect of training-data freshness without
re-engineering the inference path.

## Limitations

- Mean-pool ProtT5 collapses sequence-position information; methods
  using residue-level attention can outperform on local-feature
  tasks (binding sites, signal peptides).
- The reranker is per-aspect but not per-tier (NK/LK/PK) at
  inference time — a query's tier is determined by LAFA at
  evaluation time. The submitted boosters are the highest-Fmax
  single-tier specialisation per aspect (NK-MFO, NK-BPO, NK-CCO).
- Anc2Vec embeddings are from the 2020-10 release; GO terms added
  to the ontology after that date receive zero vectors, which
  degrades the v6 neighbour-centroid feature on those terms.

## Authors

- **Francisco Miguel Pérez Canales** — PROTEA architecture, KNN +
  v6 feature pipeline, LAFA container.
- **PhD co-supervisors**: David Orellana-Martín (Universidad de
  Sevilla), Ana M. Rojas (CABD Sevilla).

## Contact

`frapercan1@gmail.com` (primary), `frapercan1@alum.us.es` (academic).
