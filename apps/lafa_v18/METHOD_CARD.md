# protea-v18 (LAFA Method Card)

LAFA v2 submission #3 from PROTEA. Full pipeline: ProtT5 PLM, cosine
KNN with aspect-separated GO propagation, v6 feature enrichment
(NW/SW alignments, taxonomy voters, Anc2Vec semantic coherence,
embedding-PCA projection), and per-aspect LightGBM rerank (one
booster per GO aspect: BPO, MFO, CCO). Companion to `protea-knn-v1`
(single-PLM KNN baseline, no features, no reranker) and
`protea-knn-8plm` (8-PLM score-level ensemble, no features, no
reranker); all three images share the same `protea-method-runtime`
base so the per-stage contribution is measured under identical
inference plumbing.

Draft for the LAFA evaluator (recipient: An Phan,
`anphan0828/LAFA_container_guide`). The author will deliver this card
together with the container reference; this file is the source of
truth in-tree.

## Method overview

For each query protein the container does the following:

1. Embed the query with **ProtT5-XL UniRef50** (mean pool over the
   residue axis, single ~1024-dim vector).
2. Partition the GO ontology by aspect (MFO, BPO, CCO) and run an
   independent cosine-KNN search per aspect (K=5 by default) against
   the frozen reference embeddings. Aspect-separated transfer recovers
   the BPO recall ceiling that a unified search produces on the same
   bank.
3. Transfer the GO annotations of the K neighbours to the query. Each
   candidate GO term inherits the standard KNN columns (vote count,
   minimum and mean cosine distance to its voting neighbours,
   k-position).
4. Enrich every candidate with the v6 feature pass:
   * Needleman-Wunsch and Smith-Waterman alignment scores vs the
     voting neighbours (identity, similarity, alignment length, gap %).
   * Taxonomic distance, common-ancestor count, and per-aspect
     "voters same / close fraction" features.
   * Anc2Vec semantic-coherence cosines (candidate vs neighbour
     centroid, candidate vs query's pre-cutoff known GO set, max-cos
     plus has-embedding flag).
   * 16-dim embedding-PCA projection of the query (frozen mean and
     components shipped in `pca_state.npz`).
5. Score every candidate with the per-aspect LightGBM booster that
   matches its `aspect` column. Three boosters are loaded from
   `reranker/{F,P,C}.txt` in the bundle. Each booster outputs a
   calibrated `[0, 1]` probability. Candidates whose aspect has no
   booster in the bundle fall back to `max(0, 1 - min_distance)` for
   stable ordering.

### Lineage features

protea-contracts v0.3.0 added four lineage features
(`lineage_is_ancestor_of_known`, `lineage_is_descendant_of_known`,
`lineage_ancestor_of_count`, `lineage_descendant_of_count`) to
`ALL_FEATURES`. The v18 entrypoint does not compute them inside the
container, since per-query known-annotation lookup is not available
in a LAFA blind-evaluation setting. When the per-aspect booster has
been trained with lineage splits the LightGBM native missing-value
branch routes the predictions; when the booster does not reference
the lineage columns the feature pass is a no-op. The fallback is
bit-identical to the production scoring router on the same bundle.

## Training data

| Source            | Cutoff                        | Volume                                                  |
|-------------------|-------------------------------|---------------------------------------------------------|
| UniProt sequences | release `2025_02`             | ~574k canonical accessions in the reference bank        |
| GO annotations    | GOA UniProt v226 (2025-05-03) | annotations strictly before LAFA t0 (v227, 2025-09-04) |
| GO ontology       | OBO release 2026-01-23        | superset of v226's GO terms                             |
| Reranker boosters | LightGBM 4.x, per aspect      | one booster per GO aspect (BPO, MFO, CCO), trained off-line by protea-reranker-lab against the matching `Dataset` row |

Training cutoff aligned with LAFA t0: the GAF used for reference
annotations is the latest release strictly before the evaluation
window opens. The per-aspect boosters were trained against the
matching frozen `Dataset` so the feature-schema hash of the booster
agrees byte-for-byte with the bundle's `feature_schema_sha`.

## External resources at inference

- **ProtT5-XL UniRef50** (`Rostlab/prot_t5_xl_half_uniref50-enc`):
  pulled from HuggingFace Hub on first use, cached on a host-mounted
  HF cache directory (`/hf-cache`). No model weights are baked into
  the image.
- **No internet access** required at inference time once the ProtT5
  weights and the frozen-data bundle are present on disk.

## Container

| Field      | Value                                                            |
|------------|------------------------------------------------------------------|
| Image      | `ghcr.io/frapercan/protea/v18:<release tag>`                     |
| Base image | `ghcr.io/frapercan/protea/method-runtime:<release tag>` (ADR-D15) |
| Source     | <https://github.com/frapercan/PROTEA>, path `apps/lafa_v18/`     |
| License    | MIT                                                              |

The image follows the LAFA container guide bind-mount layout:

```
/bundle               frozen reference bundle (read-only)
/input/queries.fasta  FASTA of query proteins (read-only)
/output/              writable dir for predictions.tsv
/hf-cache             HuggingFace cache for ProtT5 weights
```

Default entrypoint runs `protea-predict --aspect_separated` against
the bind-mounted bundle, with v6 features and the per-aspect reranker
enabled by default. Extra positional args are forwarded to
`protea-predict`, so the evaluator can sweep K or distance metrics
without rebuilding (the three pinned pipeline flags are not
overridable from the command line).

## Frozen-data bundle

The container expects a bind-mounted directory at `/bundle`. Layout:

```
<bundle>/
manifest.json                 (cutoff version + schema sha)
reference_embeddings.parquet  (ProtT5 reference matrix)
reference_annotations.parquet (canonical accession GO annotations)
go_term_metadata.parquet      (OBO release: id, go_id, aspect)
pca_state.npz                 (16-dim embedding-PCA mean + components)
anc2vec.npz                   (Anc2Vec GO embeddings, GO release 2020-10-06)
reranker/
    F.txt                     (per-aspect LightGBM booster: MFO)
    P.txt                     (per-aspect LightGBM booster: BPO)
    C.txt                     (per-aspect LightGBM booster: CCO)
```

If the per-aspect boosters are absent the container falls back to the
single-booster legacy path (`reranker.txt` at the bundle root) and
then to the no-rerank baseline (KNN distance ordering). The
`feature_schema_sha` in `manifest.json` is the canonical hash from
protea-contracts (`compute_schema_sha` over the sorted column set);
the boosters refuse to score when their internal schema sha drifts
from the bundle's.

## Output format

Tab-separated, one row per `(query, GO term)` candidate:

```
<query_accession>\t<go_id>\t<score>
```

Score is the per-aspect LightGBM reranker probability in `[0, 1]`.
Candidates whose aspect has no booster in the bundle fall back to
`max(0, 1 - min_distance)` (the v1 baseline scoring rule) so output
ordering remains stable when the bundle ships an incomplete
`reranker/` directory.

## Approach details

- **PLM**: ProtT5-XL UniRef50 (half-precision encoder), mean-pooled
  over the residue dimension (single ~1024-dim vector per protein).
  Production default in PROTEA.
- **KNN**: cosine distance, K=5 by default. Three independent
  searches partitioned by GO aspect (P, F, C).
- **v6 features**: NW and SW alignments (parasail), taxonomy voters
  (ete3 NCBI taxonomy), Anc2Vec semantic-coherence cosines (GO
  release 2020-10-06 pretrained), 16-dim embedding-PCA projection of
  the query.
- **Reranker**: per-aspect LightGBM boosters trained off-line by the
  `protea-reranker-lab` repo. Each booster scores its own aspect's
  candidates; missing per-aspect boosters fall back to a single
  global booster, then to KNN distance ordering, in that priority.

## Reproducibility

- **Inference container**:
  `ghcr.io/frapercan/protea/v18:v<release>` built from
  `apps/lafa_v18/Dockerfile`. Source on GitHub
  (<https://github.com/frapercan/PROTEA>).
- **Inference library**: `protea-method`
  (<https://github.com/frapercan/protea-method>). All KNN, feature,
  and reranker code lives here; the container is a thin shell that
  loads the bundle and calls `protea_method.pipeline.predict` with
  the v18 flags pinned.
- **Reranker training**: `protea-reranker-lab`
  (<https://github.com/frapercan/protea-reranker-lab>). The lab
  consumes the same frozen `Dataset` row that the bundle exporter
  publishes, trains the per-aspect LightGBM boosters, and writes them
  back to PROTEA's `RerankerModel` table. The bundle exporter then
  bakes the booster bytes into `reranker/{F,P,C}.txt`.
- **Base image**: `protea-method-runtime` (ADR-D15) at the same
  release tag. The base image owns the heavy dependency graph
  (torch, transformers, lightgbm, faiss, pyarrow, parasail, ete3)
  so all three v2 submissions share a single rebuild path.
- **Bundle exporter**: `scripts/export_lafa_bundle.py` materialises
  the frozen bundle from PROTEA's database for any
  `(embedding_config, annotation_set, reranker_model_id)` triple.

## Per-cutoff revisions

Per the 2026-05-06 contract with An Phan, PROTEA can publish a new
`protea-frozen-v<N>-<YYYY-MM-DD>` revision when LAFA extends its
evaluation window. The container image is reusable across cutoffs;
only the bind-mounted bundle changes. Re-training of the per-aspect
boosters happens off-line in `protea-reranker-lab` and is republished
as a new `RerankerModel` row that the next bundle export consumes.

## Limitations

- Mean-pool ProtT5 collapses sequence-position information; methods
  using residue-level attention can outperform on local-feature
  tasks (binding sites, signal peptides). The 8-PLM ensemble
  submission (`protea-knn-8plm`) partly recovers position-sensitive
  signal at the embedding level; v18 keeps the single PLM and pushes
  feature gain into the reranker stage instead.
- Lineage features (`lineage_*`) are not computed inside the
  container in v2. Boosters trained against them route via the
  LightGBM missing-value branch. A future cutoff that exposes
  per-query known annotations can re-enable the lineage compute path
  without retraining.
- Per-aspect rerank trusts the `aspect` column of the bundle's
  `go_term_metadata.parquet`. Terms whose aspect is missing or
  ambiguous (e.g. obsolete cross-aspect terms) fall through to the
  KNN distance fallback and are emitted with their baseline score.
- Bundle size grows with the v6 state files (`pca_state.npz`,
  `anc2vec.npz`) and the three per-aspect boosters. The v18 bundle
  is typically 1.5x to 2x the v1 baseline bundle for the same
  embedding configuration.

## Authors

- **Francisco Miguel Perez Canales** (author and sole maintainer of
  PROTEA): pipeline architecture, KNN plus v6 feature design,
  per-aspect rerank, LAFA containerisation.
- **PhD co-supervisors**: David Orellana-Martin (Universidad de
  Sevilla), Ana M. Rojas (CABD Sevilla).

## Contact

`frapercan1@gmail.com` (primary), `frapercan1@alum.us.es` (academic).
