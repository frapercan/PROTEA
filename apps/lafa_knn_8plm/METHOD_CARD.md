# protea-knn-8plm (LAFA Method Card)

LAFA v2 submission #2 from PROTEA. Score-level mean ensemble of eight
protein language models, cosine KNN with aspect-separated GO
propagation, no v6 features, no learned reranker. Companion to
`protea-knn-v1` (single ProtT5 baseline) and `protea-v18` (full
pipeline with v6 features and per-aspect LightGBM rerank); all three
images share the same `protea-method-runtime` base so the ensemble
gain is measured under identical inference plumbing.

Draft for the LAFA evaluator (recipient: An Phan,
`anphan0828/LAFA_container_guide`). The author will deliver this card
together with the container reference; this file is the source of
truth in-tree.

## Method overview

For each query protein the container does the following:

1. Embed the query with eight protein language models (mean-pool over
   the residue axis, one fixed-dimension vector per model).
2. Partition the GO ontology by aspect (MFO, BPO, CCO).
3. For each PLM and each aspect, run an independent cosine-KNN search
   (K=5 by default) against the frozen per-PLM reference embeddings.
   Aspect-separated transfer recovers the BPO recall ceiling that a
   unified search produces on the same bank.
4. Transfer the GO annotations of the K neighbours to the query. For
   each ``(query, GO term)`` candidate compute a per-PLM score
   ``max(0, 1 - min_distance)`` where ``min_distance`` is the cosine
   distance to the closest neighbour that voted for the term in the
   candidate's aspect.
5. Aggregate the eight per-PLM scores into a single ensemble score by
   the mean (default) or the max. The mean divides by the full
   ensemble size, not the number of voters, so a term seen by only
   one PLM is penalised by a factor of `1 / 8`.

The v2 ensemble omits the v6 feature pass (Anc2Vec centroids, PCA
projection, taxonomy voters) and the LightGBM reranker. These layers
ship in `protea-v18`, the third F-LAFA v2 submission.

## Ensemble line-up

| Key            | Family    | Checkpoint                                 | Pooling |
|----------------|-----------|--------------------------------------------|---------|
| `esm2_150m`    | ESM-2     | `facebook/esm2_t30_150M_UR50D`             | mean    |
| `esm2_650m`    | ESM-2     | `facebook/esm2_t33_650M_UR50D`             | mean    |
| `esm2_3b`      | ESM-2     | `facebook/esm2_t36_3B_UR50D`               | mean    |
| `prot_t5`      | ProtT5    | `Rostlab/prot_t5_xl_half_uniref50-enc`     | mean    |
| `prostt5`      | ProstT5   | `Rostlab/ProstT5`                          | mean    |
| `ankh_base`    | Ankh      | `ElnaggarLab/ankh-base`                    | mean    |
| `ankh_large`   | Ankh      | `ElnaggarLab/ankh-large`                   | mean    |
| `esmc_600m`    | ESM-C     | `esmc_600m` (EvolutionaryScale SDK)        | mean    |

All eight encoders return a single mean-pooled vector per query.
Strip-CLS / strip-EOS handling for the ESM family matches the
`protea-backends` convention (see `src/protea_backends/esm/__init__.py`).

## Why mean aggregation

The F-LAFA.2 acceptance criterion (per the executor plan) and ADR-D23
specify "8 PLMs averaged at score level". Mean is the canonical
late-fusion baseline in the protein-PLM ensembling literature and has
three properties this submission needs:

1. **Calibrated coverage**. Dividing by the full ensemble size (not
   the number of voters) penalises GO terms that only one model
   nominates, which is the dominant failure mode of max-fusion in the
   lab grid.
2. **No per-PLM trust prior**. Score-weighted fusion would require a
   held-out per-PLM weight vector that we have not validated for the
   LAFA t0 cutoff; introducing one without evidence would be
   research-by-press-release.
3. **Auditable**. Mean is the simplest combination that respects the
   acceptance criterion verbatim and is trivial for An Phan's
   evaluator to reproduce from the per-PLM matrices.

Max is exposed as `--aggregation max` for ablation runs; it stays out
of the default to keep the submission identity stable.

## Training data

| Source            | Cutoff                  | Volume                                                  |
|-------------------|-------------------------|---------------------------------------------------------|
| UniProt sequences | release `2025_02`       | ~574k canonical accessions in the reference bank        |
| GO annotations    | GOA UniProt v226 (2025-05-03) | annotations strictly before LAFA t0 (v227, 2025-09-04) |
| GO ontology       | OBO release 2026-01-23  | superset of v226's GO terms                             |

Training cutoff aligned with LAFA t0: the GAF used for reference
annotations is the latest release strictly before the evaluation
window opens. Per-PLM reference embeddings re-encode the same UniProt
slice with each of the eight models, so the ensemble compares like
with like.

## External resources at inference

- **HuggingFace weights** for ESM-2 (150M / 650M / 3B), ProtT5,
  ProstT5, Ankh-base, Ankh-large: pulled from the Hub on first use,
  cached on a host-mounted HF cache directory (`/hf-cache`).
- **EvolutionaryScale SDK** for ESM-C 600M: pulled by the `esm` Python
  package shipped in the image. No additional bind mount required.
- **No internet access** is required at inference time once all eight
  PLM weight files and the frozen-data bundle are present on disk.

## Container

| Field         | Value                                                                                |
|---------------|--------------------------------------------------------------------------------------|
| Image         | `ghcr.io/frapercan/protea/knn-8plm:<release tag>`                                    |
| Base image    | `ghcr.io/frapercan/protea/method-runtime:<release tag>` (ADR-D15)                    |
| Source        | <https://github.com/frapercan/PROTEA>, path `apps/lafa_knn_8plm/`                    |
| License       | MIT                                                                                  |

The image follows the LAFA container guide bind-mount layout:

```
/bundle               frozen reference bundle (per-PLM matrices)
/input/queries.fasta  FASTA of query proteins (read-only)
/output/              writable dir for predictions.tsv
/hf-cache             HuggingFace cache for PLM weights
```

Default entrypoint runs the ensemble driver with
`--aggregation mean` and the canonical 8-PLM line-up. Extra
positional args are forwarded, so the evaluator can sweep K,
aggregation, distance, or PLM subset without rebuilding.

## Frozen-data bundle

The container expects a bind-mounted directory at `/bundle`. Layout:

```
<bundle>/
manifest.json                 (cutoff version + schema sha)
reference_annotations.parquet (shared across PLMs: accession, go_term_id, go_id)
go_term_metadata.parquet      (OBO 2026-01-23: id, go_id, aspect)
plms/
    <plm_key>/reference_embeddings.parquet  (per-PLM reference matrix)
```

`pca_state.npz`, `anc2vec.npz`, and `reranker/` are ignored by the
ensemble driver. A legacy single-matrix layout
(`<bundle>/reference_embeddings.parquet` at the root) is accepted for
smoke runs.

## Output format

Tab-separated, one row per `(query, GO term)` candidate:

```
<query_accession>\t<go_id>\t<score>
```

Score is the score-level mean across the 8 PLMs of
`max(0, 1 - min_distance_per_plm)`, divided by 8 (not the number of
voters), so terms voted by all 8 models score up to 1.0 and terms
voted by 1 model score up to `1 / 8 = 0.125`. Scores are clamped to
`[0, 1]`.

## Approach details

- **PLMs**: 8 models (see the line-up table), all mean-pooled.
- **KNN**: cosine distance, K=5 by default. Three aspect-separated
  searches per PLM (P / F / C). Eight x three = 24 KNN runs per
  inference batch.
- **GO transfer**: per-PLM each neighbour's GO annotations are voted
  across candidates. The candidate score is
  `max(0, 1 - min_distance)`.
- **Ensemble fusion**: mean over the 8 PLMs by default,
  `--aggregation max` available for ablation.
- **No learned reranker**: candidate ordering follows the aggregated
  score only.

## Reproducibility

- **Inference container**:
  `ghcr.io/frapercan/protea/knn-8plm:v<release>` built from
  `apps/lafa_knn_8plm/Dockerfile`. Source on GitHub
  (<https://github.com/frapercan/PROTEA>).
- **Inference library**: `protea-method`
  (<https://github.com/frapercan/protea-method>). KNN and per-PLM
  prediction routines live there; the container is a thin shell that
  loops over PLMs and combines the candidate sets.
- **Base image**: `protea-method-runtime` (ADR-D15) at the same
  release tag. All three F-LAFA v2 submissions share a single rebuild
  path through this base layer.
- **Bundle exporter**: `scripts/export_lafa_bundle.py` materialises
  the frozen bundle from PROTEA's database for any
  `(embedding_config_set, annotation_set, reranker)` triple. The 8plm
  bundle is exported once per LAFA cutoff with `--plms <8-key list>`.

## Per-cutoff revisions

Per the 2026-05-06 contract with An Phan, PROTEA can publish a new
`protea-frozen-v<N>-<YYYY-MM-DD>` revision when LAFA extends its
evaluation window. The container image is reusable across cutoffs;
only the bind-mounted bundle changes.

## Limitations

- Mean-pool collapses sequence-position information for all eight
  models; methods using residue-level attention can outperform on
  local-feature tasks (binding sites, signal peptides). The ensemble
  partly recovers this because different PLMs encode different
  inductive biases (e.g. ProstT5 carries 3Di-style structural cues
  even when only the AA channel is fed at inference).
- Score-level mean is unweighted. A single weak PLM can drag the
  ensemble down on aspects where it disagrees with the rest. The lab
  grid will revisit weighted fusion once we have per-aspect
  validation curves.
- Predictions are emitted only for GO terms that received at least
  one neighbour vote from at least one PLM. Coverage is therefore
  bounded by the densest annotated PLM-aspect pair in the reference
  bank; very sparse aspect-PLM combinations (e.g. CCO under a small
  ESM-2 variant for fungal proteins) will fall back to the contribution
  of the better-covered models.
- The eight PLMs do not share an embedding dimension; the per-PLM
  KNN keeps them independent and the score-level fusion sidesteps the
  rank-mismatch problem at the cost of losing cross-PLM neighbour
  geometry. `protea-v18` re-introduces cross-PLM structure through
  v6 features and the per-aspect reranker.

## Authors

- **Francisco Miguel Perez Canales** (author and sole maintainer of
  PROTEA): ensemble design, container architecture, LAFA
  containerisation.
- **PhD co-supervisors**: David Orellana-Martin (Universidad de
  Sevilla), Ana M. Rojas (CABD Sevilla).

## Contact

`frapercan1@gmail.com` (primary), `frapercan1@alum.us.es` (academic).
