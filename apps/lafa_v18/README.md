# protea-v18

F-LAFA v2 submission #3 (ADR-D23). Full PROTEA pipeline: ProtT5 PLM,
aspect-separated KNN GO propagation, v6 feature enrichment
(NW/SW alignments, taxonomy voters, Anc2Vec semantic coherence,
embedding-PCA projection), and per-aspect LightGBM rerank (one
booster per GO aspect: BPO, MFO, CCO). Sits on top of the v1 baseline
(`protea-knn-v1`) and the v2 ensemble (`protea-knn-8plm`) to expose
the doctoral-thesis full pipeline to An Phan's evaluator on identical
LAFA-side conditions.

Published as `ghcr.io/frapercan/protea/v18:<tag>` by
`.github/workflows/lafa-v18-container.yml` on push-to-main, release,
and manual dispatch. Tags mirror the PROTEA SemVer scheme plus
`latest` for the default branch and `sha-<short>` for any commit.

## Why a single PLM

The v18 pipeline pins a single PLM (ProtT5-XL UniRef50, half-precision
encoder). Two reasons:

1. T-RES.1b analysis: the `anc2vec_neighbor` family and the per-aspect
   aggregation features dominate LightGBM importance. Switching the
   query encoder moves the head of the feature distribution, not the
   tail; the gain from a multi-PLM ensemble at the embedding level is
   captured separately by the v2 `protea-knn-8plm` submission.
2. Container size and bundle size both stay close to the v1 baseline,
   so the evaluator can run all three submissions on the same hardware
   budget.

ProtT5 is the production default in PROTEA (same encoder as
`protea-knn-v1` and the historical CAFA 6 submission lineage that the
"v18" name refers to).

## Architecture

Layered on top of `protea-method-runtime` (ADR-D15). All heavy layers
(Python 3.12, torch, transformers, `protea-method`, ProtT5 encoder)
come from the base image; this layer adds:

1. The bind-mount conventions from `anphan0828/LAFA_container_guide`
   (`/input/queries.fasta`, `/output/predictions.tsv`, `/bundle`,
   `/hf-cache`).
2. A small entrypoint script (`lafa_entrypoint.sh`) that pins the v18
   pipeline configuration: `--aspect_separated`, v6 features ON,
   per-aspect reranker ON.
3. OCI labels identifying the LAFA method name (`protea-v18`) so the
   evaluator harness can pin the artefact.

The frozen reference bundle is never baked into the image. It is
bind-mounted at runtime; see "Bundle layout" below.

## Run example

```bash
docker run --rm \
    -v /path/to/protea-frozen-v226-v18:/bundle:ro \
    -v $HOME/.cache/huggingface:/hf-cache \
    -v $PWD/queries.fasta:/input/queries.fasta:ro \
    -v $PWD/out:/output \
    ghcr.io/frapercan/protea/v18:latest
```

The container writes `/output/predictions.tsv` with one row per
`(query, GO term)` candidate, scored by the per-aspect reranker
(LightGBM probability, in `[0, 1]`). Rows whose aspect has no booster
in the bundle fall back to `max(0, 1 - min_distance)` for stable
ordering.

## Bundle layout

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

The bundle exporter (`scripts/export_lafa_bundle.py`) materialises this
layout from PROTEA's database for any `(embedding_config,
annotation_set, reranker_model_id)` triple. The same `manifest.json`
schema works across the three F-LAFA v2 submissions; only the
artefacts in the `reranker/` directory and the optional v6 state files
differ.

If `reranker/F.txt` / `P.txt` / `C.txt` are absent the entrypoint
falls back to the single-booster legacy path (`reranker.txt` at the
bundle root) and then to no-rerank baseline ordering. The image will
still emit predictions in all three fallback modes; only the score
column changes meaning.

### Lineage features

The protea-contracts v0.3.0 schema added four `lineage_*` features to
`ALL_FEATURES`. The v18 entrypoint does not compute them inside the
container (per-query known-set lookup is not available in a LAFA blind
evaluation). When a per-aspect booster has been trained with lineage
splits the LightGBM native missing-value branch routes the
predictions; when the booster does not reference the lineage columns
the feature pass is a no-op. Both paths are bit-identical to the
production scoring router on the same bundle.

## Configuration overrides

Positional args appended to `docker run` are forwarded to
`protea-predict` verbatim. For example, to switch the KNN backend to
faiss and bump K to 10:

```bash
docker run --rm <mounts> ghcr.io/frapercan/protea/v18:latest \
    --backend faiss --k 10
```

This intentionally lets the evaluator sweep K, distance metric, or
backend without rebuilding the image. The three pinned pipeline flags
(`--aspect_separated` plus the implicit v6 and reranker paths) cannot
be undone from the command line; if a non-v18 run is needed, use the
`method-runtime` base image directly.

## Build args

| Arg                  | Default  | Notes                                                                                  |
|----------------------|----------|----------------------------------------------------------------------------------------|
| `METHOD_RUNTIME_REF` | `latest` | Tag of `protea-method-runtime` to inherit from. Pin to a SemVer tag for reproducible builds. |

## Smoke test

`tests/test_lafa_v18_container.py` exercises the entrypoint contract
without launching docker:

* the shell script is executable, POSIX `sh -n` parsable, and pins the
  v18 pipeline flag (`--aspect_separated`) without the `--no_v6`
  and `--no_reranker` opt-outs the v1 baseline ships,
* the Dockerfile extends the published `protea-method-runtime` image
  and creates the LAFA bind-mount points,
* the entrypoint surfaces stable non-zero exit codes when the LAFA
  bind mounts are missing (`64` / `65` / `66`),
* the workflow file is valid YAML and targets the expected GHCR path.

The full end-to-end smoke (build plus run against a synthetic bundle)
runs in CI under the `lafa-v18-container` workflow on tagged releases
and manual dispatch.

## Method card

`METHOD_CARD.md` ships the LAFA-facing description (training data,
cutoffs, pipeline stages, limitations, contact). Submission to LAFA
and direct correspondence with An Phan are owned by the user, not the
container build.
