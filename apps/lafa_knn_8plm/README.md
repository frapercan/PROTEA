# protea-knn-8plm

F-LAFA v2 submission #2 (ADR-D23). Score-level mean ensemble of eight
protein language models, aspect-separated KNN GO propagation, no v6
features, no learned reranker. Sits between the v1 baseline (one PLM)
and `protea-v18` (full pipeline) on the same LAFA-side conditions.

Published as `ghcr.io/frapercan/protea/knn-8plm:<tag>` by
`.github/workflows/lafa-knn-8plm-container.yml` on push-to-main,
release, and manual dispatch. Tags mirror the PROTEA SemVer scheme
plus `latest` for the default branch and `sha-<short>` for any commit.

## Ensemble line-up

| Key            | Family    | Checkpoint                                 |
|----------------|-----------|--------------------------------------------|
| `esm2_150m`    | ESM-2     | `facebook/esm2_t30_150M_UR50D`             |
| `esm2_650m`    | ESM-2     | `facebook/esm2_t33_650M_UR50D`             |
| `esm2_3b`      | ESM-2     | `facebook/esm2_t36_3B_UR50D`               |
| `prot_t5`      | ProtT5    | `Rostlab/prot_t5_xl_half_uniref50-enc`     |
| `prostt5`      | ProstT5   | `Rostlab/ProstT5`                          |
| `ankh_base`    | Ankh      | `ElnaggarLab/ankh-base`                    |
| `ankh_large`   | Ankh      | `ElnaggarLab/ankh-large`                   |
| `esmc_600m`    | ESM-C     | `esmc_600m` (EvolutionaryScale SDK)        |

All eight produce one mean-pooled vector per query. Per-PLM reference
embeddings ship under `<bundle>/plms/<key>/reference_embeddings.parquet`
(the v1 baseline keeps a single matrix at the bundle root; the
ensemble driver falls back to that path if a per-PLM file is missing).

## Architecture

Layered on top of `protea-method-runtime` (ADR-D15). All heavy layers
(Python 3.12, torch, transformers, `protea-method`, ProtT5 encoder)
come from the base image; this layer adds:

1. The bind-mount conventions from `anphan0828/LAFA_container_guide`
   (`/input/queries.fasta`, `/output/predictions.tsv`, `/bundle`,
   `/hf-cache`).
2. The ensemble driver (`ensemble_driver.py`) and PLM registry
   (`plm_encoders.py`).
3. A small entrypoint script (`lafa_entrypoint.sh`) that pins the
   `--aggregation mean` default and forwards extra positional args to
   the driver.
4. The EvolutionaryScale ESM SDK (`esm>=3,<4`), needed by the ESM-C
   path; everything else is satisfied by the base image's transformers
   install.
5. OCI labels identifying the LAFA method name (`protea-knn-8plm`) so
   the evaluator harness can pin the artefact.

The frozen reference bundle is never baked into the image. It is
bind-mounted at runtime.

## Run example

```bash
docker run --rm \
    -v /path/to/protea-frozen-v226-8plm:/bundle:ro \
    -v $HOME/.cache/huggingface:/hf-cache \
    -v $PWD/queries.fasta:/input/queries.fasta:ro \
    -v $PWD/out:/output \
    ghcr.io/frapercan/protea/knn-8plm:latest
```

The container writes `/output/predictions.tsv` with one row per
`(query, GO term)` candidate, score being the mean of the per-PLM
`max(0, 1 - cosine_distance)` votes across the canonical 8-PLM
line-up (`agg = sum(scores) / 8`, so terms voted by fewer PLMs score
proportionally lower).

## Bundle layout

```
<bundle>/
manifest.json
reference_annotations.parquet
go_term_metadata.parquet
plms/
    esm2_150m/reference_embeddings.parquet
    esm2_650m/reference_embeddings.parquet
    esm2_3b/reference_embeddings.parquet
    prot_t5/reference_embeddings.parquet
    prostt5/reference_embeddings.parquet
    ankh_base/reference_embeddings.parquet
    ankh_large/reference_embeddings.parquet
    esmc_600m/reference_embeddings.parquet
```

`pca_state.npz`, `anc2vec.npz`, and `reranker/` are ignored by the
ensemble driver (v6 features and the LightGBM reranker ship with
`protea-v18`).

If a per-PLM file is missing the driver falls back to a single
`<bundle>/reference_embeddings.parquet` at the bundle root, which is
useful for smoke runs where only one matrix is available.

## Configuration overrides

Positional args appended to `docker run` are forwarded to the
ensemble driver verbatim:

```bash
docker run --rm <mounts> ghcr.io/frapercan/protea/knn-8plm:latest \
    --aggregation max --k 10 --plms esm2_650m,prot_t5,ankh_large
```

The bind-mount layout (4 mounts above) is the only thing the
entrypoint pins; everything else (`--aggregation`, `--k`,
`--metric`, `--plms`, `--model_dir`, `--device`) is overridable
without rebuilding. `--aggregation max` is the only secondary
fusion strategy supported in v2.

## Build args

| Arg                    | Default  | Notes                                                                 |
|------------------------|----------|-----------------------------------------------------------------------|
| `METHOD_RUNTIME_REF`   | `latest` | Tag of `protea-method-runtime` to inherit from. Pin to SemVer for reproducible builds. |

## Smoke test

`tests/test_lafa_knn_8plm_container.py` exercises the build inputs and
the ensemble driver helpers without launching docker or loading any
PLM weights. Coverage:

* per-PLM helpers (bundle loaders, query stacker, score conversion)
  produce the expected shapes on a synthetic bundle,
* aggregation modes (`mean`, `max`) combine candidates correctly,
* the entrypoint script is executable, POSIX `sh -n` parseable, and
  forwards positional args,
* the Dockerfile extends the published method-runtime image,
* the workflow file is valid YAML and targets the expected GHCR path.

The full end-to-end smoke (build + run against a synthetic bundle)
runs in CI under the `lafa-knn-8plm-container` workflow on tagged
releases and manual dispatch.

## Method card

`METHOD_CARD.md` ships the LAFA-facing description (training data,
cutoffs, ensemble strategy, limitations, contact). Submission to LAFA
and direct correspondence with An Phan are owned by the user, not the
container build.
