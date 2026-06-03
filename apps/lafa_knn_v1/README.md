# protea-knn-v1

F-LAFA v2 submission #1 (ADR-D23). ProtT5 PLM, KNN GO propagation, no
v6 features, no learned reranker. Pure baseline so subsequent
submissions (`protea-knn-8plm`, `protea-v18`) can be compared on
identical LAFA-side conditions.

Published as `ghcr.io/frapercan/protea/knn-v1:<tag>` by
`.github/workflows/lafa-knn-v1-container.yml` on push-to-main,
release, and manual dispatch. Tags mirror the PROTEA SemVer scheme
plus `latest` for the default branch and `sha-<short>` for any commit.

## Architecture

Layered on top of `protea-method-runtime` (ADR-D15). All heavy layers
(Python 3.12, torch, transformers, `protea-method`, ProtT5 encoder)
come from the base image; this layer adds:

1. The bind-mount conventions from `anphan0828/LAFA_container_guide`
   (`/input/queries.fasta`, `/output/predictions.tsv`, `/bundle`,
   `/hf-cache`).
2. A small entrypoint script (`lafa_entrypoint.sh`) that pins the v1
   KNN configuration (`--aspect_separated --no_v6 --no_reranker`).
3. OCI labels identifying the LAFA method name (`protea-knn-v1`) so
   the evaluator harness can pin the artefact.

The frozen reference bundle is never baked into the image. It is
bind-mounted at runtime; see `apps/method_runtime/README.md` for the
bundle layout.

## Run example

```bash
docker run --rm \
    -v /path/to/protea-frozen-v226-2025-05-03:/bundle:ro \
    -v $HOME/.cache/huggingface:/hf-cache \
    -v $PWD/queries.fasta:/input/queries.fasta:ro \
    -v $PWD/out:/output \
    ghcr.io/frapercan/protea/knn-v1:latest
```

The container writes `/output/predictions.tsv` with one row per
`(query, GO term)` candidate, score = `max(0, 1 - cosine_distance)`.

## Configuration overrides

Positional args appended to `docker run` are forwarded to
`protea-predict` verbatim. For example, to switch the KNN backend to
faiss:

```bash
docker run --rm <mounts> ghcr.io/frapercan/protea/knn-v1:latest \
    --backend faiss --k 10
```

This intentionally lets An Phan's evaluator sweep K or distance
metrics without rebuilding the image. The pinned baseline flags
(`--aspect_separated --no_v6 --no_reranker`) cannot be undone from
the command line; if a non-baseline run is needed, use the
`method-runtime` base image directly.

## Build args

| Arg | Default | Notes |
|-----|---------|-------|
| `METHOD_RUNTIME_REF` | `latest` | Tag of `protea-method-runtime` to inherit from. Pin to a SemVer tag for reproducible builds. |

## Smoke test

`tests/test_lafa_knn_v1_container.py` exercises the entrypoint
contract without launching docker:

* the shell script is executable, POSIX-`sh` parsable, and pins the
  three baseline flags,
* the Dockerfile builds on top of the published
  `protea-method-runtime` image,
* the workflow file is valid YAML and targets the expected GHCR path.

The full end-to-end smoke (build + run against a synthetic bundle)
runs in CI under the `lafa-knn-v1-container` workflow on tagged
releases and manual dispatch.

## Method card

`METHOD_CARD.md` ships the LAFA-facing description (training data,
cutoffs, pipeline, limitations, contact). Submission to LAFA and
direct correspondence with An Phan are owned by the user, not the
container build.
