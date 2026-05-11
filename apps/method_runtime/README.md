# protea-method-runtime

Tier 3 distribution channel for PROTEA inference, per ADR-D15. Slim
container shipping `protea-method` plus the ProtT5 query encoder and a
FASTA-in / TSV-out CLI (`protea-predict`).

Published as `ghcr.io/frapercan/protea/method-runtime:<tag>` by
`.github/workflows/method-runtime-container.yml` on push-to-main,
release, and manual dispatch. Tag scheme follows PROTEA SemVer releases
(plus `latest` for the default branch and `sha-<short>` for any commit).

## Audience

Per ADR-D15 the package has three faces:

| Tier | Footprint | Use case |
|------|-----------|----------|
| 1 | `pip install protea-method` | Caller already has embeddings; just predict |
| 2 | `pip install protea-method[esm]` | Caller has FASTA; embed + predict end-to-end |
| 3 | This image | HPC airgap, single FASTA-in / TSV-out container |

The LAFA v2 submission containers (ADR-D23: `protea-knn-v1`,
`protea-knn-8plm`, `protea-v18`) layer their bind-mount conventions on
top of this image so the heavy layers (Python + torch + transformers +
`protea-method`) only build once.

## Bundle layout

The frozen reference bundle is bind-mounted at runtime; nothing is
baked into the image. The schema matches the v1 LAFA wrapper
(`apps/lafa_container/protea_main.py`):

```
<bundle>/
├── manifest.json                # cutoff_version + feature_schema_sha
├── reference_embeddings.parquet # (accession, embedding)
├── reference_annotations.parquet# (accession, go_term_id, go_id, ...)
├── go_term_metadata.parquet     # (go_term_id, go_id, aspect, name, ia_weight)
├── pca_state.npz                # mean + components for query PCA (optional)
├── anc2vec.npz                  # Anc2Vec GO embedding dictionary (optional)
└── reranker/                    # per-aspect LightGBM boosters
    ├── F.txt                    # MFO booster (optional)
    ├── P.txt                    # BPO booster (optional)
    ├── C.txt                    # CCO booster (optional)
    └── routing.json             # provenance (optional, not enforced)
```

A legacy single-booster layout (`reranker.txt` at the bundle root) is
accepted; the per-aspect `reranker/` directory takes precedence when
both are present. Either booster path is skipped under `--no_reranker`.

## Run example

```bash
docker run --rm \
    -v /path/to/protea-frozen-v226-2025-05-03:/bundle \
    -v $HOME/.cache/huggingface:/hf-cache \
    -e HF_CACHE=/hf-cache \
    -v $PWD/queries.fasta:/queries.fasta \
    -v $PWD/predictions.tsv:/predictions.tsv \
    ghcr.io/frapercan/protea/method-runtime:latest \
        --query_file /queries.fasta \
        --frozen_data_dir /bundle \
        --output /predictions.tsv \
        --aspect_separated
```

## CLI reference

```
protea-predict
    --query_file PATH          FASTA of query sequences
    --frozen_data_dir PATH     Bind-mounted bundle directory
    --output PATH              Output TSV (gzipped if suffix is .gz)
    --k INT                    KNN neighbours (default 5)
    --metric {cosine,l2}       Distance metric (default cosine)
    --backend {numpy,faiss}    KNN backend (default numpy)
    --aspect_separated         One KNN per GO aspect (P / F / C)
    --no_v6                    Skip v6 feature enrichment
    --no_reranker              Skip LightGBM rerank
    --model_dir PATH           HuggingFace cache (default $HF_CACHE)
```

Output is tab-separated `<query_accession>\t<go_id>\t<score>` with one
row per (query, GO term) candidate. When no booster ships in the
bundle (or `--no_reranker` is set) the score is `max(0, 1 - distance)`.

## Build args

| Arg | Default | Notes |
|-----|---------|-------|
| `PROTEA_METHOD_REF` | `master` | Git ref of `protea-method` to install (use a tag for reproducible builds). |

## Relationship to `apps/lafa_container/`

`apps/lafa_container/` is the F-LAFA v1 wrapper that shipped in PROTEA
`v0.7.0`-`v0.7.2`. It will be rebased on top of this image as part of
F-LAFA v2 (separate slice). Until then both images co-exist; this one
is the base layer, the LAFA v1 image is the legacy delivery.
