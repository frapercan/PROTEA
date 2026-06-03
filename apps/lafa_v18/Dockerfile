# syntax=docker/dockerfile:1.7
#
# protea-v18 - F-LAFA v2 submission #3 (ADR-D23).
#
# Full PROTEA pipeline. ProtT5 PLM, aspect-separated KNN GO propagation,
# v6 feature enrichment (NW/SW alignments, taxonomy voters, Anc2Vec
# semantic coherence, embedding-PCA projection), per-aspect LightGBM
# reranker (BPO/MFO/CCO). The third LAFA v2 submission per ADR-D23;
# companion to ``protea-knn-v1`` (single-PLM baseline) and
# ``protea-knn-8plm`` (8-PLM score-level ensemble).
#
# Base image is the Tier 3 ``protea-method-runtime`` (ADR-D15). All
# heavy layers (Python 3.12 + torch + transformers + protea-method +
# ProtT5 encoder) come from there; this image only adds a thin
# entrypoint that pins the v18 full-pipeline configuration and follows
# the LAFA bind-mount conventions from ``anphan0828/LAFA_container_guide``.
#
# Run example (matches LAFA evaluator harness)::
#
#     docker run --rm \
#         -v /path/to/protea-frozen-v226-v18:/bundle:ro \
#         -v $HOME/.cache/huggingface:/hf-cache \
#         -v $PWD/queries.fasta:/input/queries.fasta:ro \
#         -v $PWD/out:/output \
#         ghcr.io/frapercan/protea/v18:latest
#
# The default ENTRYPOINT runs ``protea-predict`` with the v18 flags
# (``--aspect_separated`` and the implicit v6+reranker paths) against
# the bind-mounted bundle and writes ``/output/predictions.tsv``.
# Callers can override flags (K, distance metric, faiss backend) by
# appending positional args to ``docker run``; the three pinned
# pipeline flags cannot be undone from the command line.

ARG METHOD_RUNTIME_REF=latest
FROM ghcr.io/frapercan/protea/method-runtime:${METHOD_RUNTIME_REF} AS runtime

# Bind-mount points expected by the LAFA evaluator. We create them
# under root before dropping privileges so the runtime ``protea`` user
# does not need to be able to mkdir at run time.
USER root
RUN mkdir -p /input /output /bundle /hf-cache \
    && chown -R protea:protea /input /output /bundle /hf-cache

WORKDIR /app
COPY apps/lafa_v18/lafa_entrypoint.sh /app/lafa_entrypoint.sh
RUN chmod 0755 /app/lafa_entrypoint.sh

# Override the base image labels with the v18 full-pipeline identity.
LABEL org.opencontainers.image.title="protea-v18" \
      org.opencontainers.image.description="PROTEA F-LAFA v2 submission: full pipeline (ProtT5 + aspect-separated KNN + v6 features + per-aspect LightGBM reranker)." \
      org.opencontainers.image.source="https://github.com/frapercan/PROTEA" \
      org.opencontainers.image.licenses="MIT" \
      net.functionbench.lafa.method="protea-v18" \
      net.functionbench.lafa.adr="D23"

ENV HF_CACHE=/hf-cache \
    PROTEA_V18_BUNDLE=/bundle \
    PROTEA_V18_QUERY=/input/queries.fasta \
    PROTEA_V18_OUTPUT=/output/predictions.tsv

USER protea
ENTRYPOINT ["/app/lafa_entrypoint.sh"]
