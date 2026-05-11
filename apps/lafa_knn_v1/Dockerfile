# syntax=docker/dockerfile:1.7
#
# protea-knn-v1 — F-LAFA v2 submission #1 (ADR-D23).
#
# ProtT5 PLM, KNN GO propagation, no v6 features, no learned reranker.
# Pure baseline so An Phan's evaluator can compare against the
# subsequent ensemble (``protea-knn-8plm``) and full pipeline
# (``protea-v18``) on identical LAFA-side conditions.
#
# Base image is the Tier 3 ``protea-method-runtime`` (ADR-D15). All
# heavy layers (Python 3.12 + torch + transformers + protea-method +
# ProtT5 encoder) come from there; this image only adds a thin
# entrypoint that pins the v1 KNN configuration and follows the LAFA
# bind-mount conventions from ``anphan0828/LAFA_container_guide``.
#
# Run example (matches LAFA evaluator harness)::
#
#     docker run --rm \
#         -v /path/to/protea-frozen-v226-2025-05-03:/bundle:ro \
#         -v $HOME/.cache/huggingface:/hf-cache \
#         -v $PWD/queries.fasta:/input/queries.fasta:ro \
#         -v $PWD/out:/output \
#         ghcr.io/frapercan/protea/knn-v1:latest
#
# The default ENTRYPOINT runs ``protea-predict`` with the v1 KNN flags
# (``--aspect_separated --no_v6 --no_reranker``) against the
# bind-mounted bundle and writes ``/output/predictions.tsv``. Callers
# can override flags by appending positional args to ``docker run``.

ARG METHOD_RUNTIME_REF=latest
FROM ghcr.io/frapercan/protea/method-runtime:${METHOD_RUNTIME_REF} AS runtime

# Bind-mount points expected by the LAFA evaluator. We create them
# under root before dropping privileges so the runtime ``protea`` user
# does not need to be able to mkdir at run time.
USER root
RUN mkdir -p /input /output /bundle /hf-cache \
    && chown -R protea:protea /input /output /bundle /hf-cache

WORKDIR /app
COPY apps/lafa_knn_v1/lafa_entrypoint.sh /app/lafa_entrypoint.sh
RUN chmod 0755 /app/lafa_entrypoint.sh

# Override the base image labels with the v1 KNN identity.
LABEL org.opencontainers.image.title="protea-knn-v1" \
      org.opencontainers.image.description="PROTEA F-LAFA v2 submission: ProtT5 + KNN baseline (no v6, no reranker)." \
      org.opencontainers.image.source="https://github.com/frapercan/PROTEA" \
      org.opencontainers.image.licenses="MIT" \
      net.functionbench.lafa.method="protea-knn-v1" \
      net.functionbench.lafa.adr="D23"

ENV HF_CACHE=/hf-cache \
    PROTEA_KNN_V1_BUNDLE=/bundle \
    PROTEA_KNN_V1_QUERY=/input/queries.fasta \
    PROTEA_KNN_V1_OUTPUT=/output/predictions.tsv

USER protea
ENTRYPOINT ["/app/lafa_entrypoint.sh"]
