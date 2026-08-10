# syntax=docker/dockerfile:1.7
#
# protea-sparse-knn: LAFA submission over learned sparse codes.
#
# Ankh-base embeddings projected through a learned sparse encoder, then
# nearest-neighbour GO transfer against a frozen reference bank. One
# backbone, one retrieval pass, all three aspects.
#
# Layered on ``protea-method-runtime`` (ADR-D15) for torch and
# transformers. It adds:
#
#   * sparse_driver.py:    embed, project, retrieve, transfer, propagate,
#   * protea-backends:     the backend that produced the bank, so the
#                          queries are tokenised the same way,
#   * scipy:               the sparse product used for retrieval,
#   * lafa_entrypoint.sh:  the bind-mount layout from the LAFA guide.
#
# Run example (matches the LAFA evaluator harness)::
#
#     docker run --rm \
#         -v /path/to/protea-sparse-knn-bundle:/bundle:ro \
#         -v $HOME/.cache/huggingface:/hf-cache \
#         -v $PWD/queries.fasta:/input/queries.fasta:ro \
#         -v $PWD/go-basic.obo:/input/go-basic.obo:ro \
#         -v $PWD/out:/output \
#         ghcr.io/frapercan/protea/sparse-knn:latest

ARG METHOD_RUNTIME_REF=latest
FROM ghcr.io/frapercan/protea/method-runtime:${METHOD_RUNTIME_REF} AS runtime

USER root

# Bind-mount points expected by the LAFA evaluator.
RUN mkdir -p /input /output /bundle /hf-cache \
    && chown -R protea:protea /input /output /bundle /hf-cache

# protea-backends is the producer of the frozen bank. Pinning the
# resolved commit rather than a branch keeps the image reproducible and
# matches the internal-dependency rule; a branch pin would make that
# branch the delivery channel.
ARG PROTEA_BACKENDS_REF
RUN test -n "${PROTEA_BACKENDS_REF}" \
    || (echo "PROTEA_BACKENDS_REF build arg is required" >&2 && exit 2)
RUN pip install --no-cache-dir \
    "protea-backends @ git+https://github.com/frapercan/protea-backends@${PROTEA_BACKENDS_REF}" \
    "scipy>=1.11"

WORKDIR /app
RUN mkdir -p /app/apps/lafa_sparse_knn
COPY apps/lafa_sparse_knn/__init__.py /app/apps/lafa_sparse_knn/__init__.py
COPY apps/lafa_sparse_knn/sparse_driver.py /app/apps/lafa_sparse_knn/sparse_driver.py
COPY apps/lafa_sparse_knn/lafa_entrypoint.sh /app/lafa_entrypoint.sh
RUN touch /app/apps/__init__.py \
    && chmod 0755 /app/lafa_entrypoint.sh \
    && chown -R protea:protea /app/apps /app/lafa_entrypoint.sh

LABEL org.opencontainers.image.title="protea-sparse-knn" \
      org.opencontainers.image.description="PROTEA LAFA submission: Ankh-base embeddings projected through a learned sparse encoder, nearest-neighbour GO transfer against a frozen bank." \
      org.opencontainers.image.source="https://github.com/frapercan/PROTEA" \
      org.opencontainers.image.licenses="Unlicense" \
      net.functionbench.lafa.method="protea-sparse-knn"

ENV HF_CACHE=/hf-cache \
    PROTEA_SPARSE_BUNDLE=/bundle \
    PROTEA_SPARSE_QUERY=/input/queries.fasta \
    PROTEA_SPARSE_OBO=/input/go-basic.obo \
    PROTEA_SPARSE_OUTPUT=/output/predictions.tsv \
    PYTHONPATH=/app

USER protea
ENTRYPOINT ["/app/lafa_entrypoint.sh"]
