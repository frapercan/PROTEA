# syntax=docker/dockerfile:1.7
#
# protea-knn-8plm - F-LAFA v2 submission #2 (ADR-D23).
#
# 8-PLM ensemble layered on top of ``protea-method-runtime`` (ADR-D15).
# Eight protein language models embed the queries, an aspect-separated
# KNN runs per PLM against per-PLM frozen reference matrices, and the
# ensemble driver combines candidate sets by score-level mean (default,
# overridable to max via --aggregation).
#
# This image inherits the heavy dependency graph (torch +
# transformers + protea-method + ProtT5 encoder) from the base. It adds:
#
#   * ensemble_driver.py: per-PLM KNN loop + score-fusion aggregator,
#   * plm_encoders.py:    canonical 8-PLM registry (esm2 150M / 650M /
#                         3B, ProtT5, ProstT5, Ankh-base, Ankh-large,
#                         ESM-C 600M),
#   * lafa_entrypoint.sh: pins the bind-mount layout from the LAFA
#                         container guide and forwards positional args
#                         to the driver,
#   * apt deps required by the ESM SDK (ESM-C path).
#
# Run example (matches LAFA evaluator harness)::
#
#     docker run --rm \
#         -v /path/to/protea-frozen-v226-8plm:/bundle:ro \
#         -v $HOME/.cache/huggingface:/hf-cache \
#         -v $PWD/queries.fasta:/input/queries.fasta:ro \
#         -v $PWD/out:/output \
#         ghcr.io/frapercan/protea/knn-8plm:latest
#
# Sweep at runtime without rebuilding::
#
#     docker run --rm <mounts> ghcr.io/frapercan/protea/knn-8plm:latest \
#         --aggregation max --k 10 --plms esm2_650m,prot_t5,ankh_large

ARG METHOD_RUNTIME_REF=latest
FROM ghcr.io/frapercan/protea/method-runtime:${METHOD_RUNTIME_REF} AS runtime

USER root

# Bind-mount points expected by the LAFA evaluator.
RUN mkdir -p /input /output /bundle /hf-cache \
    && chown -R protea:protea /input /output /bundle /hf-cache

# ESM-C (Cambrian) path requires the ``esm`` SDK from EvolutionaryScale;
# everything else (ESM-2 / ProtT5 / ProstT5 / Ankh) is already covered
# by the transformers install in the base image. ``esm`` carries a
# small native build step on some platforms, hence the explicit pin.
RUN pip install --no-cache-dir "esm>=3.0.0,<4.0.0"

# Ensure the apps/ package layout works with ``python -m
# apps.lafa_knn_8plm.ensemble_driver``: the entrypoint imports
# ``apps.lafa_knn_8plm.plm_encoders`` and ``prott5_encoder`` (vendored
# in the base image at /app/prott5_encoder.py).
WORKDIR /app
RUN mkdir -p /app/apps/lafa_knn_8plm
COPY apps/lafa_knn_8plm/__init__.py /app/apps/lafa_knn_8plm/__init__.py
COPY apps/lafa_knn_8plm/plm_encoders.py /app/apps/lafa_knn_8plm/plm_encoders.py
COPY apps/lafa_knn_8plm/ensemble_driver.py /app/apps/lafa_knn_8plm/ensemble_driver.py
COPY apps/lafa_knn_8plm/lafa_entrypoint.sh /app/lafa_entrypoint.sh
RUN touch /app/apps/__init__.py \
    && chmod 0755 /app/lafa_entrypoint.sh \
    && chown -R protea:protea /app/apps /app/lafa_entrypoint.sh

# Override the base image labels with the 8plm ensemble identity.
LABEL org.opencontainers.image.title="protea-knn-8plm" \
      org.opencontainers.image.description="PROTEA F-LAFA v2 submission: 8-PLM ensemble (esm2 150M/650M/3B, ProtT5, ProstT5, Ankh-base/large, ESM-C 600M), aspect-separated KNN, score-level mean fusion." \
      org.opencontainers.image.source="https://github.com/frapercan/PROTEA" \
      org.opencontainers.image.licenses="MIT" \
      net.functionbench.lafa.method="protea-knn-8plm" \
      net.functionbench.lafa.adr="D23"

ENV HF_CACHE=/hf-cache \
    PROTEA_KNN_8PLM_BUNDLE=/bundle \
    PROTEA_KNN_8PLM_QUERY=/input/queries.fasta \
    PROTEA_KNN_8PLM_OUTPUT=/output/predictions.tsv \
    PROTEA_KNN_8PLM_AGGREGATION=mean \
    PYTHONPATH=/app

USER protea
ENTRYPOINT ["/app/lafa_entrypoint.sh"]
