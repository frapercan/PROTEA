# syntax=docker/dockerfile:1.7
#
# protea-method-runtime base image.
#
# Tier 3 distribution channel from ADR-D15. Ships the pure inference path
# (``protea-method``) plus the ProtT5 query encoder and a thin FASTA-in /
# TSV-out CLI (``protea-predict``). The frozen reference bundle is bind
# mounted at runtime via ``--frozen_data_dir``; weights for ProtT5 are
# loaded from a HuggingFace cache (``HF_CACHE`` env var or
# ``--model_dir``).
#
# Role in F-LAFA v2 (ADR-D23): three LAFA submission containers
# (``protea-knn-v1``, ``protea-knn-8plm``, ``protea-v18``) extend this
# image with their own bind-mount layout and frozen artefacts. Keeping
# the runtime separate from the LAFA wrapper means a single rebuild of
# the heavy layers (torch + transformers + protea-method) services all
# three submissions.
#
# Run example::
#
#     docker run --rm \
#         -v /path/to/protea-frozen-v226-2025-05-03:/bundle \
#         -v $HOME/.cache/huggingface:/hf-cache \
#         -e HF_CACHE=/hf-cache \
#         -v $PWD/queries.fasta:/queries.fasta \
#         -v $PWD/predictions.tsv:/predictions.tsv \
#         ghcr.io/frapercan/protea/method-runtime:latest \
#             --query_file /queries.fasta \
#             --frozen_data_dir /bundle \
#             --output /predictions.tsv

FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# System deps:
#  - git: install protea-method via git+https.
#  - ca-certificates: TLS for the install + HuggingFace fetches.
#  - libgomp1: OpenMP runtime required by LightGBM at import time
#    (python:3.12-slim does not ship it; without it lightgbm fails
#    with "libgomp.so.1: cannot open shared object file").
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps. protea-method pulls numpy, pandas, lightgbm,
# faiss-cpu, protea-contracts. The remaining packages cover the ProtT5
# query encoder and the parquet bundle reader. The version pins match
# the LAFA v1 container so downstream LAFA v2 images can rebase here
# without re-vetting the dependency graph.
ARG PROTEA_METHOD_REF=main
RUN pip install \
    "protea-method @ git+https://github.com/frapercan/protea-method.git@${PROTEA_METHOD_REF}" \
    "pyarrow>=14" \
    "transformers>=4.40,<4.50" \
    "torch>=2.0" \
    "sentencepiece>=0.2.1,<0.3"

WORKDIR /app
COPY apps/method_runtime/protea_predict.py /app/protea_predict.py
COPY apps/method_runtime/prott5_encoder.py /app/prott5_encoder.py
COPY apps/method_runtime/self_prior.py /app/self_prior.py
COPY apps/method_runtime/universal_scorer.py /app/universal_scorer.py
COPY apps/method_runtime/__init__.py /app/__init__.py

# Image-level metadata: callers introspect ``LABEL`` keys via
# ``docker inspect`` when assembling a LAFA submission manifest.
LABEL org.opencontainers.image.title="protea-method-runtime" \
      org.opencontainers.image.description="Base runtime for PROTEA inference (KNN + features + reranker, ProtT5 query encoder)." \
      org.opencontainers.image.source="https://github.com/frapercan/PROTEA" \
      org.opencontainers.image.licenses="MIT"

# Drop privileges. /bundle and /hf-cache are bind-mount points the
# caller provides.
RUN useradd --create-home --uid 1000 protea
USER protea

ENTRYPOINT ["python", "/app/protea_predict.py"]
