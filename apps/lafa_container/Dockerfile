# syntax=docker/dockerfile:1.7
#
# LAFA-compatible PROTEA container.
#
# The image bundles the inference pipeline (protea-method) and the
# ProtT5 query encoder. Frozen reference data (embeddings, annotations,
# PCA state, Anc2Vec dictionary, reranker booster) is *not* baked in;
# it is bind-mounted at runtime via ``--frozen_data_dir``. ProtT5 model
# weights are also expected via a HuggingFace cache (set ``HF_CACHE`` or
# pass ``--model_dir`` at runtime, mounted from the host).
#
# Built and pushed to ghcr.io by ``.github/workflows/lafa-container.yml``
# on push-to-main, release, and manual dispatch. Tags follow PROTEA's
# semver releases so the LAFA container version line up with the
# inference-path code.
#
# Run example::
#
#     docker run --rm \
#         -v /path/to/protea-frozen-v226-2025-05-03:/bundle \
#         -v $HOME/.cache/huggingface:/hf-cache \
#         -e HF_CACHE=/hf-cache \
#         -v $PWD/queries.fasta:/queries.fasta \
#         -v $PWD/predictions.tsv:/predictions.tsv \
#         ghcr.io/frapercan/protea/lafa:latest \
#             --query_file /queries.fasta \
#             --frozen_data_dir /bundle \
#             --output_baseline /predictions.tsv \
#             --aspect_separated

FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# git is needed to install protea-method via git+https.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps. protea-method pulls numpy, pandas, lightgbm,
# faiss-cpu, protea-contracts. The remaining packages cover the ProtT5
# query encoder and the parquet bundle reader.
ARG PROTEA_METHOD_REF=master
RUN pip install \
    "protea-method @ git+https://github.com/frapercan/protea-method.git@${PROTEA_METHOD_REF}" \
    "pyarrow>=14" \
    "transformers>=4.30" \
    "torch>=2.0" \
    "sentencepiece>=0.1.99"

WORKDIR /app
COPY apps/lafa_container/protea_main.py /app/protea_main.py
COPY apps/lafa_container/prott5_encoder.py /app/prott5_encoder.py
COPY apps/lafa_container/__init__.py /app/__init__.py

# Drop privileges. /bundle and /hf-cache are bind-mount points the
# caller provides; the LAFA evaluator runs the container with read-only
# bundle access by default.
RUN useradd --create-home --uid 1000 protea
USER protea

ENTRYPOINT ["python", "/app/protea_main.py"]
