#!/usr/bin/env bash
# Override the default CPU torch installed by ``poetry install`` with the
# CUDA build for GPU embedding workers. ``pyproject.toml`` pins torch to
# the ``pytorch-cpu`` source so CI runners and the slim production Docker
# image do not pull the ~6 GB NVIDIA / triton stack. Hosts that actually
# run GPU inference (compute_embeddings backends) need to flip torch back
# to the CUDA wheel after each ``poetry install`` / ``poetry update``.
#
# Usage:
#   bash scripts/install_gpu_torch.sh                 # default cu121
#   CUDA_VARIANT=cu118 bash scripts/install_gpu_torch.sh
#
# The script uses ``--no-deps`` to avoid disturbing the rest of the
# resolved environment (transformers, esm, etc. stay on the versions
# poetry already locked).
set -euo pipefail

CUDA_VARIANT="${CUDA_VARIANT:-cu121}"
INDEX_URL="https://download.pytorch.org/whl/${CUDA_VARIANT}"

if ! command -v poetry >/dev/null 2>&1; then
    echo "poetry is required on PATH" >&2
    exit 1
fi

VENV_PATH="$(poetry env info --path 2>/dev/null || true)"
if [[ -z "${VENV_PATH}" ]]; then
    echo "no poetry virtualenv found — run 'poetry install' first" >&2
    exit 1
fi

echo ">>> overriding torch + torchvision with ${CUDA_VARIANT} wheels (no-deps)"
"${VENV_PATH}/bin/pip" install --no-deps --upgrade --force-reinstall \
    --index-url "${INDEX_URL}" \
    torch torchvision

echo ">>> installed:"
"${VENV_PATH}/bin/pip" list --format=columns | grep -Ei '^(torch|torchvision)\s'
