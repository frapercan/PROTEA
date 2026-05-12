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
#   VENV_PATH=/tmp/torch-smoke bash scripts/install_gpu_torch.sh
#
# Why no ``--no-deps``: torch on Linux ships its CUDA runtime through the
# ``nvidia-*`` PyPI packages (nvidia-cudnn-cu12, nvidia-cublas-cu12,
# nvidia-cuda-runtime-cu12, ...). Without those, ``import torch`` fails at
# load time with ``ImportError: libcudnn.so.9: cannot open shared object
# file``. A previous version of this script passed ``--no-deps`` to keep
# the poetry-resolved environment untouched; that left fresh GPU deploys
# unable to import torch at all (see deploy-keeper incident 2026-05-12,
# job 384fa8de). Letting pip resolve the runtime deps is the correct fix;
# any unrelated bumps (sympy, networkx, MarkupSafe) stay inside the
# permissive ranges declared in ``pyproject.toml``.
set -euo pipefail

CUDA_VARIANT="${CUDA_VARIANT:-cu121}"
INDEX_URL="https://download.pytorch.org/whl/${CUDA_VARIANT}"

# Allow callers to point at a venv directly (smoke tests, CI) without
# requiring poetry. Fall back to ``poetry env info --path`` for the normal
# deploy path.
VENV_PATH="${VENV_PATH:-}"
if [[ -z "${VENV_PATH}" ]]; then
    if ! command -v poetry >/dev/null 2>&1; then
        echo "poetry is required on PATH (or set VENV_PATH)" >&2
        exit 1
    fi
    VENV_PATH="$(poetry env info --path 2>/dev/null || true)"
fi

if [[ -z "${VENV_PATH}" || ! -x "${VENV_PATH}/bin/pip" ]]; then
    echo "no usable virtualenv at '${VENV_PATH}' (run 'poetry install' or set VENV_PATH)" >&2
    exit 1
fi

echo ">>> overriding torch + torchvision with ${CUDA_VARIANT} wheels (with deps)"
# The PyTorch wheel index mirrors the runtime deps torch needs
# (sympy, networkx, MarkupSafe, jinja2, filelock, fsspec, nvidia-*),
# so a single ``--index-url`` is enough. Avoid ``--extra-index-url``
# here: PyPI may carry a newer torch built against a different CUDA
# than ${CUDA_VARIANT}, and pip would happily pick it up.
"${VENV_PATH}/bin/pip" install --upgrade --force-reinstall \
    --index-url "${INDEX_URL}" \
    torch torchvision

echo ">>> installed:"
"${VENV_PATH}/bin/pip" list --format=columns \
    | grep -Ei '^(torch|torchvision|triton|nvidia-)' \
    || true

echo ">>> import torch self-check:"
"${VENV_PATH}/bin/python" - <<'PY'
import torch
print(f"torch {torch.__version__} cuda_available={torch.cuda.is_available()}")
PY
