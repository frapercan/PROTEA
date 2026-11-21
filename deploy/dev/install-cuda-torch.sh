#!/usr/bin/env bash
# The CUDA build of the torch and torchvision a venv already holds, for a node with
# an NVIDIA card.
#
# poetry.lock pins both from the CPU index, which is what CI and every node without
# a card install. This replaces the two with the CUDA build of the SAME versions and
# adds the CUDA runtime libraries and triton; nothing else in the venv moves. The
# exact neighbour search returns the same neighbours on either build; the card only
# makes its first pass faster.
#
# Usage: deploy/dev/install-cuda-torch.sh [VENV] [CUDA_TAG]
#   VENV      the venv (default ~/protea-dev/venv)
#   CUDA_TAG  the PyTorch index tag (default cu130); the driver must support it
set -euo pipefail

venv="${1:-$HOME/protea-dev/venv}"
tag="${2:-cu130}"
read -r torch_version vision_version < <("$venv/bin/python" - <<'PY'
from importlib.metadata import version

print(version("torch").split("+")[0], version("torchvision").split("+")[0])
PY
)

uv pip install --python "$venv/bin/python" \
    "torch==${torch_version}+${tag}" "torchvision==${vision_version}+${tag}" \
    --index-url "https://download.pytorch.org/whl/${tag}" \
    --extra-index-url https://pypi.org/simple --index-strategy unsafe-best-match
"$venv/bin/python" -c 'import torch; print("torch", torch.__version__, "cuda", torch.cuda.is_available())'
