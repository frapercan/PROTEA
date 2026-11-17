"""The smell budget exempts a Triton kernel from the parameter limit, and only from it.

A ``@triton.jit`` function's arguments are the pointers and strides its launch
passes, so the six-parameter limit cannot hold for it (the author's decision for
ADR-D63). Its length still counts, like any function's.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _checker():
    spec = importlib.util.spec_from_file_location(
        "check_smells", ROOT / "scripts" / "check_smells.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses read their module's namespace
    spec.loader.exec_module(module)
    return module


def _offenders(tmp_path: Path, source: str) -> list[tuple[str, str]]:
    path = tmp_path / "pkg" / "kernels.py"
    path.parent.mkdir()
    path.write_text(source, encoding="utf-8")
    return [(o.kind, o.name) for o in _checker().scan_file(path, tmp_path)]


def test_a_kernel_may_take_many_parameters(tmp_path: Path) -> None:
    source = "import triton\n\n@triton.jit\ndef k(a, b, c, d, e, f, g, h):\n    pass\n"
    assert _offenders(tmp_path, source) == []


def test_a_plain_function_may_not(tmp_path: Path) -> None:
    source = "def k(a, b, c, d, e, f, g, h):\n    pass\n"
    assert _offenders(tmp_path, source) == [("params", "k")]


def test_a_kernel_still_counts_its_length(tmp_path: Path) -> None:
    body = "".join(f"    x{i} = {i}\n" for i in range(70))
    source = f"import triton\n\n@triton.jit\ndef k(a, b, c, d, e, f, g, h):\n{body}"
    assert _offenders(tmp_path, source) == [("method", "k")]
