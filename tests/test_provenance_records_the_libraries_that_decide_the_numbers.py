"""Provenance names the library versions, and names them even when absent.

WHY THIS TEST EXISTS. Until 2026-09-15 nothing in PROTEA recorded which
libraries computed a vector. ``capture_provenance`` carried ``python_version``
and never ``transformers`` or ``torch``; ``dependency_revisions`` filters on a
``protea-`` prefix so it cannot see either; ``sequence_embedding`` has no
environment column; and the reuse predicate is existence alone --
``NOT EXISTS (sequence_id, embedding_config_id)`` -- so vectors computed under
two different stacks coexist under one config id with nothing able to tell them
apart.

That is not hypothetical. Both axes were measured on the same day:

* ``transformers`` decides the tokenisation. Between 4.48.1 and 5.17.0 the ankh
  backend turns a 78-residue sequence into 157 tokens instead of 79,
  interleaving ``<unk>`` before every residue, and raises nothing.
* ``torch`` decides the arithmetic. On esmc_600m, moving transformers 4.48.1 ->
  5.17.0 at a fixed torch is byte-identical, while torch 2.12 -> 2.14 moves
  1136 of 1152 coordinates -- still differing after the cast to the float16 the
  database stores.

The absent-package case is the one worth pinning. A probe that omits a key when
the package is missing produces a record that cannot be told apart, later, from
one written before the field existed. ``None`` says "looked, not installed";
a missing key says nothing at all, and the whole point of this field is to be
readable by someone who was not here.

Reading the version from distribution metadata rather than importing is also
load-bearing: importing ``torch`` costs seconds, and this helper is documented
as cheap and safe to call anywhere, including inside operation handlers.
"""

from __future__ import annotations

from typing import Any

import pytest

from protea.core import provenance
from protea.core.provenance import _LIBRARIES, capture_provenance


def test_provenance_carries_a_version_for_every_declared_library() -> None:
    payload = capture_provenance()
    assert "libraries" in payload, "provenance stopped recording the library versions"
    libraries = payload["libraries"]
    assert set(libraries) == set(_LIBRARIES), (
        f"expected a key for each of {_LIBRARIES}, got {sorted(libraries)}"
    )


def test_a_library_that_is_not_installed_records_none_rather_than_vanishing() -> None:
    """A missing key reads as a field nobody captured. None reads as looked-and-absent."""
    original = provenance._LIBRARIES
    provenance._LIBRARIES = (*original, "a-package-that-is-not-installed-anywhere")
    try:
        libraries = capture_provenance()["libraries"]
    finally:
        provenance._LIBRARIES = original

    assert "a-package-that-is-not-installed-anywhere" in libraries
    assert libraries["a-package-that-is-not-installed-anywhere"] is None


def test_the_torch_build_survives_into_the_record() -> None:
    """``2.14.0+cpu`` and ``2.14.0+cu130`` are different answers to the same question.

    The two machines run different torch builds by design -- the server pins the
    CPU wheel, the compute node overrides it with a CUDA one -- so the local
    version segment is the part that says which machine's arithmetic produced a
    vector. Stripping it would throw away the only discriminator this record has.
    """
    torch_version = capture_provenance()["libraries"]["torch"]
    if torch_version is None:
        pytest.skip("torch is not installed in this environment")
    assert torch_version == _installed_version("torch"), (
        "the recorded torch version was normalised; the +cpu / +cuXXX build must survive"
    )


def _installed_version(name: str) -> Any:
    from importlib.metadata import version

    return version(name)
