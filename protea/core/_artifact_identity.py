"""An artefact's identity, and the only way to read one.

A ``*_uri`` is a LOCATOR and a sha256 is an IDENTITY, and a row that carries
the first without the second cannot tell a swapped artefact from the right
one. The project already knows this in three places -- ``obo_sha256``,
``information_accretion_set.content_sha256``, ``residue_code_shards`` -- and
each of them implemented the hashing and the comparison again. There were
five hand-rolled ``hashlib.sha256`` call sites and no shared module when this
one was written.

WHY ONE FUNCTION AND NOT A MIXIN. The obvious move is a declarative mixin
giving every set a ``{thing}_uri`` and a ``{thing}_sha256``. It was considered
and left: a mixin fixes the SPELLING of two columns, and what goes wrong is
not the spelling. ``evaluation_set`` had the uri and no sha at all;
``dataset`` has three uris, one of which is paired and two of which are not.
What enforces the PAIRING is ``scripts/check_sets.py``, which reads the
schema and refuses a uri with no matching digest. So the shared piece worth
having is not the declaration, it is :func:`fetch_and_verify`: the single
read path, so that "fetch without verifying" stops being expressible.

WHAT A MISSING EXPECTATION MEANS, because this is the part that could rot.
``expected`` is ``None`` for every row written before its table had the
column, and those rows are real: 133 evaluation sets predate this one. A
``None`` therefore CANNOT raise, or the fix would break reading every
existing window. It is reported instead, through ``on_unverified``.

AND HERE IS WHAT THIS DOCSTRING USED TO CLAIM AND THE CODE DID NOT DO. It
said "the generator refuses, a reader warns". No generator path calls this at
all: the generator WRITES the digest, it never reads one back. Every caller
today is a reader and every one of them warns. The sentence described a
design, not the code, which is the same thing the reuse docstring did two
files away. What is still missing, and is a separate change with its own
name, is that a caller which PRODUCES A NUMBER should record whether it
verified, in its own row rather than in a warning: a warning in a worker
reaches the journal and in the API reaches nobody.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from pathlib import Path
from typing import Any

#: Read in blocks rather than whole: a ground-truth parquet of the long
#: window is tens of megabytes, and /tmp on the server is a 16 GB tmpfs
#: carved out of the machine's 30.
_BLOCK = 1024 * 1024


class ArtifactMismatch(RuntimeError):
    """The bytes under a key are not the bytes a row says live there.

    Its own class because the caller's reaction is not the caller's choice:
    an evaluation that scored against a swapped truth produced a number about
    something else, and that has to stop the run rather than warn in a log
    nobody reads.
    """


def sha256_of_bytes(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


def sha256_of_file(path: Path) -> str:
    """The digest of a file, read in blocks."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_BLOCK):
            digest.update(chunk)
    return digest.hexdigest()


def fetch_and_verify(
    store: Any,
    key: str,
    expected: str | None,
    label: str,
    on_unverified: Callable[[str], None] | None = None,
) -> bytes:
    """The bytes under ``key``, checked against ``expected``.

    THE ONLY WAY A VERIFIED ARTEFACT SHOULD BE READ. Calling ``store.get``
    directly is what this exists to replace: it is not that the old code
    verified badly, it is that there was nothing to verify against and the
    read looked identical to a verified one.

    ``expected`` of ``None`` means the row predates its sha column. It does
    not raise -- 133 windows are in that state -- and it does not pass in
    silence either: ``on_unverified`` is called with a sentence saying which
    artefact went unchecked, so the caller decides whether that is a warning
    or a refusal.
    """
    blob = store.get(key)
    if expected is None:
        if on_unverified is not None:
            on_unverified(
                f"{label} was read WITHOUT verification: the row carries no sha256, so "
                "it predates the column. The bytes may be the right ones and nothing "
                "here can say so."
            )
        return blob
    got = sha256_of_bytes(blob)
    if got != expected:
        raise ArtifactMismatch(
            f"{label} does not match what its row declares. The row says "
            f"{expected[:16]}... and the bytes under {key!r} hash to {got[:16]}.... "
            "Reading them anyway would score against a truth nobody sealed, so this "
            "refuses. Either the object was replaced, or the row belongs to a "
            "different artefact."
        )
    return blob
