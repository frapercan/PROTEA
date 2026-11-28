"""Declared samples, drawn in hash order with no random state.

A declared sample is one a later reader can draw again from the same inputs on any
machine. Its members are taken in ascending sha256 of ``seed|scope|accession``,
where the scope is the id of what is sampled (a window, a photo): the same seed
draws independently from two scopes and identically from one, and nothing depends
on a random generator, an iteration order or a platform. The order is the hex
digest's, which is the bytes' order, so a database that sorts the raw digests
(``sha256`` over the same UTF-8 text) draws the same sample.

- :mod:`.window_sample`: the stratified sample of a window's targets
  (``declare_window_sample``).
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable

__all__ = ["HASH_ORDER_SEPARATOR", "hash_order_key", "hash_ordered"]

#: What joins the seed, the scope and the accession in the hashed text.
HASH_ORDER_SEPARATOR = "|"


def hash_order_key(seed: str, scope: str, accession: str) -> str:
    """The key an accession is drawn by: the sha256 of ``seed|scope|accession``, in hex."""
    text = HASH_ORDER_SEPARATOR.join((seed, scope, accession))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def hash_ordered(accessions: Iterable[str], seed: str, scope: str) -> list[str]:
    """The distinct accessions in ascending hash order (the accession breaks a tie)."""
    return sorted(set(accessions), key=lambda a: (hash_order_key(seed, scope, a), a))
