"""How the reference bank crosses the driver: binary if it can, text if it must.

SQLAlchemy asks psycopg for TEXT and offers no option to change it, so a
``halfvec`` arrives as the literal ``[v1,v2,...]`` and the column type parses
one float per dimension in Python. ``register_vector`` has already given the
connection pgvector's binary loaders (see
``infrastructure/database/engine.py``), so the only thing missing is a cursor
that asks for binary, and that has to be the driver's own.

REGISTERING THE ADAPTERS IS NOT THE SAME AS GETTING BINARY, which is the
mistake that cost this module a day: a connection can hold every adapter and
still carry text, and checking the TYPE that comes back proves nothing, because
the ``HalfVector`` is built from the text after it arrives.

MEASURED 2026-11-04 on 20.000 rows of ``f3a7fe16`` (1.536 dims), loopback:

    text    0,307 ms a row
    binary  0,027 ms a row      a factor of 11,2

and the arrays are IDENTICAL BIT FOR BIT -- ``array_equal`` over the uint16
views of 30,7 million float16s, maximum absolute difference 0,0 -- with the
accessions in the same order. They cannot move: the text form is a lossless
decimal rendering of the same float16s, so the two agreeing is a property and
not a lucky sample.

ON THE WIRE it is bytes, not parsing, that decides. A dense 1.536-dim row is
17.992 bytes as text and 3.076 binary, 5,8x, so a 684.048-row bank is 12,3 GB
against 2,1. On the compute node, whose link sustains 5,0 MB/s, that is 41
minutes against 7, and it is why exploration 1's first arm was abandoned after
three loads were cut mid-stream.

NOT FOR THE SPARSE BANK, deliberately. An 8.192-dim k-WTA row is almost all
zeros, a zero renders as one character, and its text is 16.985 bytes against
16.388 binary: nothing to win on the wire. Its PARSE cost is 8.192 floats a row
and would gain, but it goes through ``stream_sparse_pool``, which derives its
shape from the rows it receives, so routing it belongs in its own change.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

__all__ = ["fill_from_binary"]

#: The name of the driver cursor the binary pool read runs on.
_BINARY_CURSOR = "protea_reference_pool"


def _binary_pool_rows(base_q: Any, chunk: int) -> Iterator[tuple[str, Any]]:
    """Yield ``(accession, HalfVector)`` for the pool over a BINARY cursor.

    The cursor is named, so Postgres streams it instead of materialising, and
    it is read-only inside the session\'s transaction.
    """
    session = base_q.session
    compiled = base_q.statement.compile(
        dialect=session.get_bind().dialect,
        compile_kwargs={"render_postcompile": True},
    )
    raw = session.connection().connection.driver_connection
    with raw.cursor(name=_BINARY_CURSOR, binary=True) as cur:
        cur.itersize = chunk
        cur.execute(str(compiled), compiled.params)
        yield from cur


def fill_from_binary(
    base_q: Any, embeddings: np.ndarray, chunk: int, total: int
) -> list[str] | None:
    """Fill ``embeddings`` over a binary cursor, or return ``None`` to fall back.

    INSIDE A SAVEPOINT, which is the whole reason this is a separate method.
    A driver that cannot give a binary cursor, or a statement psycopg
    compiles differently from SQLAlchemy, raises inside a transaction and
    poisons it: the text retry would then fail too, on an error that has
    nothing to do with it. ``begin_nested`` rolls back to the savepoint and
    leaves the operation's transaction usable, so the fallback is a real
    fallback and not a second way to die.

    It writes into the caller's array and the fallback overwrites from index
    zero, so a failure part way through costs the rows read and nothing else.
    Returning ``None`` rather than raising keeps the decision at the call
    site, where both paths are visible side by side.

    AND IT COUNTS THE ROWS, which a first version did not and a test caught.
    ``embeddings`` is allocated at ``total`` from the same query's
    ``count()``; a cursor that hands back fewer rows would leave the tail as
    whatever ``np.empty`` found in memory, with an accession list too short
    to notice, and the bank would be silently part garbage. A mismatch is
    not recoverable here and not worth guessing about, so it falls back and
    says so.

    The fallback is NOT silent: it warns, and the route reaches
    ``load_references_done`` as ``transfer``, so a bank that quietly went
    back to text shows up in the job's own record instead of as an
    unexplained slow load. That event is what the exploration's bank rule
    reads.
    """
    try:
        with base_q.session.begin_nested():
            accessions: list[str] = []
            for i, (acc, emb) in enumerate(_binary_pool_rows(base_q, chunk)):
                embeddings[i] = emb.to_numpy()
                accessions.append(acc)
            if len(accessions) != total:
                raise RuntimeError(
                    f"the binary cursor gave {len(accessions)} rows where the "
                    f"query counted {total}; the bank would be part garbage"
                )
            return accessions
    except Exception:
        logger.warning(
            "the reference pool could not be read over a binary cursor, so it "
            "falls back to text: one float per dimension parsed in Python, and "
            "5,8x the bytes on the wire for a dense bank. Measured 11,2x slower.",
            exc_info=True,
        )
        return None
