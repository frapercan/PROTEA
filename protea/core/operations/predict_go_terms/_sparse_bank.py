"""A k-WTA bank is the wrong SHAPE for a dense copy, not merely a large one.

Measured over the whole store 2026-10-28: the ``learned-code`` bank is 684.048
rows of 8192 dimensions with exactly 64 non-zeros each. Dense float32 is
22,41 GB, and ``disk_cache._derive_reference_views`` built the f32 copy
unconditionally and only dropped it at the return, so it held f16 plus two f32
copies at once -- **11,21 + 22,41 + 22,41 = 56,0 GB** on a machine with 30 GiB.
The same rows as a CSR are 0,35 GB, a factor of 64. So the representation this
campaign exists to measure sat in the store with no number against it, and the
reason was never the search: ``protea_method.knn_search``'s numpy backend takes
a ``csr_array`` as it stands.

Its own module because the reasoning is longer than the code, and because
``_batch_op_reference`` is at its file and class size ceilings: inlining this
pushed the file to 832 LOC against a budget of 800 and the class to 522 against
500, which the smell gate refused. The gate was right -- a bank's storage class
is its own subject.
"""

from __future__ import annotations

from typing import Any

import numpy as np
from scipy.sparse import csr_array, issparse
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.embedding.embedding_config import (
    SPARSE_CODE_BACKENDS,
    EmbeddingConfig,
)


def bank_is_sparse(session: Session, embedding_config_id: Any) -> bool:
    """Whether this config's rows are k-WTA codes rather than dense vectors.

    Read off the config's BACKEND and never off its dimension: a wide dense
    bank is expensive and still dense, and the two want opposite treatment.
    :data:`SPARSE_CODE_BACKENDS` names the producers whose writer applies a
    top-k, so the tag is evidence about the shape of the rows.

    A config that is absent reads as dense. Guessing sparse for a bank nobody
    can describe would hand the search a CSR built from rows whose producer is
    unknown; dense is the branch that has always run.
    """
    config = session.get(EmbeddingConfig, embedding_config_id)
    return config is not None and config.model_backend in SPARSE_CODE_BACKENDS


def bank_bytes(bank: Any) -> int:
    """How much memory a reference bank holds, dense or sparse.

    ``.nbytes`` is a dense attribute and a CSR has none, so the size line of
    ``load_references_done`` raised rather than reporting. The three arrays are
    the whole of a CSR's footprint.
    """
    if issparse(bank):
        return int(bank.data.nbytes + bank.indices.nbytes + bank.indptr.nbytes)
    return int(bank.nbytes)


def empty_sparse_bank() -> csr_array:
    """A sparse bank with no rows, for a pool whose count came back zero.

    Shaped ``(0, 0)`` because the dimension is read off the first row and there
    is no first row. The dense path's empty value is a 1-D ``np.empty((0,))``
    for the same reason: neither carries a width nobody measured.
    """
    return csr_array((0, 0), dtype=np.float32)


def stream_sparse_pool(
    rows: Any,
    *,
    dim: int,
    n_accessions_hint: int,
) -> tuple[list[str], csr_array]:
    """Accumulate ``(accession, embedding)`` rows into a float32 CSR.

    ``rows`` is any iterable of ``(accession, HalfVector)`` already in bank
    order; ordering is the caller's business and is load-bearing -- see
    ``_bank_order``, which pins ``ORDER BY accession COLLATE "C"`` because 91%
    of queries have neighbours at distance ~0 and candidates tie at the k-th
    slot. Row ``i`` of the result must be ``accessions[i]``, which is how
    ``search_knn`` names its hits.

    THE SHAPE COMES FROM THE ROWS RECEIVED, not from a count taken separately,
    and that is a deliberate difference from the dense path. The dense one
    pre-allocates ``COUNT(*)`` rows and fills them by position, so a stream
    that yields fewer rows than the count leaves trailing ALL-ZERO rows in the
    bank; after normalisation those are unit vectors and can be returned as
    neighbours. Here the same disagreement is a shorter bank and never a silent
    fake row. ``n_accessions_hint`` is only for the ``indptr`` reservation.

    float32 and not float16 because scipy.sparse has no float16 at all. In
    production the values come from a ``halfvec`` column either way, so the
    numbers agree with the dense path and only the container's dtype differs.
    """
    data: list[np.ndarray] = []
    indices: list[np.ndarray] = []
    indptr: list[int] = [0]
    accessions: list[str] = []
    for acc, emb in rows:
        row = emb.to_numpy()
        nz = np.flatnonzero(row)
        indices.append(nz.astype(np.int32))
        data.append(row[nz].astype(np.float32))
        indptr.append(indptr[-1] + int(nz.size))
        accessions.append(acc)
    codes = csr_array(
        (
            np.concatenate(data) if data else np.empty(0, dtype=np.float32),
            np.concatenate(indices) if indices else np.empty(0, dtype=np.int32),
            np.asarray(indptr, dtype=np.int64),
        ),
        shape=(len(accessions), dim),
        dtype=np.float32,
    )
    return accessions, codes


#: Why a sparse bank does not go through the disk cache, written once.
#:
#: The cache stores and reads ``np.save`` / ``np.load`` files. Its own module
#: documents a constant that must be bumped "when the MEANING of what is cached
#: changes", and records two prior incidents where it was not and stale files
#: were served under a still-valid key. A format change there also turns any
#: mismatch into a silent permanent MISS, because the read is wrapped in a bare
#: ``except Exception: return None``.
#:
#: What it costs to go without, measured 2026-10-28: 10,1 minutes for the whole
#: 684.048 row bank at 0,88 ms a row, against 12,6 minutes for the dense
#: ankh-base bank that DOES get cached. The sparse bank is not the expensive one
#: to load; it is the one whose cached format has not been designed yet.
NO_DISK_CACHE_FOR_SPARSE = (
    "a sparse bank is streamed from the database every time: 10,1 min measured "
    "for 684.048 rows, against a cached format that has not been designed"
)
