"""Bulk DB loaders extracted out of ``training_dump_helpers``.

Three private helpers used by the dump pipeline:

* :func:`_count_embeddings_with_dim`: single COUNT + ``vector_dims``
  probe for a given embedding-config UUID.
* :func:`_stream_embeddings`: pre-allocated ``np.empty`` matrix filled
  via ``yield_per`` over the ``sequence_embedding`` join.
* :func:`_load_annotation_aggregations`: per-aspect grouping of
  annotation rows under the GO ``P/F/C`` aspects, filtering against a
  provided ``acc_to_idx`` so only proteins with a preloaded embedding
  contribute to the reference set.

All three are intentionally I/O-only: they take a SQLAlchemy
``Connection`` (already opened by the caller) and return plain Python /
NumPy structures. Keeping them out of ``training_dump_helpers`` lets
``_preload_all_embeddings`` and ``_build_reference_from_cache`` stay
under the §3 60-LOC method ceiling without the dump-helper file
ballooning further.
"""

from __future__ import annotations

import uuid
from typing import Any

import numpy as np
from sqlalchemy import text
from sqlalchemy.engine import Connection

from protea.core.annotation_intern import intern_string
from protea.core.domain.aspect import ASPECT_CODES as _ASPECTS


def _count_embeddings_with_dim(
    conn: Connection, emb_config_id: uuid.UUID
) -> tuple[int, int]:
    """Return ``(total_rows, embedding_dim)`` for a config.

    ``embedding_dim`` falls back to 960 when the lookup probe finds no
    row for the config (e.g. on an empty fixture); callers can rely on
    a sane default rather than ``None`` propagating through the
    pre-allocation step.
    """
    count_row = conn.execute(
        text(
            "SELECT COUNT(*), "
            "       (SELECT vector_dims(se2.embedding) "
            "          FROM sequence_embedding se2 "
            "         WHERE se2.embedding_config_id = :ecid LIMIT 1) "
            "  FROM protein p "
            "  JOIN sequence_embedding se "
            "    ON se.sequence_id = p.sequence_id "
            "   AND se.embedding_config_id = :ecid"
        ),
        {"ecid": emb_config_id},
    ).one()
    total = int(count_row[0])
    dim = int(count_row[1]) if count_row[1] else 960
    return total, dim


def _stream_embeddings(
    conn: Connection,
    emb_config_id: uuid.UUID,
    total: int,
    dim: int,
    stream_chunk: int,
) -> tuple[np.ndarray, list[str]]:
    """Stream all rows into a pre-allocated float16 matrix.

    Two-pass strategy (count + stream) is intentional: a single-pass
    ``list.append`` of NumPy rows would balloon memory while the JIT
    grows the buffer; pre-allocation caps the working set at
    ``total * dim * 2`` bytes plus the row cursor.
    """
    embeddings = np.empty((total, dim), dtype=np.float16)
    accessions: list[str] = []
    result_proxy = conn.execute(
        text(
            "SELECT p.accession, se.embedding "
            "  FROM protein p "
            "  JOIN sequence_embedding se "
            "    ON se.sequence_id = p.sequence_id "
            "   AND se.embedding_config_id = :ecid"
        ),
        {"ecid": emb_config_id},
    ).yield_per(stream_chunk)

    for i, (acc, emb_str) in enumerate(result_proxy):
        if isinstance(emb_str, str):
            emb_arr = np.fromstring(emb_str.strip("[]"), sep=",", dtype=np.float16)
        else:
            emb_arr = np.array(emb_str, dtype=np.float16)
        embeddings[i] = emb_arr
        accessions.append(acc)

    return embeddings, accessions


def _load_annotation_aggregations(
    conn: Connection,
    annotation_set_id: uuid.UUID,
    acc_to_idx: dict[str, int],
) -> tuple[
    dict[str, set[str]],
    dict[str, dict[str, list[dict[str, Any]]]],
]:
    """Group annotation rows by GO aspect (``P``/``F``/``C``).

    Returns ``(aspect_accs, aspect_go_map)``:

    * ``aspect_accs[aspect]``: set of accessions with at least one
      annotation of that aspect AND a preloaded embedding (i.e. the
      accession is present in ``acc_to_idx``).
    * ``aspect_go_map[aspect][acc]``: list of GO term records keyed by
      accession. Qualifier and evidence-code strings are interned via
      ``annotation_intern.intern_string`` to avoid duplicating the same
      tokens across millions of rows.

    NOT-qualified rows are skipped at the SQL layer (any qualifier
    containing ``NOT`` is excluded), matching the upstream contract.
    """
    ann_rows = conn.execute(
        text(
            "SELECT pga.protein_accession, gt.aspect, pga.go_term_id, "
            "       pga.qualifier, pga.evidence_code "
            "  FROM protein_go_annotation pga "
            "  JOIN go_term gt ON gt.id = pga.go_term_id "
            " WHERE pga.annotation_set_id = :asid "
            "   AND gt.aspect IN ('P', 'F', 'C') "
            "   AND (pga.qualifier IS NULL OR pga.qualifier NOT LIKE '%%NOT%%')"
        ),
        {"asid": annotation_set_id},
    ).yield_per(50_000)

    aspect_accs: dict[str, set[str]] = {a: set() for a in _ASPECTS}
    aspect_go_map: dict[str, dict[str, list[dict[str, Any]]]] = {a: {} for a in _ASPECTS}
    for acc, asp, go_term_id, qualifier, evidence_code in ann_rows:
        if asp in aspect_accs and acc in acc_to_idx:
            aspect_accs[asp].add(acc)
            aspect_go_map[asp].setdefault(acc, []).append(
                {
                    "go_term_id": go_term_id,
                    "qualifier": intern_string(qualifier),
                    "evidence_code": intern_string(evidence_code),
                }
            )

    return aspect_accs, aspect_go_map
