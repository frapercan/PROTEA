"""The term axis and its ancestor closure, which depend on no window at all.

WHY THESE TWO CAN BE WRITTEN BEFORE THE WINDOWS ARE CHOSEN. Every other table
is keyed by a window or by a sequence. These two are keyed by ``cut`` alone:
they are the vocabulary a model is allowed to name and the subsumption between
those names. Which transitions train the model does not change either one.

MEASURED on the axis ``releases/2022-07-01``, 2026-11-08, with the production
``Dag``:

    terms in the snapshot      43.558, all live, all with an aspect
    direct edges               77.055
    closure pairs             595.072, both ends live in every one

The closure's own docstring says 783.477 "on GO", which is a different release.
That number was quoted and this one was measured, and the two are not the same
quantity.

THE VECTORS ARE NAMED AND VERIFIED, NEVER QUOTED. ``terms`` carries
``vector_uri`` and ``vector_sha256`` for the text-embedding artefact of this
axis, and the operation FETCHES those bytes and checks the digest before
writing either column. A sha copied from a message into a row is a locator for
bytes nobody checked, which is the defect this project has spent a week
closing. On 2026-11-08 that artefact was NOT in the platform's store: 6.426
objects over twelve prefixes and nothing matching it. So ``terms`` refuses
until it is published, and the refusal says which key it looked for.
"""

from __future__ import annotations

import uuid
from typing import Any, NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy.orm import Session

from protea.core._artifact_identity import fetch_and_verify
from protea.core._evaluation_graph import _live_terms_with_aspect, _load_children_by_go_id
from protea.core.ontology.dag import Dag

#: ``terms``: the axis, with the vectors by reference.
TERMS_SCHEMA = pa.schema(
    [
        ("cut", pa.string()),
        ("go_id", pa.string()),
        ("aspect", pa.string()),
        ("vector_uri", pa.string()),
        ("vector_sha256", pa.string()),
    ]
)

#: ``term_ancestors``: the subsumption closure of the axis.
ANCESTORS_SCHEMA = pa.schema(
    [("cut", pa.string()), ("go_id", pa.string()), ("ancestor", pa.string())]
)


class Axis(NamedTuple):
    """The axis read once: its live terms with aspect, and its direct edges."""

    cut: str
    aspect: dict[str, str]
    edges: tuple[tuple[str, str], ...]


def axis_of(session: Session, cut: uuid.UUID) -> Axis:
    """The declared axis, or a refusal if it can name nothing.

    The aspect comes from the same function the evaluation uses for its live
    set, so a term's namespace here is the namespace the truth was classified
    under rather than a second lookup that agrees until it does not.
    """
    live, aspect = _live_terms_with_aspect(session, cut)
    if not live:
        raise ValueError(f"OntologySnapshot {cut} has no live terms, so it cannot be the term axis")
    sin_aspecto = sorted(live - set(aspect))
    if sin_aspecto:
        raise ValueError(
            f"{len(sin_aspecto)} live terms of axis {cut} have no aspect: "
            f"{sin_aspecto[:5]}. A term with no namespace cannot be grouped by one, "
            "and the loader groups by it"
        )
    hijos = _load_children_by_go_id(session, cut)
    edges = tuple((padre, hijo) for padre, conjunto in hijos.items() for hijo in sorted(conjunto))
    return Axis(cut=str(cut), aspect={t: aspect[t] for t in live}, edges=edges)


def verified_vectors(store: Any, key: str, expected_sha: str) -> str:
    """The uri of the axis's text vectors, after checking the bytes are those bytes.

    IT DOWNLOADS TO CHECK, on purpose. ``exists`` would answer whether
    something is at that key, which is not the question: the question is
    whether the sha this table is about to publish describes what is there.
    The artefact is a few hundred megabytes from a MinIO on this machine, so
    the check costs seconds and buys the only property the column has.
    """
    fetch_and_verify(store, key, expected_sha, f"the term vectors at {key}")
    return store.url(key)


def write_terms(axis: Axis, vector_uri: str, vector_sha256: str, path: str) -> int:
    """One row per live term of the axis, sorted by id."""
    ids = sorted(axis.aspect)
    pq.write_table(
        pa.table(
            {
                "cut": [axis.cut] * len(ids),
                "go_id": ids,
                "aspect": [axis.aspect[t] for t in ids],
                "vector_uri": [vector_uri] * len(ids),
                "vector_sha256": [vector_sha256] * len(ids),
            },
            schema=TERMS_SCHEMA,
        ),
        path,
    )
    return len(ids)


def write_ancestors(axis: Axis, path: str) -> int:
    """Every ``(go_id, ancestor)`` the direct edges imply, within the axis.

    A pair with an end outside the axis is dropped rather than written: the
    loader draws from ``terms``, so an ancestor it cannot name is a row it
    would join to nothing. On this axis there are none, measured, and the
    filter is what keeps that true rather than assumed.
    """
    dag = Dag(edges=axis.edges)
    dentro = set(axis.aspect)
    pares = sorted(
        (descendiente, ancestro)
        for ancestro, descendiente in dag.closure()
        if ancestro in dentro and descendiente in dentro
    )
    pq.write_table(
        pa.table(
            {
                "cut": [axis.cut] * len(pares),
                "go_id": [d for d, _ in pares],
                "ancestor": [a for _, a in pares],
            },
            schema=ANCESTORS_SCHEMA,
        ),
        path,
    )
    return len(pares)
