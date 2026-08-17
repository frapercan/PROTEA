# protea/core/operations/export_gate_bundle.py
"""Freeze everything the encoder gates need into one binary artifact, and read the store once.

The two gates that decide whether the learned representation programme has a
result at all are cheap in compute and expensive in access: six fits for the
fit-and-index disjointness condition, forty for the objective battery, and both
need mean-pooled embeddings and propagated GO closures for a reference pool.

Those live in the database, the database lives on the other machine, and the
standing rule here is that agents are never pointed at it. So the gates cannot
run where the graphics card is, and the card is where they belong.

This operation breaks that by making the database read a published artifact
instead of a live connection. It runs once, on the machine that owns the store,
and writes a single compressed archive that the lab loads with numpy. Nothing
downstream needs a connection, the bundle is content-addressed by the payload
that produced it, and a rerun on the same inputs writes the same bytes.

Binary rather than a text pull, and that is not a preference. A halfvec rendered
as text measures about 50,000 bytes per vector at 2560 dimensions, so a
corpus-scale reference pool crosses the wireless link at tens of gigabytes and
arrives as a string that has to be parsed back. Compressed float32 is roughly
one twentieth of that and is already the shape the consumer wants.

It writes no rows. The only side effect is the artifact.
"""

from __future__ import annotations

import hashlib
import io
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

import numpy as np
from pydantic import Field
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import (
    EmitFn,
    Operation,
    OperationResult,
    ProteaPayload,
)
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.storage import get_artifact_store

PositiveInt = Annotated[int, Field(gt=0)]

#: Reference accessions, ordered, so a rerun on the same inputs selects the same
#: pool. The lab's own draw had no ORDER BY and a fixed seed therefore shuffled a
#: differently ordered list on every run; the same defect here would make the
#: bundle irreproducible in a way nothing downstream could detect.
_REF_ACCESSIONS = """
    SELECT DISTINCT protein_accession
      FROM protein_go_annotation
     WHERE annotation_set_id = :ann
     ORDER BY protein_accession
"""

#: Leaf annotations for the pool. Propagation happens in the consumer, against
#: the ontology snapshot it is already loading, because propagating here would
#: freeze a closure against an OBO the consumer cannot see.
_LEAF_ANNOTATIONS = """
    SELECT pga.protein_accession, gt.go_id
      FROM protein_go_annotation pga
      JOIN go_term gt ON gt.id = pga.go_term_id
     WHERE pga.annotation_set_id = :ann
       AND pga.protein_accession = ANY(:accs)
"""

#: Mean-pooled vector per accession, averaged over chunks in index order. A
#: chunked config writes one row per chunk, so a consumer that took the first
#: row would silently use a prefix of long proteins and the whole of short ones.
_MEAN_EMBEDDINGS = """
    SELECT p.accession, se.chunk_index_s, se.embedding::text
      FROM protein p
      JOIN sequence_embedding se ON se.sequence_id = p.sequence_id
     WHERE se.embedding_config_id = :cfg
       AND p.accession = ANY(:accs)
     ORDER BY p.accession, se.chunk_index_s
"""


class ExportGateBundlePayload(ProteaPayload, frozen=True):
    """What to freeze.

    ``queries`` are the proteins the gates score; everything else becomes the
    reference pool they retrieve from, minus the queries and minus anything
    sharing a sequence with one.
    """

    embedding_config_id: str
    annotation_set_id: str
    queries: list[str]
    ref_n: PositiveInt = 60000
    seed: int = 42
    output_name: str = "gate-bundle"


def _parse_vector(raw: str) -> np.ndarray:
    return np.fromstring(raw.strip()[1:-1], sep=",", dtype=np.float32)


def _twin_accessions(session: Session, queries: list[str]) -> set[str]:
    """Every accession sharing a sequence with a query, including the queries.

    The bank is keyed by protein and embeddings key on sequence, so an accession
    that merely shares a sequence with a query arrives as a donor at cosine
    exactly 1.0 and is a guaranteed rank one. Excluding by accession alone let
    that through, and at K equal to 3 it is a third of the vote.
    """
    rows = session.execute(
        select(Protein.accession)
        .where(
            Protein.sequence_id.in_(
                select(Protein.sequence_id).where(Protein.accession.in_(queries))
            )
        )
    ).scalars().all()
    return set(rows) | set(queries)


def _pull_mean(session: Session, config_id: str, accessions: list[str]) -> dict[str, np.ndarray]:
    """Accession to mean-pooled vector, averaged over chunks."""
    chunks: dict[str, list[np.ndarray]] = {}
    for accession, _chunk_index, raw in session.execute(
        text(_MEAN_EMBEDDINGS), {"cfg": config_id, "accs": accessions}
    ):
        chunks.setdefault(accession, []).append(_parse_vector(raw))
    return {a: np.vstack(v).mean(0).astype(np.float32) for a, v in chunks.items()}


def _select_reference(
    session: Session, annotation_set_id: str, excluded: set[str], ref_n: int, seed: int
) -> list[str]:
    """An ordered draw, then a seeded sample, in that order.

    Sampling an unordered result is how a fixed seed selects a different pool on
    every run. The order comes from the database and the choice comes from the
    seed, so the pair is reproducible.
    """
    import random

    candidates = [
        a for a in session.execute(text(_REF_ACCESSIONS), {"ann": annotation_set_id}).scalars()
        if a not in excluded
    ]
    if len(candidates) <= ref_n:
        return candidates
    return sorted(random.Random(seed).sample(candidates, ref_n))


@dataclass(frozen=True)
class _Contents:
    """What ends up in the archive. These six are assembled together and never used apart."""

    references: list[str]
    queries: list[str]
    reference_embeddings: dict[str, np.ndarray]
    query_embeddings: dict[str, np.ndarray]
    leaves: dict[str, list[str]]
    excluded: set[str]


def _pull_and_report(
    session: Session, p: ExportGateBundlePayload, ref_accessions: list[str], emit: EmitFn
) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray], list[str], list[str]]:
    """Pull both matrices and say how many accessions actually had a vector.

    Only accessions that HAVE one survive. A reference row without a vector would
    otherwise arrive downstream as a silent hole in the matrix, and the count is
    emitted rather than inferred because a pool that quietly shrank is a pool
    nobody can size afterwards.
    """
    ref_embeddings = _pull_mean(session, p.embedding_config_id, ref_accessions)
    query_embeddings = _pull_mean(session, p.embedding_config_id, p.queries)
    kept = [a for a in ref_accessions if a in ref_embeddings]
    kept_queries = [a for a in p.queries if a in query_embeddings]
    emit("bundle.embeddings",
         f"{len(kept)} of {len(ref_accessions)} references and "
         f"{len(kept_queries)} of {len(p.queries)} queries have a vector",
         {"references": len(kept), "queries": len(kept_queries)}, "info")
    return ref_embeddings, query_embeddings, kept, kept_queries


def _pack(p: ExportGateBundlePayload, c: _Contents) -> bytes:
    """One compressed archive, with its own manifest inside it.

    The manifest travels in the file rather than beside it, because a bundle whose
    provenance lives in a separate record is a bundle whose provenance is one
    copy away from being lost.
    """
    buffer = io.BytesIO()
    np.savez_compressed(
        buffer,
        reference_accessions=np.array(c.references, dtype=object),
        reference_embeddings=np.vstack([c.reference_embeddings[a] for a in c.references]),
        query_accessions=np.array(c.queries, dtype=object),
        query_embeddings=np.vstack([c.query_embeddings[a] for a in c.queries]),
        # Leaves as one JSON blob rather than a ragged array, because the
        # consumer propagates them and needs them keyed, not padded.
        reference_leaves=json.dumps(c.leaves),
        manifest=json.dumps({
            "embedding_config_id": p.embedding_config_id,
            "annotation_set_id": p.annotation_set_id,
            "ref_n_requested": p.ref_n,
            "ref_n_written": len(c.references),
            "queries_written": len(c.queries),
            "excluded_by_sequence": len(c.excluded) - len(p.queries),
            "seed": p.seed,
            "dim": (int(next(iter(c.reference_embeddings.values())).shape[0])
                    if c.reference_embeddings else 0),
        }),
    )
    return buffer.getvalue()


def _resolve_project_root() -> Path:
    # protea/core/operations/export_gate_bundle.py -> parents[3] is the repo root
    return Path(__file__).resolve().parents[3]


def _bundle_key(payload: dict[str, Any], name: str) -> str:
    """Content address, so a rerun on the same inputs is visibly the same bundle."""
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:12]
    return f"gate-bundles/{name}-{digest}.npz"


class ExportGateBundleOperation(Operation):
    name = "export_gate_bundle"
    description = (
        "Freeze the mean-pooled embeddings, leaf GO annotations and sequence "
        "identities the encoder gates need into one compressed archive and publish "
        "it to the artifact store, so the gates can run on a machine with no "
        "database connection. Writes no rows."
    )

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ExportGateBundlePayload.model_validate(payload)
        emit("bundle.start", f"freezing {p.embedding_config_id}",
             {"queries": len(p.queries), "ref_n": p.ref_n}, "info")

        excluded = _twin_accessions(session, p.queries)
        emit("bundle.exclusion",
             f"{len(excluded) - len(p.queries)} accessions share a sequence with a query",
             {"queries": len(p.queries), "excluded": len(excluded)}, "info")

        ref_accessions = _select_reference(
            session, p.annotation_set_id, excluded, p.ref_n, p.seed
        )
        ref_embeddings, query_embeddings, kept, kept_queries = _pull_and_report(
            session, p, ref_accessions, emit
        )

        leaves: dict[str, list[str]] = {}
        for accession, go_id in session.execute(
            text(_LEAF_ANNOTATIONS), {"ann": p.annotation_set_id, "accs": kept}
        ):
            leaves.setdefault(accession, []).append(go_id)

        raw = _pack(p, _Contents(kept, kept_queries, ref_embeddings,
                                 query_embeddings, leaves, excluded))

        from protea.infrastructure.settings import load_settings

        store = get_artifact_store(load_settings(_resolve_project_root()))
        key = _bundle_key(payload, p.output_name)
        uri = store.put(key, raw)

        emit("bundle.published", f"{len(raw) / 1e6:.1f} MB at {key}",
             {"uri": uri, "bytes": len(raw)}, "info")

        return OperationResult(
            result={
                "uri": uri,
                "key": key,
                "bytes": len(raw),
                "references": len(kept),
                "queries": len(kept_queries),
                "excluded_by_sequence": len(excluded) - len(p.queries),
                # Said out loud because a consumer will otherwise assume the pool
                # is the whole annotated set.
                "caveat": (
                    "the reference pool is a seeded sample of the ordered candidate "
                    "list, not the whole annotated set; ref_n_written may be below "
                    "ref_n_requested where an accession has no stored vector"
                ),
            }
        )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return f"freeze a gate bundle for config {payload.get('embedding_config_id')}"
