"""A small cut for the literature ladder's native writers, built through the ORM.

One full photo of LAFA's frame with its members and sequences, an ontology with its
edges and an alternative id, every annotation row the three writers read (the truth
of the donors and the targets, their NOT rows, and the targets' own electronic and
curated rows in four channels), a window over it with its truth in a local store,
the cut's homology table with its hits in that store, and the table's self scores.
Small enough to compute every expected score by hand, which the tests do.
"""

from __future__ import annotations

import hashlib
import io
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy.orm import Session

from protea.core.evaluation import (
    EvaluationData,
    groundtruth_key_for,
    serialize_evaluation_data_to_parquet,
)
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.go_term_alias import GOTermAlias
from protea.infrastructure.orm.models.annotation.go_term_relationship import GOTermRelationship
from protea.infrastructure.orm.models.annotation.homology_self_scores import HomologySelfScores
from protea.infrastructure.orm.models.annotation.homology_table import HomologyTable
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.storage.local import LocalFsArtifactStore
from tests.helpers.cut_photos import Member, make_photo, member

#: Each term's aspect and parents (``is_a``) in the photo's ontology.
TERMS: dict[str, tuple[str, tuple[str, ...]]] = {
    "GO:0008150": ("P", ()),
    "GO:0003674": ("F", ()),
    "GO:0005575": ("C", ()),
    "GO:0000001": ("P", ("GO:0008150",)),
    "GO:0000002": ("P", ("GO:0000001",)),
    "GO:0000005": ("P", ("GO:0008150",)),
    "GO:0000003": ("F", ("GO:0003674",)),
    "GO:0000006": ("F", ("GO:0000003",)),
    "GO:0000004": ("C", ("GO:0005575",)),
}
#: GO:0000011 is an alternative id of GO:0000001.
ALIAS = ("GO:0000011", "GO:0000001")

#: The members: the targets T1, T2 and T3 (Swiss-Prot); TW, T1's twin; the donors D1
#: (reviewed, experimental), D2 (TrEMBL, experimental), D3 (reviewed, no rows) and N1
#: (a UniRef neighbour with an electronic orthology row). Lengths set the identities.
SEQUENCES = {
    "T1": "M" + "A" * 99,
    "TW": "M" + "A" * 99,
    "T2": "M" + "C" * 199,
    "T3": "M" + "D" * 79,
    "D1": "M" + "E" * 119,
    "D2": "M" + "F" * 99,
    "D3": "M" + "G" * 99,
    "N1": "M" + "H" * 59,
}
FLAGS = {"T1": "ts", "TW": "ts", "T2": "s", "T3": "s", "D1": "ts", "D2": "t", "D3": "s", "N1": "n"}

#: Every annotation row of the photo's corpus: (protein, term, code, qualifier, reference).
ROWS: list[tuple[str, str, str, str | None, str | None]] = [
    ("T1", "GO:0000001", "EXP", None, "PMID:1"),
    ("T1", "GO:0000004", "IDA", None, "PMID:1"),
    ("TW", "GO:0000002", "IDA", None, "PMID:2"),
    ("TW", "GO:0000003", "IDA", "NOT|enables", "PMID:2"),
    ("D1", "GO:0000002", "IMP", None, "PMID:3"),
    ("D1", "GO:0000006", "IPI", None, "PMID:3"),
    ("D1", "GO:0000011", "TAS", None, "PMID:3"),
    ("D2", "GO:0000005", "IDA", None, "PMID:4"),
    ("N1", "GO:0000005", "IEA", None, "GO_REF:0000107"),
    # The targets' own non-experimental rows, one channel each.
    ("T1", "GO:0000005", "IEA", None, "GO_REF:0000107"),  # orthology_electronic
    ("T1", "GO:0000003", "IEA", None, "GO_REF:0000002"),  # signature_electronic
    ("T1", "GO:0000004", "ISS", None, "PMID:5"),  # curated
    ("T2", "GO:0000002", "IBA", None, "GO_REF:0000033"),  # phylogenetic
    ("T2", "GO:0000006", "IEA", None, "GO_REF:0000043"),  # reviewed_field_electronic
    ("T3", "GO:0000006", "IEA", None, "GO_REF:0000002"),  # signature_electronic
]

#: The hits of the homology table: (query, target, fident, alnlen, evalue, bits).
HITS = [
    ("T1", "D1", 0.8, 100, 1e-40, 200.0),
    ("T1", "D2", 0.5, 100, 1e-20, 100.0),
    ("T1", "N1", 0.6, 50, 1e-10, 80.0),
    ("T2", "D1", 0.3, 200, 1e-5, 50.0),
]
#: The self scores of the table's queries: T3's sequence has none.
SELF_BITS = {"T1": 250.0, "T2": 400.0}


def md5(accession: str) -> str:
    return hashlib.md5(SEQUENCES[accession].encode()).hexdigest()


def _sha256(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()


@dataclass
class World:
    """What the tests read back: the photo, the window, the table and the store."""

    photo: CutPhoto
    window: EvaluationSet
    table: HomologyTable
    store: LocalFsArtifactStore
    term_ids: dict[str, int]


def _ontology(session: Session, photo: CutPhoto) -> dict[str, int]:
    snapshot = photo.ontology_snapshot_id
    terms = {
        go_id: GOTerm(go_id=go_id, aspect=a, ontology_snapshot_id=snapshot)
        for go_id, (a, _) in TERMS.items()
    }
    session.add_all(terms.values())
    session.flush()
    session.add_all(
        GOTermRelationship(
            child_go_term_id=terms[child].id,
            parent_go_term_id=terms[parent].id,
            relation_type="is_a",
            ontology_snapshot_id=snapshot,
        )
        for child, (_, parents) in TERMS.items()
        for parent in parents
    )
    alias = GOTerm(go_id=ALIAS[0], aspect="P", ontology_snapshot_id=snapshot, is_obsolete=True)
    session.add(alias)
    session.add(
        GOTermAlias(
            ontology_snapshot_id=snapshot,
            alias_go_id=ALIAS[0],
            primary_go_id=ALIAS[1],
            kind="alt_id",
        )
    )
    session.flush()
    return {**{go_id: t.id for go_id, t in terms.items()}, ALIAS[0]: alias.id}


def _rows(session: Session, photo: CutPhoto, ids: dict[str, int]) -> None:
    session.add_all(Protein(accession=a, canonical_accession=a) for a in SEQUENCES)
    session.flush()
    session.add_all(
        ProteinGOAnnotation(
            annotation_set_id=photo.annotation_set_id,
            protein_accession=protein,
            go_term_id=ids[go_id],
            evidence_code=code,
            qualifier=qualifier,
            db_reference=reference,
        )
        for protein, go_id, code, qualifier, reference in ROWS
    )


def _window(
    session: Session, photo: CutPhoto, store: LocalFsArtifactStore, tmp: Path
) -> EvaluationSet:
    closing = AnnotationSet(
        source="goa", source_version="227", ontology_snapshot_id=photo.ontology_snapshot_id
    )
    session.add(closing)
    session.flush()
    window = EvaluationSet(
        old_annotation_set_id=photo.annotation_set_id,
        new_annotation_set_id=closing.id,
        pivot_snapshot_id=photo.ontology_snapshot_id,
        old_native_snapshot_id=photo.ontology_snapshot_id,
        new_native_snapshot_id=photo.ontology_snapshot_id,
        truth_profile="own_graph",
        toi_rule="both_ends_trimmed",
        not_codes="experimental",
        target_population="swissprot_same_sequence",
        binding_rule="cafa",
        window_role="training",
        stats={"pivot_ontology_snapshot_id": str(photo.ontology_snapshot_id), "mode": "lafa"},
    )
    session.add(window)
    session.flush()
    truth = EvaluationData(
        nk={"T3": {"GO:0000005"}},
        lk={"T2": {"GO:0000004"}},
        pk={"T1": {"GO:0000006"}},
        known={"T1": {"GO:0000001", "GO:0000004"}},
        pk_known={"T1": {"GO:0000003"}},
    )
    local = serialize_evaluation_data_to_parquet(truth, tmp / f"truth-{window.id}.parquet")
    window.groundtruth_uri = store.put(groundtruth_key_for(window.id), local)
    window.groundtruth_sha256 = hashlib.sha256(local.read_bytes()).hexdigest()
    return window


def _hits_blob() -> bytes:
    names = ("query_md5", "target_md5", "fident", "alnlen", "evalue", "bits")
    columns = list(zip(*HITS, strict=True))
    table = pa.table(
        {
            "query_md5": pa.array([md5(a) for a in columns[0]], pa.large_string()),
            "target_md5": pa.array([md5(a) for a in columns[1]], pa.large_string()),
            "fident": pa.array(columns[2], pa.float32()),
            "alnlen": pa.array(columns[3], pa.int32()),
            "evalue": pa.array(columns[4], pa.float64()),
            "bits": pa.array(columns[5], pa.float32()),
        }
    ).select(list(names))
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


def queries_fasta_sha256() -> str:
    """The sha256 of the table's queries as its prepare pass writes them: the superset's sequences."""
    superset = {md5(a): SEQUENCES[a] for a, flags in FLAGS.items() if "s" in flags}
    return hashlib.sha256(
        "".join(f">{m}\n{superset[m]}\n" for m in sorted(superset)).encode()
    ).hexdigest()


def table_queries() -> int:
    return len({md5(a) for a, flags in FLAGS.items() if "s" in flags})


def _table(session: Session, photo: CutPhoto, store: LocalFsArtifactStore) -> HomologyTable:
    blob = _hits_blob()
    uri = store.put(f"homology/{photo.cut_release}/hits.parquet", blob)
    table = HomologyTable(
        cut_photo_id=photo.id,
        cut_release=photo.cut_release,
        mmseqs_version="8cc5ce367b5638c4306c2d7cfc652dd099a4643f",
        mmseqs_binary_sha256=_sha256(b"mmseqs"),
        command=["mmseqs", "easy-search", "queries.fasta", "bank.fasta", "hits.m8"],
        queries=table_queries(),
        bank=len(set(SEQUENCES.values())),
        hits=len(HITS),
        self_hits_dropped=0,
        queries_with_hits=2,
        members_without_sequence=0,
        hits_uri=uri,
        hits_sha256=_sha256(blob),
        manifest={"counts": {"prepare": {"queries_fasta_sha256": queries_fasta_sha256()}}},
        manifest_uri="file:///homology.json",
        manifest_sha256=_sha256(f"homology-{photo.cut_release}".encode()),
        short_name=f"homology-{photo.cut_release}",
        description="The homology table.",
    )
    session.add(table)
    session.flush()
    return table


def self_scores_blob() -> bytes:
    scored = sorted((md5(a), bits) for a, bits in SELF_BITS.items())
    table = pa.table(
        {
            "query_md5": pa.array([m for m, _ in scored], pa.string()),
            "fident": pa.array([1.0] * len(scored), pa.float32()),
            "alnlen": pa.array([len(SEQUENCES[a]) for a in sorted(SELF_BITS, key=md5)], pa.int32()),
            "evalue": pa.array([1e-50] * len(scored), pa.float64()),
            "bits": pa.array([b for _, b in scored], pa.float64()),
        }
    )
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


def add_self_scores(session: Session, world: World) -> HomologySelfScores:
    blob = self_scores_blob()
    row = HomologySelfScores(
        homology_table_id=world.table.id,
        mmseqs_version=world.table.mmseqs_version,
        mmseqs_binary_sha256=world.table.mmseqs_binary_sha256,
        command=[["mmseqs", "align"]],
        queries=table_queries(),
        queries_without_self_score=table_queries() - len(SELF_BITS),
        scores_uri=world.store.put("homology-self-scores/211/self-scores.parquet", blob),
        scores_sha256=_sha256(blob),
        manifest={},
        manifest_uri="file:///self-scores.json",
        manifest_sha256=_sha256(b"self-scores-211"),
        short_name="homology-self-scores-211",
        description="The self scores.",
    )
    session.add(row)
    session.flush()
    return row


def make_world(session: Session, root: Path) -> World:
    """The whole cut, committed."""
    members: list[Member] = [member(a, FLAGS[a], residues=s) for a, s in SEQUENCES.items()]
    photo = make_photo(session, members)
    ids = _ontology(session, photo)
    _rows(session, photo, ids)
    store = LocalFsArtifactStore(root / "store")
    window = _window(session, photo, store, root)
    table = _table(session, photo, store)
    session.commit()
    return World(photo, window, table, store, ids)


def whole_row(session: Session, set_id: uuid.UUID) -> dict[str, Any]:
    """A prediction set as ``SELECT *`` returns it, which is what the method seal reads."""
    from sqlalchemy import text

    return dict(
        session.execute(text("SELECT * FROM prediction_set WHERE id = :p"), {"p": set_id})
        .mappings()
        .one()
    )


__all__ = [
    "ALIAS",
    "FLAGS",
    "HITS",
    "ROWS",
    "SELF_BITS",
    "SEQUENCES",
    "TERMS",
    "World",
    "add_self_scores",
    "make_world",
    "md5",
    "queries_fasta_sha256",
    "self_scores_blob",
    "table_queries",
    "whole_row",
]
