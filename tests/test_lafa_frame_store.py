"""LAFA's frame against Postgres: the two reads and LAFA's targets on a store small enough to read.

``tests/test_lafa_frame.py`` pins every step on ontologies built by hand. This
pins the SQL that builds them from the store, end to end through
``compute_evaluation_data_lafa``, and the join that finds LAFA's targets:

* P1 holds an obsolete term at t0, which its successor names, and gains a new
  aspect at t1: LK.
* P2 gains, at t1, a term only t1's ontology has, under "protein binding"; the
  trim leaves it binding only, and the binding rule drops its F side.
* P3 is denied at t1, by an experimental ``NOT``, what it held at both ends.
* P4 has nothing at t0 and gains at t1, NK; its sequence changed between the two
  Swiss-Prot releases, so LAFA's targets leave it out.

A ``regulates`` edge is in the store and must not propagate.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import date

import pytest
from sqlalchemy.orm import Session

from protea.core._lafa_frame_data import load_end_ontology
from protea.core._window_population import SameSequence, same_sequence_at_both_ends
from protea.core.lafa_frame import LafaWindow, compute_evaluation_data_lafa
from protea.core.operations._cut_photo_channel_table import (
    channel_table_digest,
    declare_channel_table,
)
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.go_term_alias import GOTermAlias
from protea.infrastructure.orm.models.annotation.go_term_relationship import GOTermRelationship
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.protein.swissprot_release import (
    SwissProtMembership,
    SwissProtRelease,
)
from tests.test_run_count_map import pg  # noqa: F401

F_ROOT, BINDING, PROTEIN_BINDING = "GO:0003674", "GO:0005488", "GO:0005515"
CATALYTIC, KINASE, OLD = "GO:0003824", "GO:0016301", "GO:0000005"
P_ROOT, PHOSPHORYLATION = "GO:0008150", "GO:0016310"
NEW_BINDING = "GO:0140999"

LIVE = {
    F_ROOT: "F",
    BINDING: "F",
    PROTEIN_BINDING: "F",
    CATALYTIC: "F",
    KINASE: "F",
    P_ROOT: "P",
    PHOSPHORYLATION: "P",
}
IS_A = [
    (BINDING, F_ROOT),
    (PROTEIN_BINDING, BINDING),
    (CATALYTIC, F_ROOT),
    (KINASE, CATALYTIC),
    (PHOSPHORYLATION, P_ROOT),
]


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _ontology(pg: Session, version: str, extra: dict[str, str]) -> OntologySnapshot:  # noqa: F811
    snapshot = OntologySnapshot(obo_version=f"releases/{version}", obo_url="file:///dev/null")
    pg.add(snapshot)
    pg.flush()
    terms = {
        go_id: GOTerm(go_id=go_id, aspect=aspect, ontology_snapshot_id=snapshot.id)
        for go_id, aspect in {**LIVE, **extra}.items()
    }
    terms[OLD] = GOTerm(go_id=OLD, aspect="F", is_obsolete=True, ontology_snapshot_id=snapshot.id)
    pg.add_all(terms.values())
    pg.flush()
    edges = [*IS_A, *((child, PROTEIN_BINDING) for child in extra), (KINASE, PHOSPHORYLATION)]
    for number, (child, parent) in enumerate(edges):
        pg.add(
            GOTermRelationship(
                child_go_term_id=terms[child].id,
                parent_go_term_id=terms[parent].id,
                # The last edge is a ``regulates`` across aspects: never propagated.
                relation_type="regulates" if number == len(edges) - 1 else "is_a",
                ontology_snapshot_id=snapshot.id,
            )
        )
    pg.add(
        GOTermAlias(
            ontology_snapshot_id=snapshot.id,
            alias_go_id=OLD,
            primary_go_id=KINASE,
            kind="replaced_by",
        )
    )
    return snapshot


def _corpus(pg: Session, snapshot: OntologySnapshot, rows: list[tuple]) -> AnnotationSet:  # noqa: F811
    corpus = AnnotationSet(source="goa", ontology_snapshot_id=snapshot.id)
    pg.add(corpus)
    pg.flush()
    ids = {
        term.go_id: term.id for term in pg.query(GOTerm).filter_by(ontology_snapshot_id=snapshot.id)
    }
    for accession, go_id, code, qualifier in rows:
        pg.add(
            ProteinGOAnnotation(
                annotation_set_id=corpus.id,
                protein_accession=accession,
                go_term_id=ids[go_id],
                evidence_code=code,
                qualifier=qualifier,
            )
        )
    return corpus


def _photo(pg: Session, cut: int, corpus: AnnotationSet, md5: dict[str, str], name: str) -> None:  # noqa: F811
    release = SwissProtRelease(
        release=name,
        published_at=date(2000 + cut // 10, 1, 1),
        source_url="file:///sprot.tar.gz",
        accession_count=len(md5),
        content_sha256=_digest(name),
    )
    pg.add(release)
    pg.flush()
    pg.add_all(
        SwissProtMembership(release_id=release.id, accession=accession, sequence_md5=digest)
        for accession, digest in md5.items()
    )
    pg.add(
        CutPhoto(
            kind="truth_only",
            cut_release=cut,
            cut_date=date(2000 + cut // 10, 1, 1),
            annotation_set_id=corpus.id,
            ontology_snapshot_id=corpus.ontology_snapshot_id,
            swissprot_release_id=release.id,
            goref_list_read_on=date(2000 + cut // 10, 1, 1),
            channel_table_sha256=channel_table_digest(),
            manifest={},
            manifest_uri=f"file:///photo-{cut}.json",
            manifest_sha256=_digest(f"manifest-{cut}"),
            short_name=f"photo-{cut}",
            description=f"The photo of cut {cut}.",
        )
    )


def _window(pg: Session) -> LafaWindow:  # noqa: F811
    pg.add_all(
        Protein(accession=accession, canonical_accession=accession)
        for accession in ("P1", "P2", "P3", "P4")
    )
    pg.flush()
    t0 = _ontology(pg, "t0", {})
    t1 = _ontology(pg, "t1", {NEW_BINDING: "F"})
    old = _corpus(
        pg,
        t0,
        [
            ("P1", OLD, "IDA", None),
            ("P2", PROTEIN_BINDING, "IPI", None),
            ("P3", CATALYTIC, "IDA", None),
        ],
    )
    new = _corpus(
        pg,
        t1,
        [
            ("P1", KINASE, "IDA", None),
            ("P1", PHOSPHORYLATION, "IMP", None),
            ("P2", PROTEIN_BINDING, "IPI", None),
            ("P2", NEW_BINDING, "IPI", None),
            ("P3", KINASE, "IDA", None),
            ("P3", CATALYTIC, "IDA", "NOT|enables"),
            ("P4", KINASE, "EXP", None),
            # Not experimental: no part of the truth.
            ("P4", PHOSPHORYLATION, "IEA", None),
        ],
    )
    declare_channel_table(pg, None)
    same = {"P1": "a" * 32, "P2": "b" * 32, "P3": "c" * 32}
    _photo(pg, 211, old, {**same, "P4": "d" * 32}, "2022_03")
    _photo(pg, 227, new, {**same, "P4": "e" * 32}, "2025_03")
    pg.commit()
    return LafaWindow(
        old.id, new.id, t0.id, t1.id, "own_graph", "both_ends_trimmed", "experimental", "cafa"
    )


@pytest.mark.integration
def test_lafas_truth_from_the_store(pg: Session) -> None:  # noqa: F811
    data, stats = compute_evaluation_data_lafa(pg, _window(pg))

    assert data.nk == {"P4": {KINASE, CATALYTIC, F_ROOT}}
    assert data.lk == {"P1": {PHOSPHORYLATION, P_ROOT}}
    assert data.pk == {}
    assert data.binding_dropped == {"old_exp": 0, "new_exp": 1}
    assert stats["lafa_t0_positive_renamed_replaced_by"] == 1
    assert stats["lafa_t0_negated_proteins"] == stats["lafa_t1_negated_proteins"] == 1
    assert stats["terms_of_interest"] == len(LIVE)


@pytest.mark.integration
def test_lafas_targets_are_the_same_sequence_at_both_ends(pg: Session) -> None:  # noqa: F811
    window = _window(pg)
    found = same_sequence_at_both_ends(pg, window.old_set_id, window.new_set_id)
    assert found == SameSequence("2022_03", "2025_03", 4, frozenset({"P1", "P2", "P3"}))
    assert found.described()["population_sequence_changed"] == 1


@pytest.mark.integration
def test_an_end_without_a_photo_is_refused(pg: Session) -> None:  # noqa: F811
    window = _window(pg)
    with pytest.raises(ValueError, match="0 cut photos"):
        same_sequence_at_both_ends(pg, window.old_set_id, uuid.uuid4())


@pytest.mark.integration
def test_an_ontology_without_its_aliases_is_refused(pg: Session) -> None:  # noqa: F811
    window = _window(pg)
    pg.query(GOTermAlias).filter_by(ontology_snapshot_id=window.old_native).delete()
    pg.commit()
    with pytest.raises(ValueError, match="load_ontology_aliases"):
        load_end_ontology(pg, window.old_native)
