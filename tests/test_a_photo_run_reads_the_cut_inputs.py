"""A run on a photo's window reads each protein's sequence and taxon at the cut, never today's.

``protein.sequence_id`` and ``protein.taxonomy_id`` are today's UniProt entry. In the
development universe they are empty for every photo member, so a reader that used
them returned nothing and its features were silently null; in production they hold
today's values, so the same reader would hand a model knowledge published after the
cut. These tests give a member a different sequence and taxon TODAY than at the cut,
so a reader that leaks reads the wrong values instead of nothing, whatever the
environment. Covered: the alignment, length and taxonomy inputs of prediction, the
queries of the transitions export, and the target FASTA with its declared entries.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.operations import export_evaluation_targets as targets
from protea.core.operations.predict_go_terms._batch_op_feature import _FeatureLoadingMixin
from protea.core.operations.predict_go_terms._photo_sequences import photo_sequences, photo_taxa
from protea.core.transitions_export import _queries as queries
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto, CutPhotoMember
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from tests.test_load_cut_photo import SEQUENCES, _load, build_photo, pg_session  # noqa: F401

#: What P1 looks like TODAY: another sequence and another organism than at the cut.
TODAY_SEQUENCE = "MTODAYSEQ"
TODAY_TAXON = 10090


@pytest.fixture()
def photo(pg_session: Session, tmp_path: Path) -> CutPhoto:  # noqa: F811
    photo_dir, sha = build_photo(tmp_path)
    _load(pg_session, photo_dir, sha)
    today = Sequence(
        sequence=TODAY_SEQUENCE,
        sequence_hash=hashlib.md5(TODAY_SEQUENCE.encode(), usedforsecurity=False).hexdigest(),
    )
    pg_session.add(today)
    pg_session.flush()
    p1 = pg_session.execute(select(Protein).where(Protein.accession == "P1")).scalar_one()
    p1.sequence_id, p1.taxonomy_id = today.id, TODAY_TAXON
    pg_session.flush()
    return pg_session.execute(select(CutPhoto)).scalar_one()


def _at_cut(session: Session, photo: CutPhoto) -> dict[str, tuple[int, str, int | None]]:
    """Each member's sequence id, residues and taxon as the photo recorded them."""
    rows = session.execute(
        select(CutPhotoMember.accession, Sequence.id, Sequence.sequence, CutPhotoMember.taxon_id)
        .join(Sequence, Sequence.sequence_hash == CutPhotoMember.sequence_md5)
        .where(CutPhotoMember.photo_id == photo.id)
    ).all()
    return {a: (sid, seq, taxon) for a, sid, seq, taxon in rows}


@pytest.mark.integration
def test_the_shared_readers_return_the_members_at_the_cut(
    pg_session: Session,  # noqa: F811
    photo: CutPhoto,
) -> None:
    at_cut = _at_cut(pg_session, photo)
    assert at_cut["P1"][1] == SEQUENCES["P1"] != TODAY_SEQUENCE
    found = photo_sequences(pg_session, photo.id, ["P1", "P2", "ABSENT"])
    assert found == {a: (at_cut[a][0], at_cut[a][1]) for a in ("P1", "P2")}
    taxa = photo_taxa(pg_session, photo.id, ["P1", "P2", "ABSENT"])
    assert taxa == {"P1": at_cut["P1"][2], "P2": at_cut["P2"][2], "ABSENT": None}
    assert taxa["P1"] != TODAY_TAXON


@pytest.mark.integration
def test_prediction_reads_sequences_and_taxa_at_the_cut_on_a_photos_run(
    pg_session: Session,  # noqa: F811
    photo: CutPhoto,
) -> None:
    op = _FeatureLoadingMixin()
    run = photo.annotation_set_id
    at_cut = _at_cut(pg_session, photo)

    donors = op._load_sequences_for_proteins(pg_session, {"P1", "P2"}, annotation_set_id=run)
    assert donors == {"P1": SEQUENCES["P1"], "P2": SEQUENCES["P2"]}
    payload = SimpleNamespace(query_set_id=None, annotation_set_id=run)
    assert op._load_sequences_for_queries(pg_session, payload, ["P1"]) == {"P1": SEQUENCES["P1"]}

    donor_taxa = op._load_taxonomy_ids_for_proteins(pg_session, {"P1"}, annotation_set_id=run)
    assert donor_taxa == {"P1": at_cut["P1"][2]}
    assert op._load_taxonomy_ids_for_queries(pg_session, payload, ["P1"]) == {"P1": at_cut["P1"][2]}

    # A run over a set no photo loaded keeps reading today's entry, as before.
    assert op._load_sequences_for_proteins(pg_session, {"P1"}) == {"P1": TODAY_SEQUENCE}
    assert op._load_taxonomy_ids_for_proteins(pg_session, {"P1"}) == {"P1": TODAY_TAXON}


@pytest.mark.integration
def test_the_transitions_queries_take_the_sequence_at_the_cut(
    pg_session: Session,  # noqa: F811
    photo: CutPhoto,
) -> None:
    at_cut = _at_cut(pg_session, photo)
    found = queries._sequences(pg_session, ["P1", "P2"], photo.id)
    assert found == {a: (at_cut[a][0], len(SEQUENCES[a])) for a in ("P1", "P2")}
    today = queries._sequences(pg_session, ["P1", "P2"])
    assert today == {"P1": (today["P1"][0], len(TODAY_SEQUENCE))}


@pytest.mark.integration
def test_the_target_fasta_and_its_entries_take_the_sequence_at_the_cut(
    pg_session: Session,  # noqa: F811
    photo: CutPhoto,
) -> None:
    at_cut = _at_cut(pg_session, photo)
    assert targets._load_sequences(pg_session, ["P1", "P2"], photo.id) == {
        "P1": SEQUENCES["P1"],
        "P2": SEQUENCES["P2"],
    }
    assert targets._sequence_ids(pg_session, ["P1", "P2"], photo.id) == {
        "P1": at_cut["P1"][0],
        "P2": at_cut["P2"][0],
    }
    assert targets._load_sequences(pg_session, ["P1", "P2"]) == {"P1": TODAY_SEQUENCE}
