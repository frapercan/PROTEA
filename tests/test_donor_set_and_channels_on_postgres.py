"""On Postgres: the donor set and the evidence channels restrict the pool as they say.

The unit tests read the predicates off the compiled query. Here they run on the
schema the store has, over a photo built through the ORM whose members each
carry rows of one channel: a NOT-only donor, a donor whose only row is ``ND``,
and the two field translations a code and a reference cannot tell apart, the
keyword mapping on a reviewed entry (``reviewed_field_electronic``) and on an
unreviewed one (``signature_electronic``). The channel of a row is computed by
``evidence_channel_of`` with the photo's own table and the member's review
status at the cut.

The parity the mirror arm relies on is pinned here: a pool admitted through
every channel but ``no_data`` is the pool admitted through every evidence code
but ``ND``, and so are the rows the aspect build and the donation loader read.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from protea_contracts.payloads import DonorPolicy
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from protea.core.cut_photo.channels import CHANNELS, CURATED_CODES, PHYLOGENETIC_CODES
from protea.core.cut_photo.formats import TRUTH_CODES
from protea.core.operations import declare_photo_member_set as member_set_operation
from protea.core.operations.declare_photo_member_set import DeclarePhotoMemberSetOperation
from protea.core.operations.predict_go_terms._batch_op import PredictGOTermsBatchOperation
from protea.core.operations.predict_go_terms._batch_op_reference import (
    _PoolRequest,
    _ReferenceMixin,
)
from protea.core.operations.predict_go_terms._common import PredictGOTermsPayload
from protea.core.operations.predict_go_terms._coordinator import (
    _refuse_donors_the_cut_does_not_declare,
)
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.protein_go_annotation import ProteinGOAnnotation
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.session import build_session_factory
from tests.helpers.cut_photos import make_job, make_photo, member, sequence_ids

#: Member, its admission flags (``t`` truth, ``s`` superset, ``c`` curated, ``n``
#: neighbour) and its rows: (evidence code, reference, qualifier, aspect).
MEMBERS: dict[str, tuple[str, list[tuple[str, str | None, str | None, str]]]] = {
    "T1": ("ts", [("EXP", "PMID:1", None, "P")]),
    "T2": ("t", [("IDA", "PMID:2", "enables", "F"), ("IMP", "PMID:3", "NOT|enables", "P")]),
    "N1": ("t", [("EXP", "PMID:4", "NOT|enables", "F")]),
    "C1": ("c", [("ISS", "GO_REF:0000024", None, "F")]),
    "B1": ("n", [("IBA", "GO_REF:0000033", None, "C")]),
    "O1": ("n", [("IEA", "GO_REF:0000107", None, "P")]),
    "K1": ("s", [("IEA", "GO_REF:0000043", None, "C")]),
    "K2": ("n", [("IEA", "GO_REF:0000043", None, "C")]),
    "S1": ("n", [("IEA", "GO_REF:0000002", None, "F")]),
    "D1": ("n", [("ND", "GO_REF:0000015", None, "P")]),
}
TERMS = {"P": "GO:0008150", "F": "GO:0003674", "C": "GO:0005575"}
RECIPE = ["truth", "curated", "phylogenetic", "orthology_electronic", "reviewed_field_electronic"]
#: Every code the channel table lists but ND: what the mirror arm's code list says.
EVERY_CODE_BUT_ND = sorted(TRUTH_CODES | CURATED_CODES | PHYLOGENETIC_CODES | {"IEA"})


@pytest.fixture()
def pg(postgres_url: str) -> Iterator[Session]:
    """A session built as the fleet builds one, on a schema made for this test."""
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    engine.dispose()
    factory = build_session_factory(postgres_url)
    with factory() as session:
        yield session
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    engine.dispose()


def _photo(session: Session, cut: int = 211) -> tuple[CutPhoto, uuid.UUID]:
    """The photo, every member with a vector of one configuration and its rows."""
    members = [
        member(accession, flags, residues=f"MK{index:03d}")
        for index, (accession, (flags, _rows)) in enumerate(MEMBERS.items())
    ]
    photo = make_photo(session, members, cut=cut)
    config = EmbeddingConfig(model_name=f"test-{cut}", layer_indices=[])
    session.add(config)
    terms = {
        aspect: GOTerm(go_id=go_id, aspect=aspect, ontology_snapshot_id=photo.ontology_snapshot_id)
        for aspect, go_id in TERMS.items()
    }
    session.add_all(terms.values())
    session.flush()
    sequences = sequence_ids(session, photo)
    for index, (accession, (_flags, rows)) in enumerate(MEMBERS.items()):
        if session.get(Protein, accession) is None:
            session.add(Protein(accession=accession, canonical_accession=accession))
        session.add(
            SequenceEmbedding(
                sequence_id=sequences[accession],
                embedding_config_id=config.id,
                embedding=[1.0, float(index), 0.5],
                embedding_dim=3,
            )
        )
        session.add_all(
            ProteinGOAnnotation(
                annotation_set_id=photo.annotation_set_id,
                protein_accession=accession,
                go_term_id=terms[aspect].id,
                evidence_code=code,
                db_reference=reference,
                qualifier=qualifier,
            )
            for code, reference, qualifier, aspect in rows
        )
    session.flush()
    return photo, config.id


def _pool(session: Session, photo: CutPhoto, config: uuid.UUID, **policy: Any) -> set[str]:
    request = _PoolRequest(config, photo.annotation_set_id, DonorPolicy(**policy))
    return {row[0] for row in _ReferenceMixin()._reference_pool_query(session, request).all()}


def _donations(session: Session, photo: CutPhoto, **policy: Any) -> set[tuple[str, str]]:
    rows = PredictGOTermsBatchOperation()._fetch_annotation_chunk(
        session, photo.annotation_set_id, sorted(MEMBERS), None, DonorPolicy(**policy)
    )
    return {(accession, code) for accession, _term, _qualifier, code in rows}


def _aspect_rows(session: Session, photo: CutPhoto, **policy: Any) -> dict[str, set[str]]:
    accessions, _go_maps = PredictGOTermsBatchOperation._collect_aspect_annotations(
        session, ["P", "F", "C"], photo.annotation_set_id, DonorPolicy(**policy)
    )
    return accessions


def test_a_donor_is_admitted_only_through_a_positive_row(pg: Session) -> None:
    photo, config = _photo(pg)

    pool = _pool(pg, photo, config)

    assert "N1" not in pool, "its only row is a NOT row: it donates nothing"
    assert pool == set(MEMBERS) - {"N1"}
    assert "N1" not in {acc for acc, _code in _donations(pg, photo)}
    assert all("N1" not in members for members in _aspect_rows(pg, photo).values())


def test_every_channel_but_no_data_is_every_code_but_nd(pg: Session) -> None:
    photo, config = _photo(pg)
    six = [channel for channel in CHANNELS if channel != "no_data"]

    by_channels = _pool(pg, photo, config, evidence_channels=six)
    by_codes = _pool(pg, photo, config, evidence_codes=EVERY_CODE_BUT_ND)

    assert by_channels == by_codes == set(MEMBERS) - {"N1", "D1"}
    assert _donations(pg, photo, evidence_channels=six) == _donations(
        pg, photo, evidence_codes=EVERY_CODE_BUT_ND
    )
    assert _aspect_rows(pg, photo, evidence_channels=six) == _aspect_rows(
        pg, photo, evidence_codes=EVERY_CODE_BUT_ND
    )


def test_a_field_translation_is_the_curators_only_on_a_reviewed_entry(pg: Session) -> None:
    """One code and one reference, two channels: the member's review status at the cut decides."""
    photo, config = _photo(pg)

    recipe = _pool(pg, photo, config, evidence_channels=RECIPE)

    assert "K1" in recipe, "a keyword mapping on a reviewed entry is the curator's"
    assert "K2" not in recipe, "the same mapping on an unreviewed entry is a rule's"
    assert "S1" not in recipe, "signatures are the mirror arm's, never the recipe's"
    assert recipe == {"T1", "T2", "C1", "B1", "O1", "K1"}
    mirror = _pool(pg, photo, config, evidence_channels=[*RECIPE, "signature_electronic"])
    assert mirror - recipe == {"K2", "S1"}


def test_a_channel_admits_its_own_rows_and_donates_them_only(pg: Session) -> None:
    photo, config = _photo(pg)

    assert _pool(pg, photo, config, evidence_channels=["truth"]) == {"T1", "T2"}
    assert _donations(pg, photo, evidence_channels=["truth"]) == {("T1", "EXP"), ("T2", "IDA")}
    assert _aspect_rows(pg, photo, evidence_channels=["truth"]) == {
        "P": {"T1"},
        "F": {"T2"},
        "C": set(),
    }
    assert _donations(pg, photo, evidence_channels=["orthology_electronic"]) == {("O1", "IEA")}


def _declare(session: Session, photo: CutPhoto, monkeypatch: pytest.MonkeyPatch) -> str:
    """A set of the photo's truth members, declared by its own operation."""
    monkeypatch.setattr(member_set_operation, "publish", lambda key, _blob: f"memory://{key}")
    payload = {
        "cut_photo_id": str(photo.id),
        "any_of": ["truth"],
        "output_name": f"donors-level-1-experimental-{photo.cut_release}",
        "_job_id": str(make_job(session, "declare_photo_member_set")),
    }
    result = DeclarePhotoMemberSetOperation().execute(session, payload, emit=lambda *a, **k: None)
    session.flush()
    return str(result.result["query_set_id"])


def test_a_donor_set_keeps_the_members_it_lists(
    pg: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    photo, config = _photo(pg)
    level_one = _declare(pg, photo, monkeypatch)

    pool = _pool(pg, photo, config, donor_set_id=level_one)

    assert pool == {"T1", "T2"}, "the truth members, less the one with NOT rows alone"
    both = _pool(pg, photo, config, donor_set_id=level_one, evidence_channels=["truth"])
    assert both == {"T1", "T2"}
    assert _pool(pg, photo, config, donor_set_id=level_one, evidence_channels=["curated"]) == set()


def test_the_coordinator_takes_the_runs_own_photo_members_only(
    pg: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    photo, config = _photo(pg)
    other_photo, _other_config = _photo(pg, cut=194)
    own = _declare(pg, photo, monkeypatch)
    foreign = _declare(pg, other_photo, monkeypatch)

    def payload(donor_set_id: str) -> PredictGOTermsPayload:
        return PredictGOTermsPayload.model_validate(
            {
                "embedding_config_id": str(config),
                "annotation_set_id": str(photo.annotation_set_id),
                "ontology_snapshot_id": str(photo.ontology_snapshot_id),
                "query_accessions": ["T1"],
                "donor_policy": {"donor_set_id": donor_set_id},
            }
        )

    _refuse_donors_the_cut_does_not_declare(pg, payload(own))
    with pytest.raises(ValueError, match="only a photo_members set of that photo"):
        _refuse_donors_the_cut_does_not_declare(pg, payload(foreign))
    with pytest.raises(ValueError, match="is not a query set"):
        _refuse_donors_the_cut_does_not_declare(pg, payload(str(uuid.uuid4())))
