"""A cut photo with chosen members, built through the ORM, and a window placed on it.

Small enough to reason about member by member: each member names the sets it entered
by, its sequence (or none, or an md5 the ``sequence`` table does not hold), and the
photo gets the rows a full photo carries (its corpus, ontology, Swiss-Prot release and
channel table), so the operations that read a photo read this one as they read a real
one.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.evaluation import (
    EvaluationData,
    groundtruth_key_for,
    serialize_evaluation_data_to_parquet,
)
from protea.core.operations._cut_photo_channel_table import (
    channel_table_digest,
    declare_channel_table,
)
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto, CutPhotoMember
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.homology_table import HomologyTable
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.window_placement import (
    WindowPlacement,
    WindowPlacementTarget,
)
from protea.infrastructure.orm.models.job import Job
from protea.infrastructure.orm.models.protein.swissprot_release import SwissProtRelease
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from protea.infrastructure.storage.local import LocalFsArtifactStore

FLAGS = ("truth", "superset", "curated", "neighbour")


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


@dataclass(frozen=True)
class Member:
    """One member: the sets it entered by, and its sequence at the cut.

    ``residues`` None is a member the release holds no sequence for; ``orphan_md5``
    gives it an md5 the ``sequence`` table does not hold instead.
    """

    accession: str
    flags: frozenset[str]
    residues: str | None = None
    orphan_md5: bool = False

    @property
    def md5(self) -> str | None:
        if self.residues is not None:
            return Sequence.compute_hash(self.residues)
        return hashlib.md5(self.accession.encode()).hexdigest() if self.orphan_md5 else None


def member(accession: str, flags: str, residues: str | None = "M", **kw: bool) -> Member:
    """A member from a compact flag string: ``"ts"`` is truth and superset (``t s c n``)."""
    names = {"t": "truth", "s": "superset", "c": "curated", "n": "neighbour"}
    return Member(accession, frozenset(names[letter] for letter in flags), residues, **kw)


def make_job(session: Session, operation: str) -> uuid.UUID:
    job = Job(operation=operation, queue_name="protea.jobs", status="RUNNING")
    session.add(job)
    session.flush()
    return job.id


def make_photo(session: Session, members: Iterable[Member], *, cut: int = 211) -> CutPhoto:
    """A full photo of ``cut`` with these members and their sequences."""
    snapshot = OntologySnapshot(obo_version=f"releases/photo-{cut}", obo_url="file:///dev/null")
    session.add(snapshot)
    session.flush()
    corpus = AnnotationSet(source="goa", source_version=str(cut), ontology_snapshot_id=snapshot.id)
    release = SwissProtRelease(
        release=f"{cut}_01",
        published_at=date(2022, 1, 1),
        source_url="file:///sprot.tar.gz",
        accession_count=1,
        content_sha256=_digest(f"release-{cut}"),
    )
    session.add_all([corpus, release])
    session.flush()
    declare_channel_table(session, None)
    photo = CutPhoto(
        kind="full",
        cut_release=cut,
        cut_date=date(2022, 1, 1),
        annotation_set_id=corpus.id,
        ontology_snapshot_id=snapshot.id,
        swissprot_release_id=release.id,
        uniref_release="2022_01",
        uniref_published_at=date(2022, 1, 1),
        uniref_clusters_sha256=_digest(f"clusters-{cut}"),
        goref_list_read_on=date(2022, 1, 1),
        channel_table_sha256=channel_table_digest(),
        manifest={},
        manifest_uri=f"file:///photo-{cut}.json",
        manifest_sha256=_digest(f"manifest-{cut}"),
        short_name=f"photo-{cut}",
        description=f"The photo of cut {cut}.",
    )
    session.add(photo)
    session.flush()
    members = list(members)
    residues = {m.md5: m.residues for m in members if m.residues is not None}
    stored = set(
        session.execute(
            select(Sequence.sequence_hash).where(Sequence.sequence_hash.in_(residues))
        ).scalars()
    )
    session.add_all(
        Sequence(sequence=seq, sequence_hash=md5)
        for md5, seq in residues.items()
        if md5 not in stored
    )
    session.add_all(
        CutPhotoMember(
            photo_id=photo.id,
            accession=m.accession,
            sequence_md5=m.md5,
            **{flag: flag in m.flags for flag in FLAGS},
        )
        for m in members
    )
    session.flush()
    return photo


def sequence_ids(session: Session, photo: CutPhoto) -> dict[str, int]:
    """Each member with a sequence row, by accession, to that row's id."""
    rows = (
        session.query(CutPhotoMember.accession, Sequence.id)
        .join(Sequence, Sequence.sequence_hash == CutPhotoMember.sequence_md5)
        .filter(CutPhotoMember.photo_id == photo.id)
        .all()
    )
    return {accession: sequence_id for accession, sequence_id in rows}


def make_homology_table(session: Session, photo: CutPhoto) -> HomologyTable:
    table = HomologyTable(
        cut_photo_id=photo.id,
        cut_release=photo.cut_release,
        mmseqs_version="18-8cc5c",
        mmseqs_binary_sha256=_digest("mmseqs"),
        command=["mmseqs", "easy-search"],
        queries=1,
        bank=1,
        hits=0,
        self_hits_dropped=0,
        queries_with_hits=0,
        members_without_sequence=0,
        hits_uri="file:///hits.parquet",
        hits_sha256=_digest("hits"),
        manifest={},
        manifest_uri="file:///homology.json",
        manifest_sha256=_digest(f"homology-{photo.cut_release}"),
        short_name=f"homology-{photo.cut_release}",
        description="The homology table.",
    )
    session.add(table)
    session.flush()
    return table


def make_placed_window(
    session: Session,
    photo: CutPhoto,
    truth: EvaluationData,
    aspects: Mapping[str, str],
    placed: Mapping[str, tuple[float | None, int]],
    store_root: Path,
) -> EvaluationSet:
    """A window opening on ``photo``'s corpus, its truth in a local store, and its placement.

    ``aspects`` gives each GO term of the truth its aspect in the pivot; ``placed`` gives
    each target its closest identity (None for no homolog) and its length.
    """
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
    session.add_all(
        GOTerm(go_id=go_id, aspect=aspect, ontology_snapshot_id=photo.ontology_snapshot_id)
        for go_id, aspect in aspects.items()
    )
    store = LocalFsArtifactStore(store_root)
    local = serialize_evaluation_data_to_parquet(truth, store_root.parent / "truth.parquet")
    window.groundtruth_uri = store.put(groundtruth_key_for(window.id), local)
    window.groundtruth_sha256 = hashlib.sha256(local.read_bytes()).hexdigest()
    table = make_homology_table(session, photo)
    placement = WindowPlacement(
        evaluation_set_id=window.id,
        cut_photo_id=photo.id,
        homology_table_id=table.id,
        rule="closest_truth_member",
        targets=len(placed),
        targets_with_twin=sum(1 for identity, _ in placed.values() if identity == 100.0),
        targets_with_hit=sum(1 for identity, _ in placed.values() if identity not in (None, 100.0)),
        targets_without_homolog=sum(1 for identity, _ in placed.values() if identity is None),
        short_name=f"placement-{photo.cut_release}-227-{str(window.id)[:8]}",
        description="The placement.",
    )
    session.add(placement)
    session.flush()
    session.add_all(
        WindowPlacementTarget(
            placement_id=placement.id,
            accession=accession,
            sequence_md5=hashlib.md5(accession.encode()).hexdigest(),
            residues=residues,
            closest_by=None if identity is None else ("twin" if identity == 100.0 else "hit"),
            closest_identity=identity,
            closest_md5=(
                None
                if identity is None
                else hashlib.md5(accession.encode()).hexdigest()
                if identity == 100.0
                else "f" * 32
            ),
            bits=None if identity in (None, 100.0) else 50.0,
        )
        for accession, (identity, residues) in placed.items()
    )
    session.flush()
    return window


__all__ = [
    "FLAGS",
    "Member",
    "make_homology_table",
    "make_job",
    "make_photo",
    "make_placed_window",
    "member",
    "sequence_ids",
]
