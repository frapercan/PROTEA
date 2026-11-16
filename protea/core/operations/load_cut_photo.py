"""Load the photo of a cut into the database (ADR-D59, decision 4, server half).

The builder (``protea.core.cut_photo.build``) writes a photo's files where its
sources are, with no database. This operation runs on the server, on those
files copied to a directory it can read, and refuses before writing anything
unless all of the following hold:

- ``photo.json`` hashes to the sha256 the payload names, its status is
  ``complete``, and every output it lists still hashes to what it recorded;
- the cut is not after the mark, the GOA release it names was published on the
  cut's date, and every other source (the paired Swiss-Prot release, the UniRef
  release of the clusters and the ontology snapshot the payload names) was
  published on or before the cut, with no UniProt or UniRef release at or after
  the ceiling (``protea.core.source_guards``, the rules the builder asked);
- the GO graph is the one the GAF's header leads to: not newer than the GO
  release the GAF declares (``!go-version``), and no loaded graph sits between
  the two, since a cut takes the newest graph not after that release;
- the photo was classified with the channel table this platform declares, since
  the rows are stored without their channel and every reader computes it again;
- the Swiss-Prot release the manifest names is the one already loaded: same
  publication date, same entry count, same canonical table digest;
- ``sequences.parquet`` IS that release: every md5 is the md5 of its sequence,
  and its accessions and md5s hash to the release's canonical table.

Then, in the job's transaction: the annotation set of release t (created, or the
existing one when it was loaded against the same snapshot), an accession-only
``protein`` row for each member not yet known, with ``first_admitted_release``
written as a minimum as ADR-D49 does, the superset's sequences in ``sequence``
(by md5; ``protein.sequence_id`` keeps today's sequence, decision 5 reads the
paired one through ``swissprot_membership``), every member row whose GO term the
snapshot knows, the ``cut_photo`` row with its ADR-D58 descriptors and its
manifest stored by URI and sha256, the superset proteins' clusters and the
neighbour profiles. A cut has one photo: the same manifest again is a no-op, and
another manifest for a cut that has one is refused.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from pydantic import field_validator
from sqlalchemy import or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from protea.core.code_revision import code_revision
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.cut_photo.build import OUTPUTS_BY_KIND, channel_table_digest
from protea.core.operations._cut_photo_channel_table import (
    check_stored_channel_table,
    declare_channel_table,
)
from protea.core.operations._cut_photo_errors import PhotoRefused
from protea.core.operations.load_goa_annotations import _IDENTITY_ELEMENTS
from protea.core.source_guards import (
    refuse_a_cut_after_the_mark,
    refuse_a_dated_source,
    refuse_a_uniprot_release_at_or_after_the_ceiling,
)
from protea.core.swissprot_table import canonical_table
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.cut_photo import (
    CutPhoto,
    CutPhotoMember,
)
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.protein_go_annotation import (
    ProteinGOAnnotation,
)
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.protein.swissprot_release import SwissProtRelease
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

#: Rows per INSERT. The widest table here has nine columns, so a batch stays well
#: under the 65,535 bind parameters one statement may carry.
_BATCH = 5000

#: How many of the GO ids the snapshot does not know are named in the event.
_LOST_IDS_SHOWN = 20

#: Batches between two progress events. A photo is millions of rows written in one
#: transaction, and the stale-job reaper fails a running job that has emitted nothing
#: for ``stall_seconds`` (30 minutes by default), so a long stage has to keep talking.
_PROGRESS_EVERY = 100


class _Progress:
    """Count one stage's batches and rows, emitting every ``_PROGRESS_EVERY`` batches.

    ``total`` (the stage's row count, read from the parquet footer) also moves the
    job's own progress, through the ``_progress_*`` fields the worker reads.
    """

    def __init__(self, emit: EmitFn, stage: str, total: int | None = None) -> None:
        self.emit, self.stage, self.total = emit, stage, total
        self.batches = self.rows = 0

    def batch(self, rows: int) -> None:
        self.batches += 1
        self.rows += rows
        if self.batches % _PROGRESS_EVERY == 0:
            self._send("load_cut_photo.progress")

    def done(self, **counts: Any) -> None:
        self._send("load_cut_photo.stage_done", **counts)

    def _send(self, event: str, **counts: Any) -> None:
        fields: dict[str, Any] = {"stage": self.stage, "rows": self.rows, **counts}
        if self.total is not None:
            fields |= {"_progress_current": self.rows, "_progress_total": self.total}
        self.emit(event, f"{self.stage}: {self.rows} rows", fields, "info")


def _row_count(path: Path) -> int:
    return int(pq.ParquetFile(path).metadata.num_rows)


class LoadCutPhotoPayload(ProteaPayload, frozen=True):
    """Where the photo's files are, what its manifest hashes to, and its GO graph.

    Everything else (the cut, GOA, UniProt and UniRef) is read from the verified
    manifest. The graph is named here because pairing a cut with a GO release is
    a decision the builder does not take.
    """

    photo_dir: str
    manifest_sha256: str
    ontology_snapshot_id: str

    @field_validator("ontology_snapshot_id", mode="before")
    @classmethod
    def snapshot_must_be_a_uuid(cls, v: Any) -> str:
        return str(uuid.UUID(str(v)))

    @field_validator("manifest_sha256", mode="before")
    @classmethod
    def digest_must_be_hex64(cls, v: Any) -> str:
        text = str(v).strip().lower()
        if len(text) != 64 or any(c not in "0123456789abcdef" for c in text):
            raise ValueError("manifest_sha256 must be 64 hexadecimal characters")
        return text


@dataclass(frozen=True)
class VerifiedPhoto:
    """A photo every check passed: where it is, its manifest and the job loading it."""

    photo_dir: Path
    manifest_sha256: str
    manifest: dict[str, Any]
    job_id: uuid.UUID | None


@dataclass(frozen=True)
class PhotoSources:
    """The facts the checks established, carried to the writer."""

    cut: int
    cut_date: date
    snapshot: OntologySnapshot
    release: SwissProtRelease
    #: None for a truth-only photo, which has no neighbours and so no clusters.
    uniref_release: str | None
    uniref_published_at: date | None
    uniref_clusters_sha256: str | None


def file_sha256(path: Path) -> str:
    """The sha256 of a file, read in blocks."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 22), b""):
            digest.update(block)
    return digest.hexdigest()


def read_manifest(photo_dir: Path, expected_sha256: str) -> dict[str, Any]:
    """The verified manifest, or :class:`PhotoRefused` naming what failed."""
    path = photo_dir / "photo.json"
    if not path.is_file():
        raise PhotoRefused(f"{path} does not exist")
    got = file_sha256(path)
    if got != expected_sha256:
        raise PhotoRefused(f"photo.json hashes to {got}, not to {expected_sha256}")
    manifest: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("status") != "complete":
        raise PhotoRefused(
            f"the photo's status is {manifest.get('status')!r}: {manifest.get('refusals')}"
        )
    required = OUTPUTS_BY_KIND.get(str(manifest.get("kind", "full")))
    if required is None:
        raise PhotoRefused(f"the manifest names an unknown kind of photo: {manifest.get('kind')!r}")
    absent = [name for name in required if name not in manifest["outputs"]]
    if absent:
        raise PhotoRefused(
            f"the manifest lists no {', '.join(absent)}: the photo was built by an older "
            "builder; rebuild it with this revision's"
        )
    for name, recorded in manifest["outputs"].items():
        output = photo_dir / name
        if not output.is_file():
            raise PhotoRefused(f"{name} is listed by the manifest and missing")
        actual = file_sha256(output)
        if actual != recorded:
            raise PhotoRefused(f"{name} hashes to {actual}, the manifest recorded {recorded}")
    return manifest


def graph_date(obo_version: str) -> date:
    """The date of a snapshot's graph, from ``releases/YYYY-MM-DD`` or a bare date."""
    try:
        return date.fromisoformat(obo_version.rsplit("/", 1)[-1])
    except ValueError as exc:
        raise PhotoRefused(f"the GO graph {obo_version!r} carries no date to check") from exc


def _dated(obo_version: str) -> date | None:
    try:
        return date.fromisoformat(obo_version.rsplit("/", 1)[-1])
    except ValueError:
        return None


def check_graph(
    snapshot: OntologySnapshot,
    declared_go_release: str,
    loaded_graphs: Iterable[str],
    cut_date: date | None = None,
) -> None:
    """Refuse a graph the GAF's declared GO release (or the cut's date) does not lead to.

    The rule pairs a cut with the newest graph whose release is not after the one
    the GAF declares, so the graph may not be newer than that release, and no loaded
    graph may sit between the two. A GAF that declares none (GAF 2.1, as GOA wrote up
    to 2019) is bound by the cut's date instead, ADR-D60's pairing rule for cut 160.
    """
    if declared_go_release:
        bound, named = date.fromisoformat(declared_go_release), "the GO release the GAF declares"
    elif cut_date is not None:
        bound, named = cut_date, "the cut's date (the GAF declares no GO release)"
    else:
        raise PhotoRefused("the GAF declares no GO release and no cut date bounds the graph")
    graph = graph_date(snapshot.obo_version)
    if graph > bound:
        raise PhotoRefused(f"the GO graph {snapshot.obo_version} is newer than {named} ({bound})")
    between = sorted(
        v for v in loaded_graphs if (d := _dated(v)) is not None and graph < d <= bound
    )
    if between:
        raise PhotoRefused(
            f"{between[-1]} is loaded and not after {named} ({bound}): the cut takes the "
            f"newest such graph, not {snapshot.obo_version}"
        )


def check_channel_table(manifest: dict[str, Any]) -> None:
    """Refuse a photo classified with another channel table than this platform's.

    The member rows are stored without their channel (ADR-D59, decision 2: the
    channel is a declared table, not a column), so every reader computes it again
    from the platform's table. A photo whose counts and profiles came from another
    table would disagree with every such reading; it is rebuilt, not loaded.
    """
    declared = channel_table_digest()
    built_with = str(manifest.get("channel_table", {}).get("sha256", ""))
    if built_with != declared:
        raise PhotoRefused(
            f"the photo was classified with channel table {built_with}, this platform "
            f"declares {declared}: rebuild it with this revision's builder"
        )


def check_sources(
    manifest: dict[str, Any],
    snapshot: OntologySnapshot,
    release: SwissProtRelease,
    loaded_graphs: Iterable[str] = (),
) -> PhotoSources:
    """Refuse what the guard refuses, a graph the GAF does not lead to, and a
    Swiss-Prot release that is not the loaded one."""
    cut, cut_date = int(manifest["cut_release"]), date.fromisoformat(manifest["cut_date"])
    sources = manifest["sources"]
    refuse_a_cut_after_the_mark(cut)
    goa, uniprot = sources["goa"], sources["uniprot"]
    if int(goa["release"]) != cut or date.fromisoformat(goa["published"]) != cut_date:
        raise PhotoRefused(
            f"the cut is GOA {cut} of {cut_date}, the manifest's GOA is {goa['release']} "
            f"of {goa['published']}"
        )
    uniref_release, uniref_published, uniref_sha256 = _checked_uniref(manifest, cut, cut_date)
    for name, published in (
        (f"UniProt {release.release}", release.published_at),
        (f"GO {snapshot.obo_version}", graph_date(snapshot.obo_version)),
    ):
        refuse_a_dated_source(source=name, published=published, cut_release=cut, cut_date=cut_date)
    refuse_a_uniprot_release_at_or_after_the_ceiling(release.release)
    check_graph(snapshot, str(goa.get("declared_go_release") or ""), loaded_graphs, cut_date)
    check_channel_table(manifest)
    declared = (uniprot["release"], uniprot["published"], uniprot["superset_sha256"])
    loaded = (release.release, release.published_at.isoformat(), release.content_sha256)
    if declared != loaded:
        raise PhotoRefused(f"the manifest's Swiss-Prot is {declared}, the loaded one is {loaded}")
    built_from = int(manifest["counts"]["members"]["superset"])
    if built_from != release.accession_count:
        raise PhotoRefused(
            f"the photo was built from {built_from} Swiss-Prot entries, release "
            f"{release.release} holds {release.accession_count}"
        )
    return PhotoSources(
        cut=cut,
        cut_date=cut_date,
        snapshot=snapshot,
        release=release,
        uniref_release=uniref_release,
        uniref_published_at=uniref_published,
        uniref_clusters_sha256=uniref_sha256,
    )


def _checked_uniref(
    manifest: dict[str, Any], cut: int, cut_date: date
) -> tuple[str | None, date | None, str | None]:
    """The UniRef release a full photo's clusters come from, checked against the guard.

    A truth-only photo has no neighbours and must name no UniRef release.
    """
    uniref = manifest["sources"].get("uniref") or {}
    if manifest.get("kind", "full") == "truth_only":
        if uniref:
            raise PhotoRefused("a truth-only photo names a UniRef release: it has no neighbours")
        return None, None, None
    if not uniref:
        raise PhotoRefused("a full photo names no UniRef release for its neighbours")
    published = date.fromisoformat(uniref["published"])
    refuse_a_dated_source(
        source=f"UniRef {uniref['release']}",
        published=published,
        cut_release=cut,
        cut_date=cut_date,
    )
    refuse_a_uniprot_release_at_or_after_the_ceiling(str(uniref["release"]), source="UniRef")
    return str(uniref["release"]), published, str(uniref["sha256"])


def _batches(path: Path, columns: list[str]) -> Iterator[list[dict[str, Any]]]:
    for batch in pq.ParquetFile(path).iter_batches(batch_size=_BATCH, columns=columns):
        yield batch.to_pylist()


def verify_sequences(path: Path, expected_sha256: str) -> int:
    """Refuse sequences that are not the paired release, byte for byte; return how many.

    Every md5 must be the md5 of its sequence, and the accessions with their md5s
    must hash to the canonical table the loaded release was verified against.
    """
    table_of: dict[str, str] = {}
    for batch in _batches(path, ["accession", "md5", "sequence"]):
        for row in batch:
            # md5 is UniProt's sequence fingerprint, fixed by the data, not a security choice.
            got = hashlib.md5(row["sequence"].encode(), usedforsecurity=False).hexdigest()
            if got != row["md5"]:
                raise PhotoRefused(f"{row['accession']}: its sequence hashes to {got}")
            if row["accession"] in table_of:
                raise PhotoRefused(f"{row['accession']} appears twice in {path.name}")
            table_of[row["accession"]] = row["md5"]
    digest = hashlib.sha256(canonical_table(table_of)).hexdigest()
    if digest != expected_sha256:
        raise PhotoRefused(
            f"{path.name} hashes to {digest} as a table, the release to {expected_sha256}"
        )
    return len(table_of)


def verify_unreviewed_sequences(path: Path) -> int:
    """Refuse a TrEMBL sequence its md5 does not describe; return how many there are.

    The manifest pinned the file's sha256 and the archive's published md5, checked
    on the stream the builder read; this checks each record, as ``verify_sequences``
    does for Swiss-Prot.
    """
    count = 0
    for batch in _batches(path, ["accession", "md5", "sequence"]):
        for row in batch:
            # md5 is UniProt's sequence fingerprint, fixed by the data, not a security choice.
            got = hashlib.md5(row["sequence"].encode(), usedforsecurity=False).hexdigest()
            if got != row["md5"]:
                raise PhotoRefused(f"{row['accession']}: its sequence hashes to {got}")
            count += 1
    return count


def photo_descriptors(
    sources: PhotoSources, counts: dict[str, Any], goref_list_read_on: str
) -> dict[str, str]:
    """ADR-D58's descriptors of the photo, derived, never typed."""
    members, rows = counts["members"], counts["rows"]
    cut, uniref = sources.cut, sources.uniref_release
    graph = sources.snapshot.obo_version.rsplit("/", 1)[-1]
    neighbours = (
        f"{members.get('by_neighbour', 0)} as annotated neighbours in the UniRef50 {uniref} "
        "clusters of a Swiss-Prot protein"
        if uniref
        else "no neighbours (a truth-only photo: the cut only ends a window)"
    )
    return {
        "short_name": f"photo-{cut}",
        "description": (
            f"Photo of cut {cut} (GOA {cut}, {sources.cut_date.isoformat()}): "
            f"{members['members']} members, {members['superset']} of them in Swiss-Prot "
            f"{sources.release.release}, {members['by_truth']} admitted by a truth code, "
            f"{members['by_curated_only']} by curated inference alone and {neighbours}, "
            f"with {rows.get('member_rows', 0)} GOA rows on the GO graph of {graph}, "
            f"channels checked against the GO_REF list read on {goref_list_read_on}."
        ),
    }


def _annotation_set(
    session: Session, sources: PhotoSources, manifest_sha256: str, goa: dict[str, Any]
) -> tuple[AnnotationSet, bool]:
    """The annotation set of release t: the existing one on the same graph, or a new one."""
    existing = (
        session.execute(
            select(AnnotationSet).where(
                AnnotationSet.source == "goa", AnnotationSet.source_version == str(sources.cut)
            )
        )
        .scalars()
        .all()
    )
    if len(existing) > 1:
        raise PhotoRefused(f"GOA {sources.cut} has {len(existing)} annotation sets; which is t?")
    if existing:
        found = existing[0]
        if found.ontology_snapshot_id != sources.snapshot.id:
            raise PhotoRefused(
                f"the annotation set of GOA {sources.cut} is on snapshot "
                f"{found.ontology_snapshot_id}, the photo on {sources.snapshot.id}"
            )
        return found, False
    created = AnnotationSet(
        source="goa",
        source_version=str(sources.cut),
        ontology_snapshot_id=sources.snapshot.id,
        source_published_at=datetime.combine(sources.cut_date, time(0), tzinfo=UTC),
        meta={
            "created_by": "load_cut_photo",
            "cut_photo_manifest_sha256": manifest_sha256,
            "gaf_go_version": goa.get("go_version", ""),
            "gaf_date_generated": goa.get("date_generated", ""),
        },
    )
    session.add(created)
    session.flush()
    return created, True


def _store_proteins(session: Session, path: Path, cut: int, emit: EmitFn) -> int:
    added = 0
    progress = _Progress(emit, "proteins")
    for batch in _batches(path, ["accession"]):
        values = []
        for row in batch:
            canonical, is_canonical, isoform = Protein.parse_isoform(row["accession"])
            values.append(
                {
                    "accession": row["accession"],
                    "canonical_accession": canonical,
                    "is_canonical": is_canonical,
                    "isoform_index": isoform,
                    "first_admitted_release": cut,
                }
            )
        inserted = session.execute(
            pg_insert(Protein.__table__)
            .values(values)
            .on_conflict_do_nothing()
            .returning(Protein.__table__.c.accession)
        )
        added += len(inserted.all())
        session.execute(
            update(Protein)
            .where(Protein.accession.in_([v["accession"] for v in values]))
            .where(
                or_(Protein.first_admitted_release.is_(None), Protein.first_admitted_release > cut)
            )
            .values(first_admitted_release=cut)
            .execution_options(synchronize_session=False)
        )
        progress.batch(len(values))
    progress.done(proteins_added=added)
    return added


def _store_sequences(
    session: Session, path: Path, emit: EmitFn, stage: str = "sequences"
) -> tuple[int, int]:
    added = offered = 0
    progress = _Progress(emit, stage)
    for batch in _batches(path, ["md5", "sequence"]):
        unique = {row["md5"]: row["sequence"] for row in batch}
        offered += len(unique)
        values = [{"sequence_hash": md5, "sequence": seq} for md5, seq in unique.items()]
        inserted = session.execute(
            pg_insert(Sequence.__table__)
            .values(values)
            .on_conflict_do_nothing(index_elements=["sequence_hash"])
            .returning(Sequence.__table__.c.sequence_hash)
        )
        added += len(inserted.all())
        progress.batch(len(batch))
    progress.done(sequences_added=added, sequences_already_present=offered - added)
    return added, offered - added


def _store_both_sequence_files(session: Session, photo_dir: Path, emit: EmitFn) -> dict[str, int]:
    """The Swiss-Prot and the TrEMBL sequences of the paired release, stored by md5.

    A truth-only photo has no TrEMBL file; its counts say so with zeros.
    """
    counts: dict[str, int] = {}
    for name in ("sequences", "unreviewed_sequences"):
        path = photo_dir / f"{name}.parquet"
        if not path.exists():
            counts[f"{name}_added"] = counts[f"{name}_already_present"] = 0
            continue
        added, reused = _store_sequences(session, path, emit, name)
        counts[f"{name}_added"], counts[f"{name}_already_present"] = added, reused
    return counts


def _store_annotations(
    session: Session,
    path: Path,
    annotation_set_id: uuid.UUID,
    snapshot_id: uuid.UUID,
    emit: EmitFn,
) -> dict[str, Any]:
    terms = dict(
        session.execute(
            select(GOTerm.go_id, GOTerm.id).where(GOTerm.ontology_snapshot_id == snapshot_id)
        ).all()
    )
    offered = inserted = 0
    lost: Counter[str] = Counter()
    progress = _Progress(emit, "rows", total=_row_count(path))
    columns = ["accession", "go_id", "qualifier", "code", "reference", "with_from", "date"]
    for batch in _batches(path, [*columns, "assigned_by"]):
        values = []
        for row in batch:
            term = terms.get(row["go_id"])
            if term is None:
                lost[row["go_id"]] += 1
                continue
            values.append(
                {
                    "annotation_set_id": annotation_set_id,
                    "protein_accession": row["accession"],
                    "go_term_id": term,
                    "qualifier": row["qualifier"] or None,
                    "evidence_code": row["code"] or None,
                    "assigned_by": row["assigned_by"] or None,
                    "db_reference": row["reference"] or None,
                    "with_from": row["with_from"] or None,
                    "annotation_date": row["date"] or None,
                }
            )
        if values:
            offered += len(values)
            result = session.execute(
                pg_insert(ProteinGOAnnotation.__table__)
                .values(values)
                .on_conflict_do_nothing(index_elements=_IDENTITY_ELEMENTS)
                .returning(ProteinGOAnnotation.__table__.c.id)
            )
            inserted += len(result.all())
        progress.batch(len(batch))
    progress.done(rows_inserted=inserted)
    return {
        "rows_inserted": inserted,
        "rows_already_present": offered - inserted,
        "rows_with_unknown_term": sum(lost.values()),
        "unknown_terms": len(lost),
        "unknown_terms_most_frequent": dict(lost.most_common(_LOST_IDS_SHOWN)),
    }


def _store_members(
    session: Session, photo_dir: Path, photo_id: uuid.UUID, emit: EmitFn
) -> dict[str, int]:
    """One ``cut_photo_member`` row per member, with the md5 of its paired sequence.

    A superset member's md5 is in ``members.parquet`` (the canonical Swiss-Prot table);
    any other member's is the one the TrEMBL pass found, or NULL when the paired
    release does not hold the accession.
    """
    trembl: dict[str, str] = {}
    unreviewed = photo_dir / "unreviewed_sequences.parquet"
    for batch in _batches(unreviewed, ["accession", "md5"]) if unreviewed.exists() else ():
        trembl.update((row["accession"], row["md5"]) for row in batch)
    counts: Counter[str] = Counter()
    progress = _Progress(emit, "members")
    columns = ["accession", "superset", "truth", "curated", "neighbour", "cluster", "md5"]
    for batch in _batches(photo_dir / "members.parquet", columns):
        values = []
        for row in batch:
            md5 = row.pop("md5") or trembl.get(row["accession"])
            values.append({**row, "photo_id": photo_id, "sequence_md5": md5})
            counts["members"] += 1
            counts["members_with_paired_sequence"] += md5 is not None
            counts["neighbours"] += bool(row["neighbour"])
        session.execute(pg_insert(CutPhotoMember.__table__).values(values))
        progress.batch(len(values))
    progress.done(**counts)
    return dict(counts)


def manifest_key_for(cut: int, manifest_sha256: str) -> str:
    """Where a photo's manifest lives in the artifact store: the one spelling."""
    return f"cut-photos/{cut}/{manifest_sha256}/photo.json"


def store_manifest(photo_dir: Path, cut: int, manifest_sha256: str) -> str:
    """Put the verified manifest in the artifact store and return its URI."""
    project_root = Path(__file__).resolve().parents[3]
    store = get_artifact_store(load_settings(project_root))
    return str(store.put(manifest_key_for(cut, manifest_sha256), photo_dir / "photo.json"))


class LoadCutPhotoOperation:
    """Write one verified photo of a cut (ADR-D59)."""

    name = "load_cut_photo"
    description = (
        "Verify the photo of a cut built away from the server (its manifest, its "
        "sources against the guard and the loaded Swiss-Prot release, its sequences "
        "byte for byte) and write its rows, clusters and neighbour profiles (ADR-D59)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        digest = str(payload.get("manifest_sha256", ""))[:12]
        return f"photo {digest} <- {payload.get('photo_dir', '')}" if digest else ""

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = LoadCutPhotoPayload.model_validate(contract_payload(payload))
        photo_dir = Path(p.photo_dir)
        manifest = read_manifest(photo_dir, p.manifest_sha256)
        cut = int(manifest["cut_release"])
        existing = session.execute(
            select(CutPhoto).where(CutPhoto.cut_release == cut)
        ).scalar_one_or_none()
        if existing is not None:
            if existing.manifest_sha256 == p.manifest_sha256:
                emit(
                    "load_cut_photo.already_loaded",
                    f"cut {cut} already has this photo",
                    {"cut": cut, "photo_id": str(existing.id)},
                    "info",
                )
                return OperationResult(result={"photo_id": str(existing.id), "skipped": True})
            raise PhotoRefused(
                f"cut {cut} already has a photo, with manifest {existing.manifest_sha256}"
            )
        snapshot = session.get(OntologySnapshot, uuid.UUID(p.ontology_snapshot_id))
        if snapshot is None:
            raise PhotoRefused(f"ontology snapshot {p.ontology_snapshot_id} is not loaded")
        release_name = str(manifest["sources"]["uniprot"]["release"])
        release = session.execute(
            select(SwissProtRelease).where(SwissProtRelease.release == release_name)
        ).scalar_one_or_none()
        if release is None:
            raise PhotoRefused(f"Swiss-Prot {release_name} is not loaded")
        loaded_graphs = session.execute(select(OntologySnapshot.obo_version)).scalars().all()
        sources = check_sources(manifest, snapshot, release, loaded_graphs)
        check_stored_channel_table(session)
        sequences = verify_sequences(photo_dir / "sequences.parquet", release.content_sha256)
        unreviewed = (
            verify_unreviewed_sequences(photo_dir / "unreviewed_sequences.parquet")
            if "unreviewed_sequences.parquet" in manifest["outputs"]
            else 0
        )
        emit(
            "load_cut_photo.verified",
            f"photo of cut {cut} verified against Swiss-Prot {release.release}",
            {"cut": cut, "sequences": sequences, "unreviewed_sequences": unreviewed},
            "info",
        )
        verified = VerifiedPhoto(
            photo_dir=photo_dir,
            manifest_sha256=p.manifest_sha256,
            manifest=manifest,
            job_id=job_id_from_payload(payload),
        )
        return self._write(session, verified, sources, emit)

    def _write(
        self, session: Session, verified: VerifiedPhoto, sources: PhotoSources, emit: EmitFn
    ) -> OperationResult:
        photo_dir, manifest = verified.photo_dir, verified.manifest
        annotation_set, created = _annotation_set(
            session, sources, verified.manifest_sha256, manifest["sources"]["goa"]
        )
        proteins = _store_proteins(session, photo_dir / "members.parquet", sources.cut, emit)
        sequences = _store_both_sequence_files(session, photo_dir, emit)
        rows = _store_annotations(
            session, photo_dir / "rows.parquet", annotation_set.id, sources.snapshot.id, emit
        )
        emit("load_cut_photo.rows", None, rows, "info")
        channel_table = manifest["channel_table"]
        table_written = declare_channel_table(session, verified.job_id)
        photo = CutPhoto(
            kind=str(manifest.get("kind", "full")),
            cut_release=sources.cut,
            cut_date=sources.cut_date,
            annotation_set_id=annotation_set.id,
            ontology_snapshot_id=sources.snapshot.id,
            swissprot_release_id=sources.release.id,
            uniref_release=sources.uniref_release,
            uniref_published_at=sources.uniref_published_at,
            uniref_clusters_sha256=sources.uniref_clusters_sha256,
            goref_list_read_on=date.fromisoformat(channel_table["goref_list_read_on"]),
            channel_table_sha256=channel_table["sha256"],
            manifest=manifest,
            manifest_uri=store_manifest(photo_dir, sources.cut, verified.manifest_sha256),
            manifest_sha256=verified.manifest_sha256,
            code_revision=code_revision() or None,
            builder_revision=str(manifest.get("revision") or "") or None,
            job_id=verified.job_id,
            **photo_descriptors(sources, manifest["counts"], channel_table["goref_list_read_on"]),
        )
        session.add(photo)
        session.flush()
        members = _store_members(session, photo_dir, photo.id, emit)
        result = {
            "photo_id": str(photo.id),
            "short_name": photo.short_name,
            "annotation_set_id": str(annotation_set.id),
            "annotation_set_created": created,
            "proteins_added": proteins,
            **sequences,
            **{k: v for k, v in rows.items() if k != "unknown_terms_most_frequent"},
            **members,
            "manifest_uri": photo.manifest_uri,
            "channel_table_sha256": photo.channel_table_sha256,
            "channel_table_written": table_written,
        }
        emit("load_cut_photo.done", f"{photo.short_name} loaded", result, "info")
        return OperationResult(result=result)
