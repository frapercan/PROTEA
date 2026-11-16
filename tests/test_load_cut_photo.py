"""ADR-D59's loader: the photo of a cut, verified before anything is written.

Without a database: each refusal the loader names (a manifest that is not the
one named, a refused photo, an output changed after the build, a source the
guard refuses, a Swiss-Prot release that is not the loaded one, sequences that
are not that release), the descriptors as ADR-D58 slugs, and the model and the
migration declaring the same checks. Against Postgres (``--with-postgres``, CI):
a photo built by the builder's own passes loads, the same manifest again is a
no-op, another manifest for the cut is refused, the protein rows keep the
earliest admission and today's sequence pointer is left alone.

The photo is built by :mod:`protea.core.cut_photo.build`, not written by hand,
so these tests read what the builder really writes.
"""

from __future__ import annotations

import gzip
import hashlib
import importlib.util
import io
import json
import re
import tarfile
import uuid
from datetime import date
from pathlib import Path
from types import ModuleType
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.core._window_population import superset_at_start
from protea.core.cut_photo.build import (
    KNOWLEDGEBASE_STREAM_MD5,
    channel_table_digest,
    manifest_pass,
    members_pass,
    rows_pass,
    sequences_pass,
    unreviewed_pass,
)
from protea.core.cut_photo.channels import channel_rows
from protea.core.cut_photo.measure import PassSource
from protea.core.operations import load_cut_photo as loader
from protea.core.operations.load_cut_photo import (
    LoadCutPhotoOperation,
    LoadCutPhotoPayload,
    PhotoRefused,
    check_sources,
    file_sha256,
    photo_descriptors,
    read_manifest,
    verify_sequences,
)
from protea.core.source_guards import SourceGuardError
from protea.core.swissprot_table import canonical_table
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation import cut_photo as model
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.cut_photo import (
    CutPhoto,
    CutPhotoMember,
)
from protea.infrastructure.orm.models.annotation.evidence_channel import (
    EvidenceChannel,
    EvidenceChannelTable,
)
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.protein_go_annotation import (
    ProteinGOAnnotation,
)
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.protein.swissprot_release import (
    SwissProtMembership,
    SwissProtRelease,
)
from protea.infrastructure.orm.models.sequence.sequence import Sequence

CUT = 211
CUT_DATE = date(2022, 9, 16)
UNIPROT = {"release": "2022_03", "published": "2022-08-03"}
SEQUENCES = {"P1": "MKV", "P2": "MAAGG"}
MD5 = {a: hashlib.md5(s.encode(), usedforsecurity=False).hexdigest() for a, s in SEQUENCES.items()}
SUPERSET_SHA256 = hashlib.sha256(canonical_table(MD5)).hexdigest()
GRAPH = "releases/2022-07-01"
GOA_FILE = {
    "url": "https://ftp.ebi.ac.uk/pub/databases/GO/goa/old/UNIPROT/goa_uniprot_all.gaf.211.gz",
    "sha256": "b" * 64,
}
KB_FILE = {
    "url": "https://ftp.uniprot.org/pub/databases/uniprot/previous_releases/release-2022_03/"
    "knowledgebase/knowledgebase2022_03.tar.gz",
    "md5": "3d030df00f7ec4fd5b2d43ca74e3e4f7",
}
#: Q9 is the member outside the superset; its sequence comes from the paired TrEMBL.
TREMBL = [("Q9", "MQQLK"), ("X7", "MAA"), ("N1", "MNN"), ("N2", "MPP")]
#: The GO ids the test snapshot knows; GO:0000099 is the one it does not.
KNOWN_TERMS = [f"GO:000000{i}" for i in range(1, 8)]
SLUG = re.compile(r"^[a-z0-9][a-z0-9-]*$")

_VERSIONS = Path(__file__).resolve().parents[1] / "alembic" / "versions"
_MIGRATION = _VERSIONS / "d4a7c2e9f310_cut_photo_tables.py"
_MEMBERS_MIGRATION = _VERSIONS / "384568a09847_cut_photo_members.py"
_KIND_MIGRATION = _VERSIONS / "a6e1e05d3c8b_cut_photo_kind.py"


def _flat(entries: list[tuple[str, str]]) -> bytes:
    """A UniProt flat file (``.dat``) holding these (accession, sequence) entries."""
    out = []
    for accession, sequence in entries:
        out += [
            f"ID   {accession}_HUMAN    Unreviewed;    {len(sequence)} AA.\n",
            f"AC   {accession};\n",
            "DE   SubName: Full=Uncharacterized protein; see https://example.org//x\n",
            f"SQ   SEQUENCE   {len(sequence)} AA;  1 MW;  0 CRC64;\n",
            f"     {sequence}\n",
            "//\n",
        ]
    return "".join(out).encode()


def _gaf(accession: str, go: str, code: str, reference: str) -> str:
    cols = [""] * 17
    cols[0], cols[1], cols[3], cols[4], cols[5], cols[6] = (
        "UniProtKB",
        accession,
        "enables",
        go,
        reference,
        code,
    )
    cols[8], cols[12], cols[13], cols[14] = "F", "taxon:9606", "20220101", "UniProt"
    return "\t".join(cols) + "\n"


GAF = [
    "!gaf-version: 2.2\n",
    "!date-generated: 2022-09-16 10:43\n",
    "!go-version: http://purl.obolibrary.org/obo/go/releases/2022-09-12/extensions/go-plus.owl\n",
    _gaf("P1", "GO:0000001", "IDA", "PMID:1"),
    _gaf("P1", "GO:0000002", "IEA", "GO_REF:0000043"),
    _gaf("Q9", "GO:0000003", "ISS", "GO_REF:0000024"),
    _gaf("Q9", "GO:0000099", "IEA", "GO_REF:0000043"),
    _gaf("N1", "GO:0000005", "IBA", "GO_REF:0000033"),
    _gaf("N2", "GO:0000006", "IEA", "GO_REF:0000107"),
]


def _record(path: Path, pass_name: str, **extra: object) -> None:
    head = {"pass": pass_name, **UNIPROT, "cut_release": CUT, "cut_date": CUT_DATE.isoformat()}
    path.write_text(json.dumps({**head, **extra}))


def build_photo(
    tmp_path: Path, gaf: list[str] = GAF, *, truth_only: bool = False
) -> tuple[Path, str]:
    """A photo of cut 211 built by the builder's passes: its directory and manifest sha256."""
    measured = tmp_path / "measured"
    measured.mkdir(parents=True)
    (measured / "superset.tsv").write_bytes(canonical_table(MD5))
    _record(measured / "superset.json", "superset", sha256=SUPERSET_SHA256)
    _record(measured / "neighbours.json", "neighbours")
    pq.write_table(
        pa.table(
            {
                "cluster": ["C1", "C1", "C1", "C2"],
                "accession": ["P1", "N1", "N2", "P2"],
                "in_superset": [True, False, False, True],
            }
        ),
        measured / "neighbours.parquet",
    )
    out = tmp_path / "photo"
    goa = PassSource(release=CUT, published=CUT_DATE, cut_release=CUT, cut_date=CUT_DATE, out=out)
    members_pass(
        gaf,
        superset_table=measured / "superset.tsv",
        neighbours_path=None if truth_only else measured / "neighbours.parquet",
        source=goa,
        source_file=GOA_FILE,
    )
    rows_pass(gaf, members_path=out / "members.parquet", source=goa, source_file=GOA_FILE)
    fasta = "".join(f">sp|{a}|{a}_HUMAN\n{s}\n" for a, s in SEQUENCES.items()).encode()
    blob = gzip.compress(fasta)
    archive = tmp_path / "sprot.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        info = tarfile.TarInfo("uniprot_sprot.fasta.gz")
        info.size = len(blob)
        tar.addfile(info, io.BytesIO(blob))
    uniprot = PassSource(
        release="2022_03",
        published=date(2022, 8, 3),
        cut_release=CUT,
        cut_date=CUT_DATE,
        out=out,
    )
    sequences_pass(archive, uniprot, source_url="https://ftp.uniprot.org/sprot-only2022_03.tar.gz")
    if not truth_only:
        unreviewed_pass(
            io.BytesIO(_flat(TREMBL)),
            members_path=out / "members.parquet",
            source=uniprot,
            source_file=KB_FILE,
        )
        (out / KNOWLEDGEBASE_STREAM_MD5).write_text(KB_FILE["md5"] + "\n")
    manifest_pass(out, revision="builder-revision")
    return out, file_sha256(out / "photo.json")


def _snapshot(obo_version: str = GRAPH) -> OntologySnapshot:
    return OntologySnapshot(id=uuid.uuid4(), obo_url="file:///go.obo", obo_version=obo_version)


def _release(**changes: Any) -> SwissProtRelease:
    fields: dict[str, Any] = {
        "id": uuid.uuid4(),
        "release": "2022_03",
        "published_at": date(2022, 8, 3),
        "source_url": "file:///sprot.tar.gz",
        "accession_count": len(MD5),
        "content_sha256": SUPERSET_SHA256,
        **changes,
    }
    return SwissProtRelease(**fields)


def _migration(path: Path = _MIGRATION) -> ModuleType:
    """Loaded by path: ``alembic/`` is not a package and the library shadows the name."""
    spec = importlib.util.spec_from_file_location(f"_{path.stem}", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestTheManifest:
    def test_a_built_photo_reads_back(self, tmp_path: Path) -> None:
        out, sha = build_photo(tmp_path)
        manifest = read_manifest(out, sha)
        assert manifest["cut_release"] == CUT
        assert manifest["sources"]["uniref"]["release"] == "2022_03"

    def test_a_manifest_other_than_the_named_one_is_refused(self, tmp_path: Path) -> None:
        out, _ = build_photo(tmp_path)
        with pytest.raises(PhotoRefused, match="photo.json hashes to"):
            read_manifest(out, "0" * 64)

    def test_a_refused_photo_is_refused(self, tmp_path: Path) -> None:
        unknown = _gaf("N2", "GO:0000007", "IEA", "Reactome:R-1")
        out, sha = build_photo(tmp_path, [*GAF, unknown])
        with pytest.raises(PhotoRefused, match="status is 'refused'"):
            read_manifest(out, sha)

    def test_a_manifest_without_the_trembl_output_is_refused(self, tmp_path: Path) -> None:
        out, _ = build_photo(tmp_path)
        manifest = json.loads((out / "photo.json").read_text())
        del manifest["outputs"]["unreviewed_sequences.parquet"]
        (out / "photo.json").write_text(json.dumps(manifest))
        with pytest.raises(PhotoRefused, match="lists no unreviewed_sequences.parquet"):
            read_manifest(out, file_sha256(out / "photo.json"))

    def test_an_unknown_kind_of_photo_is_refused(self, tmp_path: Path) -> None:
        out, _ = build_photo(tmp_path)
        manifest = json.loads((out / "photo.json").read_text())
        (out / "photo.json").write_text(json.dumps({**manifest, "kind": "partial"}))
        with pytest.raises(PhotoRefused, match="unknown kind of photo"):
            read_manifest(out, file_sha256(out / "photo.json"))

    def test_a_truth_only_photo_needs_no_trembl_and_names_no_uniref(self, tmp_path: Path) -> None:
        out, sha = build_photo(tmp_path, truth_only=True)
        manifest = read_manifest(out, sha)
        sources = check_sources(manifest, _snapshot(), _release())
        assert manifest["kind"] == "truth_only"
        assert (sources.uniref_release, sources.uniref_clusters_sha256) == (None, None)

    def test_a_truth_only_photo_that_names_uniref_is_refused(self, tmp_path: Path) -> None:
        out, sha = build_photo(tmp_path, truth_only=True)
        manifest = read_manifest(out, sha)
        manifest["sources"]["uniref"] = {"release": "2022_03", "published": "2022-08-03"}
        with pytest.raises(PhotoRefused, match="names a UniRef release"):
            check_sources(manifest, _snapshot(), _release())

    def test_a_full_photo_that_names_no_uniref_is_refused(self, tmp_path: Path) -> None:
        out, sha = build_photo(tmp_path)
        manifest = read_manifest(out, sha)
        manifest["sources"]["uniref"] = {}
        with pytest.raises(PhotoRefused, match="names no UniRef release"):
            check_sources(manifest, _snapshot(), _release())

    def test_a_trembl_sequence_its_md5_does_not_describe_is_refused(self, tmp_path: Path) -> None:
        path = tmp_path / "unreviewed_sequences.parquet"
        pq.write_table(pa.table({"accession": ["Q9"], "md5": ["0" * 32], "sequence": ["MQ"]}), path)
        with pytest.raises(PhotoRefused, match="Q9: its sequence hashes to"):
            loader.verify_unreviewed_sequences(path)

    def test_an_output_changed_after_the_build_is_refused(self, tmp_path: Path) -> None:
        out, sha = build_photo(tmp_path)
        pq.write_table(pa.table({"accession": ["P1"], "cluster": ["C9"]}), out / "members.parquet")
        with pytest.raises(PhotoRefused, match="members.parquet hashes to"):
            read_manifest(out, sha)


class TestTheSources:
    def _manifest(self, tmp_path: Path) -> dict[str, Any]:
        out, sha = build_photo(tmp_path)
        return read_manifest(out, sha)

    def test_the_paired_sources_pass(self, tmp_path: Path) -> None:
        sources = check_sources(self._manifest(tmp_path), _snapshot(), _release())
        assert (sources.cut, sources.cut_date) == (CUT, CUT_DATE)
        assert sources.uniref_release == "2022_03"
        assert len(sources.uniref_clusters_sha256) == 64

    def test_a_graph_published_after_the_cut_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(SourceGuardError, match="after the cut"):
            check_sources(self._manifest(tmp_path), _snapshot("releases/2022-10-01"), _release())

    def test_a_graph_with_no_date_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(PhotoRefused, match="no date"):
            check_sources(self._manifest(tmp_path), _snapshot("go-basic"), _release())

    def test_a_cut_after_the_mark_is_refused(self, tmp_path: Path) -> None:
        manifest = {**self._manifest(tmp_path), "cut_release": 228}
        with pytest.raises(SourceGuardError, match="after the mark"):
            check_sources(manifest, _snapshot(), _release())

    def test_a_goa_release_that_is_not_the_cut_is_refused(self, tmp_path: Path) -> None:
        manifest = self._manifest(tmp_path)
        manifest["sources"]["goa"] = {"release": 210, "published": "2022-07-01"}
        with pytest.raises(PhotoRefused, match="the manifest's GOA is 210"):
            check_sources(manifest, _snapshot(), _release())

    def test_a_graph_newer_than_the_release_the_gaf_declares_is_refused(
        self, tmp_path: Path
    ) -> None:
        manifest = self._manifest(tmp_path)
        manifest["sources"]["goa"]["declared_go_release"] = "2022-06-30"
        with pytest.raises(PhotoRefused, match="newer than the GO release the GAF declares"):
            check_sources(manifest, _snapshot(), _release())

    def test_a_newer_loaded_graph_the_gaf_allows_is_refused(self, tmp_path: Path) -> None:
        loaded = ["releases/2022-07-01", "releases/2022-08-01", "releases/2022-10-01"]
        with pytest.raises(PhotoRefused, match="2022-08-01 is loaded"):
            check_sources(self._manifest(tmp_path), _snapshot(), _release(), loaded)

    def test_older_and_later_graphs_do_not_change_the_choice(self, tmp_path: Path) -> None:
        loaded = ["releases/2021-10-26", GRAPH, "releases/2025-07-22", "go-basic"]
        sources = check_sources(self._manifest(tmp_path), _snapshot(), _release(), loaded)
        assert sources.cut == CUT

    def test_a_gaf_that_declares_no_go_release_takes_the_newest_graph_by_the_cut(
        self, tmp_path: Path
    ) -> None:
        manifest = self._manifest(tmp_path)
        manifest["sources"]["goa"]["declared_go_release"] = ""
        assert check_sources(manifest, _snapshot(), _release(), [GRAPH]).cut == CUT
        # A newer graph not after the cut's date is the one the cut takes.
        with pytest.raises(PhotoRefused, match="the cut's date"):
            check_sources(manifest, _snapshot(), _release(), [GRAPH, "releases/2022-09-12"])

    def test_a_photo_classified_with_another_channel_table_is_refused(self, tmp_path: Path) -> None:
        manifest = self._manifest(tmp_path)
        manifest["channel_table"]["sha256"] = "c" * 64
        with pytest.raises(PhotoRefused, match="classified with channel table"):
            check_sources(manifest, _snapshot(), _release())

    def test_another_swissprot_than_the_loaded_one_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(PhotoRefused, match="the loaded one is"):
            check_sources(self._manifest(tmp_path), _snapshot(), _release(content_sha256="f" * 64))

    def test_a_superset_count_the_release_does_not_hold_is_refused(self, tmp_path: Path) -> None:
        with pytest.raises(PhotoRefused, match="holds 3"):
            check_sources(self._manifest(tmp_path), _snapshot(), _release(accession_count=3))


class TestTheSequences:
    def test_the_built_sequences_are_the_release(self, tmp_path: Path) -> None:
        out, _ = build_photo(tmp_path)
        assert verify_sequences(out / "sequences.parquet", SUPERSET_SHA256) == 2

    def test_a_sequence_its_md5_does_not_describe_is_refused(self, tmp_path: Path) -> None:
        path = tmp_path / "sequences.parquet"
        table = {"accession": ["P1"], "md5": [MD5["P2"]], "sequence": ["MKV"]}
        pq.write_table(pa.table(table), path)
        with pytest.raises(PhotoRefused, match="P1: its sequence hashes to"):
            verify_sequences(path, SUPERSET_SHA256)

    def test_sequences_of_another_release_are_refused(self, tmp_path: Path) -> None:
        out, _ = build_photo(tmp_path)
        with pytest.raises(PhotoRefused, match="as a table"):
            verify_sequences(out / "sequences.parquet", "e" * 64)


class TestTheDescriptors:
    def test_the_short_name_is_a_slug_and_the_description_names_each_set(
        self, tmp_path: Path
    ) -> None:
        out, sha = build_photo(tmp_path)
        manifest = read_manifest(out, sha)
        sources = check_sources(manifest, _snapshot(), _release())
        names = photo_descriptors(sources, manifest["counts"], "2026-11-16")
        assert sorted(names) == ["description", "short_name"]
        assert names["short_name"] == "photo-211" and SLUG.match(names["short_name"])
        assert "Swiss-Prot 2022_03" in names["description"]
        assert "2022-07-01" in names["description"]
        assert "2 as annotated neighbours in the UniRef50 2022_03 clusters" in names["description"]

    def test_the_payload_refuses_a_digest_that_is_not_one(self) -> None:
        with pytest.raises(ValueError, match="64 hexadecimal"):
            LoadCutPhotoPayload.model_validate(
                {"photo_dir": "/x", "manifest_sha256": "abc", "ontology_snapshot_id": uuid.uuid4()}
            )

    def test_the_payload_takes_the_snapshot_as_json_sends_it(self) -> None:
        snapshot = uuid.uuid4()
        p = LoadCutPhotoPayload.model_validate(
            {"photo_dir": "/x", "manifest_sha256": "a" * 64, "ontology_snapshot_id": str(snapshot)}
        )
        assert p.ontology_snapshot_id == str(snapshot)
        with pytest.raises(ValueError):
            LoadCutPhotoPayload.model_validate(
                {"photo_dir": "/x", "manifest_sha256": "a" * 64, "ontology_snapshot_id": "211"}
            )


class TestProgress:
    """A long stage keeps emitting, so the stale-job reaper never takes the load for dead."""

    def test_a_stage_reports_every_few_batches_and_when_it_ends(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(loader, "_PROGRESS_EVERY", 2)
        events: list[tuple[str, dict[str, Any]]] = []
        progress = loader._Progress(lambda e, _m, f, _l: events.append((e, f)), "rows", total=25)
        for _ in range(5):
            progress.batch(5)
        progress.done(rows_inserted=25)
        assert [e for e, _ in events] == [
            "load_cut_photo.progress",
            "load_cut_photo.progress",
            "load_cut_photo.stage_done",
        ]
        assert events[0][1] == {
            "stage": "rows",
            "rows": 10,
            "_progress_current": 10,
            "_progress_total": 25,
        }
        assert events[-1][1]["rows_inserted"] == 25


class TestTheSchema:
    def test_the_model_and_the_migrations_declare_the_same_checks(self) -> None:
        photo, members = _migration(), _migration(_MEMBERS_MIGRATION)
        for name in ("SHORT_NAME_CHECK", "DIGESTS_CHECK"):
            assert getattr(photo, name) == getattr(model, name), name
        for name in ("MEMBER_SETS_CHECK", "NEIGHBOUR_ALONE_CHECK", "DESCRIPTIONS_CHECK"):
            assert getattr(members, name) == getattr(model, name), name
        assert _migration(_KIND_MIGRATION).KIND_CHECK == model.KIND_CHECK

    def test_the_migrations_follow_each_other(self) -> None:
        photo, members = _migration(), _migration(_MEMBERS_MIGRATION)
        assert (photo.revision, photo.down_revision) == ("d4a7c2e9f310", "b71e2396e5c3")
        assert (members.revision, members.down_revision) == ("384568a09847", "f0e6749f94cb")
        kind = _migration(_KIND_MIGRATION)
        assert (kind.revision, kind.down_revision) == ("a6e1e05d3c8b", "384568a09847")

    def test_the_migrations_create_the_columns_the_models_declare(self) -> None:
        sources = {
            CutPhoto: _MIGRATION.read_text(encoding="utf-8")
            + _KIND_MIGRATION.read_text(encoding="utf-8"),
            CutPhotoMember: _MEMBERS_MIGRATION.read_text(encoding="utf-8"),
        }
        for table, source in sources.items():
            for column in table.__table__.columns:
                assert f'"{column.name}"' in source, (table.__tablename__, column.name)


# Against Postgres ---------------------------------------------------------------


def _noop_emit(*_args: object, **_kwargs: object) -> None:
    return None


@pytest.fixture()
def pg_session(postgres_url: str, monkeypatch: pytest.MonkeyPatch):
    """A fresh schema with the graph and the Swiss-Prot release of cut 211 loaded."""
    monkeypatch.setattr(
        loader, "store_manifest", lambda _dir, cut, sha: f"memory://cut-photos/{cut}/{sha}"
    )
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine, future=True) as session:
        snapshot = _snapshot()
        session.add(snapshot)
        release = _release()
        session.add(release)
        session.flush()
        session.add_all(
            SwissProtMembership(release_id=release.id, accession=accession, sequence_md5=md5)
            for accession, md5 in MD5.items()
        )
        session.add_all(
            GOTerm(go_id=go_id, ontology_snapshot_id=snapshot.id) for go_id in KNOWN_TERMS
        )
        session.commit()
        yield session
    Base.metadata.drop_all(engine)
    engine.dispose()


def _load(session: Session, out: Path, sha: str, events: list[str] | None = None) -> dict[str, Any]:
    snapshot_id = session.execute(select(OntologySnapshot.id)).scalar_one()
    payload = {
        "photo_dir": str(out),
        "manifest_sha256": sha,
        "ontology_snapshot_id": str(snapshot_id),
    }

    def emit(event: str, _message: object, fields: dict[str, Any], _level: object) -> None:
        if events is not None:
            events.append(f"{event}:{fields.get('stage', '')}")

    result = LoadCutPhotoOperation().execute(session, payload, emit=emit).result
    session.commit()
    return result


@pytest.mark.integration
def test_a_photo_loads_and_says_what_it_wrote(pg_session: Session, tmp_path: Path) -> None:
    out, sha = build_photo(tmp_path)
    events: list[str] = []
    result = _load(pg_session, out, sha, events)
    stages = [e.split(":", 1)[1] for e in events if e.startswith("load_cut_photo.stage_done")]
    assert stages == ["proteins", "sequences", "unreviewed_sequences", "rows", "members"]

    photo = pg_session.execute(select(CutPhoto)).scalar_one()
    assert photo.short_name == "photo-211"
    assert photo.manifest_uri == f"memory://cut-photos/211/{sha}"
    assert photo.manifest_sha256 == sha and photo.builder_revision == "builder-revision"
    assert photo.uniref_release == "2022_03" and photo.cut_date == CUT_DATE

    proteins = {p.accession: p for p in pg_session.execute(select(Protein)).scalars()}
    assert sorted(proteins) == ["N1", "N2", "P1", "P2", "Q9"]
    assert {p.first_admitted_release for p in proteins.values()} == {CUT}
    assert all(p.sequence_id is None for p in proteins.values())

    hashes = set(pg_session.execute(select(Sequence.sequence_hash)).scalars())
    trembl = {
        s: hashlib.md5(s.encode(), usedforsecurity=False).hexdigest()
        for s in ("MQQLK", "MNN", "MPP")
    }
    assert hashes == {*MD5.values(), *trembl.values()}
    assert result["unreviewed_sequences_added"] == 3

    rows = pg_session.execute(
        select(ProteinGOAnnotation.protein_accession, GOTerm.go_id).join(
            GOTerm, GOTerm.id == ProteinGOAnnotation.go_term_id
        )
    ).all()
    assert sorted(rows) == [
        ("N1", "GO:0000005"),
        ("N2", "GO:0000006"),
        ("P1", "GO:0000001"),
        ("P1", "GO:0000002"),
        ("Q9", "GO:0000003"),
    ]
    assert result["rows_with_unknown_term"] == 1 and result["unknown_terms"] == 1
    # What was written, counted from the rows each INSERT returned: a multi-row
    # INSERT ... ON CONFLICT reports a rowcount of -1 through psycopg, which the
    # first dev load at 211 summed into -133 proteins and -114 sequences.
    assert (result["proteins_added"], result["sequences_added"]) == (5, 2)
    assert (result["rows_inserted"], result["rows_already_present"]) == (5, 0)
    assert result["sequences_already_present"] == 0

    members = {
        m.accession: (m.superset, m.truth, m.curated, m.neighbour, m.cluster, m.sequence_md5)
        for m in pg_session.execute(select(CutPhotoMember)).scalars()
    }
    assert members == {
        "P1": (True, True, False, False, "C1", MD5["P1"]),
        "P2": (True, False, False, False, "C2", MD5["P2"]),
        "Q9": (False, False, True, False, None, trembl["MQQLK"]),
        "N1": (False, False, False, True, "C1", trembl["MNN"]),
        "N2": (False, False, False, True, "C1", trembl["MPP"]),
    }
    assert (result["members"], result["neighbours"], result["members_with_paired_sequence"]) == (
        5,
        2,
        5,
    )
    annotation_set = pg_session.get(AnnotationSet, photo.annotation_set_id)
    assert annotation_set is not None and annotation_set.source_version == "211"

    assert photo.channel_table_sha256 == channel_table_digest()
    assert result["channel_table_written"] is True
    assert pg_session.get(EvidenceChannelTable, photo.channel_table_sha256) is not None
    stored_keys = pg_session.execute(select(func.count()).select_from(EvidenceChannel)).scalar_one()
    assert stored_keys == len(channel_rows())


#: Every stored row of a photo with the channel SQL computes for it, reviewed meaning
#: in the photo's paired Swiss-Prot release.
_ROW_CHANNELS = text(
    """
    SELECT a.protein_accession, t.go_id, a.evidence_code,
           evidence_channel_of(p.channel_table_sha256, a.evidence_code, a.db_reference,
                               a.with_from, m.accession IS NOT NULL)
    FROM protein_go_annotation a
    JOIN go_term t ON t.id = a.go_term_id
    JOIN cut_photo p ON p.annotation_set_id = a.annotation_set_id
    LEFT JOIN swissprot_membership m
        ON m.release_id = p.swissprot_release_id AND m.accession = a.protein_accession
    """
)


@pytest.mark.integration
def test_sql_gives_every_stored_row_the_channel_the_builder_gave_it(
    pg_session: Session, tmp_path: Path
) -> None:
    out, sha = build_photo(tmp_path)
    _load(pg_session, out, sha)
    stored = sorted(tuple(row) for row in pg_session.execute(_ROW_CHANNELS).all())
    built = pq.read_table(out / "rows.parquet", columns=["accession", "go_id", "code", "channel"])
    expected = sorted(
        (r["accession"], r["go_id"], r["code"], r["channel"])
        for r in built.to_pylist()
        if r["go_id"] in KNOWN_TERMS
    )
    assert stored == expected
    assert ("P1", "GO:0000002", "IEA", "reviewed_field_electronic") in stored


@pytest.mark.integration
def test_a_stored_channel_table_other_than_the_codes_refuses_before_writing(
    pg_session: Session, tmp_path: Path
) -> None:
    out, sha = build_photo(tmp_path)
    assert loader.declare_channel_table(pg_session, None) is True
    pg_session.execute(
        text(
            "UPDATE evidence_channel SET channel = 'truth' "
            "WHERE evidence_code = 'IEA' AND reference_class = 'GO_REF:0000107'"
        )
    )
    pg_session.commit()
    with pytest.raises(PhotoRefused, match="keys or channels"):
        _load(pg_session, out, sha)
    pg_session.rollback()
    for table in (CutPhoto, ProteinGOAnnotation, Protein):
        assert pg_session.execute(select(func.count()).select_from(table)).scalar_one() == 0


@pytest.mark.integration
@pytest.mark.integration
def test_a_window_starting_at_the_photo_scores_its_superset(
    pg_session: Session, tmp_path: Path
) -> None:
    # The window's start is a corpus; the photo that loaded it names it, and its
    # superset members are the targets a window over ``superset_at_t0`` keeps.
    out, sha = build_photo(tmp_path)
    _load(pg_session, out, sha)
    photo = pg_session.execute(select(CutPhoto)).scalar_one()
    found = superset_at_start(pg_session, photo.annotation_set_id)
    superset = set(
        pg_session.execute(
            select(CutPhotoMember.accession).where(
                CutPhotoMember.photo_id == photo.id, CutPhotoMember.superset
            )
        ).scalars()
    )
    assert found.accessions == superset and len(superset) == 2
    assert (found.cut_release, found.photo_id) == (CUT, photo.id)
    with pytest.raises(ValueError, match="no cut photo has annotation set"):
        superset_at_start(pg_session, uuid.uuid4())


def test_the_same_manifest_again_is_a_no_op(pg_session: Session, tmp_path: Path) -> None:
    out, sha = build_photo(tmp_path)
    first = _load(pg_session, out, sha)
    again = _load(pg_session, out, sha)
    assert again == {"photo_id": first["photo_id"], "skipped": True}


@pytest.mark.integration
def test_another_manifest_for_a_cut_with_a_photo_is_refused(
    pg_session: Session, tmp_path: Path
) -> None:
    out, sha = build_photo(tmp_path / "first")
    _load(pg_session, out, sha)
    other, other_sha = build_photo(
        tmp_path / "second", [*GAF, _gaf("P2", "GO:0000004", "IDA", "PMID:2")]
    )
    with pytest.raises(PhotoRefused, match="already has a photo"):
        _load(pg_session, other, other_sha)


@pytest.mark.integration
def test_the_earliest_admission_is_kept(pg_session: Session, tmp_path: Path) -> None:
    pg_session.add_all(
        [
            Protein(accession="P1", canonical_accession="P1", first_admitted_release=205),
            Protein(accession="P2", canonical_accession="P2", first_admitted_release=220),
        ]
    )
    pg_session.commit()
    out, sha = build_photo(tmp_path)
    result = _load(pg_session, out, sha)
    admitted = dict(
        pg_session.execute(select(Protein.accession, Protein.first_admitted_release)).all()
    )
    assert admitted == {"P1": 205, "P2": CUT, "Q9": CUT, "N1": CUT, "N2": CUT}
    assert result["proteins_added"] == 3


@pytest.mark.integration
def test_an_annotation_set_of_the_cut_on_another_graph_is_refused(
    pg_session: Session, tmp_path: Path
) -> None:
    other = _snapshot("releases/2022-05-01")
    pg_session.add(other)
    pg_session.flush()
    pg_session.add(AnnotationSet(source="goa", source_version="211", ontology_snapshot_id=other.id))
    pg_session.commit()
    out, sha = build_photo(tmp_path)
    payload = {
        "photo_dir": str(out),
        "manifest_sha256": sha,
        "ontology_snapshot_id": str(
            pg_session.execute(
                select(OntologySnapshot.id).where(OntologySnapshot.obo_version == GRAPH)
            ).scalar_one()
        ),
    }
    with pytest.raises(PhotoRefused, match="is on snapshot"):
        LoadCutPhotoOperation().execute(pg_session, payload, emit=_noop_emit)


@pytest.mark.integration
def test_a_truth_only_photo_loads_with_its_kind(pg_session: Session, tmp_path: Path) -> None:
    out, sha = build_photo(tmp_path, truth_only=True)
    events: list[str] = []
    result = _load(pg_session, out, sha, events)
    stages = [e.split(":", 1)[1] for e in events if e.startswith("load_cut_photo.stage_done")]
    assert stages == ["proteins", "sequences", "rows", "members"]
    photo = pg_session.execute(select(CutPhoto)).scalar_one()
    assert (photo.kind, photo.uniref_release, photo.uniref_clusters_sha256) == (
        "truth_only",
        None,
        None,
    )
    assert "truth-only photo" in photo.description
    members = pg_session.execute(select(CutPhotoMember)).scalars().all()
    assert sorted(m.accession for m in members) == ["P1", "P2", "Q9"]
    assert not any(m.neighbour or m.cluster for m in members)
    assert result["unreviewed_sequences_added"] == 0
