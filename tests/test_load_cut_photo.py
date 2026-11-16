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
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.core.cut_photo.build import (
    manifest_pass,
    members_pass,
    rows_pass,
    sequences_pass,
)
from protea.core.cut_photo.channels import PROFILE_CHANNELS
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
    CutPhotoCluster,
    CutPhotoClusterProfile,
)
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.annotation.protein_go_annotation import (
    ProteinGOAnnotation,
)
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.protein.swissprot_release import SwissProtRelease
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
#: The GO ids the test snapshot knows; GO:0000099 is the one it does not.
KNOWN_TERMS = [f"GO:000000{i}" for i in range(1, 8)]
SLUG = re.compile(r"^[a-z0-9][a-z0-9-]*$")

_VERSIONS = Path(__file__).resolve().parents[1] / "alembic" / "versions"
_MIGRATION = _VERSIONS / "d4a7c2e9f310_cut_photo_tables.py"


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


def build_photo(tmp_path: Path, gaf: list[str] = GAF) -> tuple[Path, str]:
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
    members_pass(gaf, superset_table=measured / "superset.tsv", source=goa, source_file=GOA_FILE)
    rows_pass(
        gaf,
        members_path=out / "members.parquet",
        neighbours_path=measured / "neighbours.parquet",
        source=goa,
        source_file=GOA_FILE,
    )
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


def _migration() -> ModuleType:
    """Loaded by path: ``alembic/`` is not a package and the library shadows the name."""
    spec = importlib.util.spec_from_file_location("_d4a7c2e9f310", _MIGRATION)
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

    def test_an_output_changed_after_the_build_is_refused(self, tmp_path: Path) -> None:
        out, sha = build_photo(tmp_path)
        pq.write_table(pa.table({"accession": ["P1"], "cluster": ["C9"]}), out / "clusters.parquet")
        with pytest.raises(PhotoRefused, match="clusters.parquet hashes to"):
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
        with pytest.raises(PhotoRefused, match="newer than the release the GAF declares"):
            check_sources(manifest, _snapshot(), _release())

    def test_a_newer_loaded_graph_the_gaf_allows_is_refused(self, tmp_path: Path) -> None:
        loaded = ["releases/2022-07-01", "releases/2022-08-01", "releases/2022-10-01"]
        with pytest.raises(PhotoRefused, match="2022-08-01 is loaded"):
            check_sources(self._manifest(tmp_path), _snapshot(), _release(), loaded)

    def test_older_and_later_graphs_do_not_change_the_choice(self, tmp_path: Path) -> None:
        loaded = ["releases/2021-10-26", GRAPH, "releases/2025-07-22", "go-basic"]
        sources = check_sources(self._manifest(tmp_path), _snapshot(), _release(), loaded)
        assert sources.cut == CUT

    def test_a_gaf_that_declares_no_go_release_is_refused(self, tmp_path: Path) -> None:
        manifest = self._manifest(tmp_path)
        manifest["sources"]["goa"]["declared_go_release"] = ""
        with pytest.raises(PhotoRefused, match="declares no GO release"):
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
    def test_both_short_names_are_slugs_naming_the_cut_and_the_clustering(
        self, tmp_path: Path
    ) -> None:
        out, sha = build_photo(tmp_path)
        manifest = read_manifest(out, sha)
        sources = check_sources(manifest, _snapshot(), _release())
        names = photo_descriptors(sources, manifest["counts"], "2026-11-16")
        assert names["short_name"] == "photo-211"
        assert names["profile_short_name"] == "profiles-211-uniref-2022-03"
        assert all(SLUG.match(names[k]) for k in ("short_name", "profile_short_name"))
        assert "Swiss-Prot 2022_03" in names["description"]
        assert "2022-07-01" in names["description"]
        assert "UniRef50 2022_03" in names["profile_description"]

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
    def test_the_model_and_the_migration_declare_the_same_checks(self) -> None:
        migration = _migration()
        for name in (
            "PROFILE_CHANNEL_CHECK",
            "SHORT_NAME_CHECK",
            "PROFILE_SHORT_NAME_CHECK",
            "DESCRIPTIONS_CHECK",
            "DIGESTS_CHECK",
        ):
            assert getattr(migration, name) == getattr(model, name), name

    def test_the_profile_check_admits_exactly_the_builders_profile_channels(self) -> None:
        listed = set(re.findall(r"'([a-z_]+)'", model.PROFILE_CHANNEL_CHECK))
        assert listed == set(PROFILE_CHANNELS)

    def test_the_migration_follows_the_window_roles(self) -> None:
        migration = _migration()
        assert migration.down_revision == "b71e2396e5c3"
        assert migration.revision == "d4a7c2e9f310"

    def test_the_migration_creates_the_columns_the_model_declares(self) -> None:
        source = _MIGRATION.read_text(encoding="utf-8")
        for table in (CutPhoto, CutPhotoCluster, CutPhotoClusterProfile):
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
        session.add(_release())
        session.flush()
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
    assert stages == ["proteins", "sequences", "rows", "clusters", "profiles"]

    photo = pg_session.execute(select(CutPhoto)).scalar_one()
    assert (photo.short_name, photo.profile_short_name) == (
        "photo-211",
        "profiles-211-uniref-2022-03",
    )
    assert photo.manifest_uri == f"memory://cut-photos/211/{sha}"
    assert photo.manifest_sha256 == sha and photo.builder_revision == "builder-revision"
    assert photo.uniref_release == "2022_03" and photo.cut_date == CUT_DATE

    proteins = {p.accession: p for p in pg_session.execute(select(Protein)).scalars()}
    assert sorted(proteins) == ["P1", "P2", "Q9"]
    assert {p.first_admitted_release for p in proteins.values()} == {CUT}
    assert all(p.sequence_id is None for p in proteins.values())

    hashes = set(pg_session.execute(select(Sequence.sequence_hash)).scalars())
    assert hashes == set(MD5.values())

    rows = pg_session.execute(
        select(ProteinGOAnnotation.protein_accession, GOTerm.go_id).join(
            GOTerm, GOTerm.id == ProteinGOAnnotation.go_term_id
        )
    ).all()
    assert sorted(rows) == [("P1", "GO:0000001"), ("P1", "GO:0000002"), ("Q9", "GO:0000003")]
    assert result["rows_with_unknown_term"] == 1 and result["unknown_terms"] == 1

    clusters = dict(
        pg_session.execute(select(CutPhotoCluster.accession, CutPhotoCluster.cluster)).all()
    )
    assert clusters == {"P1": "C1", "P2": "C2"}
    profile = {
        (r.cluster, r.go_id, r.channel, r.negated): r.members
        for r in pg_session.execute(select(CutPhotoClusterProfile)).scalars()
    }
    assert profile == {
        ("C1", "GO:0000005", "phylogenetic", False): 1,
        ("C1", "GO:0000006", "orthology_electronic", False): 1,
    }
    annotation_set = pg_session.get(AnnotationSet, photo.annotation_set_id)
    assert annotation_set is not None and annotation_set.source_version == "211"


@pytest.mark.integration
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
    _load(pg_session, out, sha)
    admitted = dict(
        pg_session.execute(select(Protein.accession, Protein.first_admitted_release)).all()
    )
    assert admitted == {"P1": 205, "P2": CUT, "Q9": CUT}


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
