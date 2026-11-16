"""ADR-D59, decision 2: the evidence channel table, as the code declares it and as SQL reads it.

Without a database: the enumerated partition is the one ``channel_of`` decides, the
digest every photo records names it (pinned, so a change to the partition that
leaves the digest alone fails here), the descriptors are ADR-D58 slugs, the model
and the migration declare the same SQL, and the migration seeds the partition its
digest names. Against Postgres (``--with-postgres``, CI): the two SQL functions
give the Python's answer on every key and on the references a GAF writes, the table
is written once, and a stored table that differs from the code's is refused.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import re
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.core.cut_photo.build import channel_table_digest
from protea.core.cut_photo.channels import (
    ANY_REFERENCE,
    CHANNELS,
    CURATED_CODES,
    ELECTRONIC_REFERENCES,
    GOREF_LIST_READ_ON,
    PHYLOGENETIC_CODES,
    UnknownEvidence,
    channel_of,
    channel_rows,
    channel_table_descriptors,
)
from protea.core.cut_photo.formats import (
    AUTOMATIC_CODE,
    EMPTY_GO_REF_BY_WITH_FROM,
    NO_DATA_CODE,
    TRUTH_CODES,
    reference_class,
)
from protea.core.operations.load_cut_photo import (
    PhotoRefused,
    check_stored_channel_table,
    declare_channel_table,
)
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation import evidence_channel as model
from protea.infrastructure.orm.models.annotation.evidence_channel import (
    EvidenceChannel,
    EvidenceChannelEmptyGoRef,
    EvidenceChannelTable,
)

_MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "f0e6749f94cb_evidence_channel_tables.py"
)
_ROW_FIELDS = ("evidence_code", "reference_class", "reviewed", "channel", "reference_title")

#: The partition each channel table digest names, as the sha256 of its rows in
#: ``channel_rows`` order. The digest covers the electronic references and the
#: reading of an empty ``GO_REF``, not the code sets, so a change to a code set
#: fails here until ``channel_table_digest`` covers it too and a new pair is pinned.
PARTITION_OF_DIGEST = {
    "781a0ac1a02a8156a4b873704728529751acf61b98cff45d71c22d6d370155bc": (
        "4623a9377a4b12ed826750ad1488176764ce054b9cc377622dd0a5eb98391959"
    ),
}


def _rows_sha256(rows: list[tuple[object, ...]]) -> str:
    return hashlib.sha256(json.dumps([list(row) for row in rows]).encode()).hexdigest()


def _migration() -> ModuleType:
    """Loaded by path: ``alembic/`` is not a package and the library shadows the name."""
    spec = importlib.util.spec_from_file_location("_f0e6749f94cb", _MIGRATION)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _squash(sql: str) -> str:
    return " ".join(sql.split())


class TestThePartition:
    def test_every_row_has_the_channel_channel_of_gives(self) -> None:
        for row in channel_rows():
            assert channel_of(row.evidence_code, row.reference_class, row.reviewed) == row.channel

    def test_the_rows_are_every_declared_key_once(self) -> None:
        keys = [(r.evidence_code, r.reference_class, r.reviewed) for r in channel_rows()]
        codes = TRUTH_CODES | CURATED_CODES | PHYLOGENETIC_CODES | {NO_DATA_CODE}
        assert len(keys) == len(set(keys)) == 2 * (len(codes) + len(ELECTRONIC_REFERENCES))
        assert {k for k in keys if k[1] == ANY_REFERENCE} == {
            (code, ANY_REFERENCE, reviewed) for code in codes for reviewed in (False, True)
        }
        assert {k for k in keys if k[0] == AUTOMATIC_CODE} == {
            (AUTOMATIC_CODE, ref, reviewed)
            for ref in ELECTRONIC_REFERENCES
            for reviewed in (False, True)
        }

    def test_a_key_the_rows_do_not_list_is_refused(self) -> None:
        with pytest.raises(UnknownEvidence):
            channel_of(AUTOMATIC_CODE, "publication", True)
        with pytest.raises(UnknownEvidence):
            channel_of("XYZ", ANY_REFERENCE, False)

    def test_every_channel_is_one_the_schema_admits(self) -> None:
        listed = set(re.findall(r"'([a-z_]+)'", model.CHANNEL_CHECK))
        assert listed == set(CHANNELS)

    def test_the_digest_names_the_partition(self) -> None:
        rows = [tuple(row) for row in channel_rows()]
        assert PARTITION_OF_DIGEST.get(channel_table_digest()) == _rows_sha256(rows)

    def test_the_descriptors_are_a_slug_and_a_sentence(self) -> None:
        names = channel_table_descriptors(channel_table_digest())
        assert re.fullmatch(r"[a-z0-9][a-z0-9-]*", names["short_name"])
        assert len(names["short_name"]) <= 64 and GOREF_LIST_READ_ON in names["short_name"]
        assert names["description"].endswith(f"{len(channel_rows())} rows.")


class TestTheMigration:
    def test_it_follows_the_photo_tables(self) -> None:
        migration = _migration()
        assert (migration.revision, migration.down_revision) == ("f0e6749f94cb", "d4a7c2e9f310")

    def test_the_model_and_the_migration_declare_the_same_sql(self) -> None:
        migration = _migration()
        for name in (
            "CHANNEL_CHECK",
            "SHORT_NAME_CHECK",
            "TABLE_CHECK",
            "REFERENCE_CLASS_FUNCTION",
            "CHANNEL_FUNCTION",
        ):
            assert _squash(getattr(migration, name)) == _squash(getattr(model, name)), name

    def test_it_seeds_the_partition_its_digest_names(self) -> None:
        migration = _migration()
        assert migration.seed_digest() == migration.SEED_SHA256
        seeded = [tuple(row[k] for k in _ROW_FIELDS) for row in migration.seed_rows()]
        assert {row["table_sha256"] for row in migration.seed_rows()} == {migration.SEED_SHA256}
        assert _rows_sha256(seeded) == PARTITION_OF_DIGEST[migration.SEED_SHA256]
        assert re.fullmatch(r"[a-z0-9][a-z0-9-]*", migration.SEED_SHORT_NAME)

    def test_while_the_code_declares_the_seeded_table_they_are_equal(self) -> None:
        migration = _migration()
        if channel_table_digest() != migration.SEED_SHA256:
            return  # a later table: the seed stays what it was, pinned above
        names = channel_table_descriptors(migration.SEED_SHA256)
        assert (migration.SEED_SHORT_NAME, migration.SEED_DESCRIPTION) == (
            names["short_name"],
            names["description"],
        )
        assert migration.EMPTY_GO_REF_BY_WITH_FROM == EMPTY_GO_REF_BY_WITH_FROM
        assert migration.SEED_GOREF_LIST_READ_ON.isoformat() == GOREF_LIST_READ_ON

    def test_it_creates_the_columns_the_model_declares(self) -> None:
        source = _MIGRATION.read_text(encoding="utf-8")
        for table in (EvidenceChannelTable, EvidenceChannel, EvidenceChannelEmptyGoRef):
            for column in table.__table__.columns:
                assert f'"{column.name}"' in source, (table.__tablename__, column.name)


# Against Postgres ---------------------------------------------------------------


@pytest.fixture()
def pg(postgres_url: str) -> Iterator[Session]:
    """A fresh schema holding the channel table this code declares."""
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine, future=True) as session:
        assert declare_channel_table(session, None) is True
        session.commit()
        yield session
    Base.metadata.drop_all(engine)
    engine.dispose()


#: References as a GAF writes them, with the with/from an empty ``GO_REF`` is read by.
REFERENCES = [
    ("GO_REF:0000043", ""),
    ("PMID:1|GO_REF:0000024", ""),
    ("GO_REF:|GO_REF:0000002", ""),
    ("GO_REF:00000431", ""),
    ("PMID:123", ""),
    ("DOI:10.1000/x|Reactome:R-HSA-1", ""),
    ("Reactome:R-HSA-1", ""),
    ("GO_REF:abc", ""),
    ("", ""),
    (None, None),
    ("GO_REF:", "UniProtKB-SubCell:SL-0039"),
    ("GO_REF:", "UniProtKB-KW:KW-0472"),
    ("GO_REF:", "UniProtKB-SubCell:SL-0039|UniProtKB-SubCell:SL-0086"),
    ("GO_REF:", "|UniProtKB-KW:KW-0472|"),
    ("GO_REF:", "UniProtKB-SubCell"),
    ("GO_REF:", "UniProtKB-SubCell:SL-0039|UniProtKB-KW:KW-0472"),
    ("GO_REF:", "UniProtKB-SubCell:SL-0039|InterPro:IPR000001"),
    ("GO_REF:", ""),
    ("GO_REF:", None),
    ("PMID:1|GO_REF:", "UniProtKB-KW:KW-0472"),
    ("Reactome:R-1|GO_REF:", "UniProtKB-KW:KW-0472"),
]


def _sql_class(session: Session, field: str | None, with_from: str | None) -> str:
    return session.execute(
        text("SELECT evidence_reference_class(:t, :r, :w)"),
        {"t": channel_table_digest(), "r": field, "w": with_from},
    ).scalar_one()


def _sql_channel(
    session: Session, code: str, field: str | None, with_from: str | None, reviewed: bool
) -> str | None:
    return session.execute(
        text("SELECT evidence_channel_of(:t, :c, :r, :w, :v)"),
        {"t": channel_table_digest(), "c": code, "r": field, "w": with_from, "v": reviewed},
    ).scalar_one()


@pytest.mark.integration
@pytest.mark.parametrize(("field", "with_from"), REFERENCES)
def test_the_sql_reference_class_is_the_python_one(
    pg: Session, field: str | None, with_from: str | None
) -> None:
    assert _sql_class(pg, field, with_from) == reference_class(field or "", with_from or "")


@pytest.mark.integration
def test_the_sql_channel_is_the_python_one_on_every_key(pg: Session) -> None:
    for row in channel_rows():
        field = "PMID:1" if row.reference_class == ANY_REFERENCE else row.reference_class
        assert _sql_channel(pg, row.evidence_code, field, "", row.reviewed) == row.channel, row


@pytest.mark.integration
@pytest.mark.parametrize("reviewed", [False, True])
@pytest.mark.parametrize(
    ("code", "field", "with_from"),
    [
        ("IEA", "GO_REF:", "UniProtKB-SubCell:SL-0039"),
        ("IEA", "GO_REF:", "UniProtKB-KW:KW-0472"),
        ("IEA", "GO_REF:", "InterPro:IPR000001"),
        ("IEA", "PMID:1", ""),
        ("IEA", "GO_REF:0000999", ""),
        ("IDA", "GO_REF:0000999", ""),
        ("XYZ", "PMID:1", ""),
    ],
)
def test_a_row_reads_as_the_python_reads_it(
    pg: Session, code: str, field: str, with_from: str, reviewed: bool
) -> None:
    try:
        want: str | None = channel_of(code, reference_class(field, with_from), reviewed)
    except UnknownEvidence:
        want = None
    assert _sql_channel(pg, code, field, with_from, reviewed) == want


@pytest.mark.integration
def test_the_table_is_written_once(pg: Session) -> None:
    assert declare_channel_table(pg, None) is False
    check_stored_channel_table(pg)
    rows = pg.execute(select(func.count()).select_from(EvidenceChannel)).scalar_one()
    assert rows == len(channel_rows())
    version = pg.get(EvidenceChannelTable, channel_table_digest())
    assert version is not None
    assert version.short_name == channel_table_descriptors(version.sha256)["short_name"]


@pytest.mark.integration
@pytest.mark.parametrize(
    ("change", "named"),
    [
        (
            "UPDATE evidence_channel SET channel = 'truth' "
            "WHERE evidence_code = 'IEA' AND reference_class = 'GO_REF:0000002'",
            "its keys or channels",
        ),
        (
            "UPDATE evidence_channel_empty_go_ref SET reads_as = 'GO_REF:0000002'",
            "its reading of an empty GO_REF",
        ),
        (
            "UPDATE evidence_channel_table SET goref_list_read_on = '2020-01-01'",
            "the day GO's list was read",
        ),
    ],
)
def test_a_stored_table_that_differs_is_refused(pg: Session, change: str, named: str) -> None:
    pg.execute(text(change))
    with pytest.raises(PhotoRefused, match=re.escape(named)):
        check_stored_channel_table(pg)
