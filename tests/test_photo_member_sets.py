"""Sets of a photo's members chosen by admission flag, and the migration that admits them.

Without a database: the admission route and the flag rules (their refusals, their
truth tables, the breadth levels they write), the hash order, the payload, the routing,
the sets gate and what the migration makes and drops.

With ``--with-postgres``: on a photo whose members cover every combination of flags,
with members that have no sequence and one whose md5 names no sequence row, the dry
run counts and names what the rule chooses and writes nothing; a declaration writes the
entries a reference reading of the photo gives, its artifact is the population's sorted
list, and it issues the same statements for a photo ten times larger; a sample is the
hash order's first members; duplicates and the impossible are refused; the checks
refuse an undeclared row of either kind; and the revision upgrades and downgrades.
"""

from __future__ import annotations

import hashlib
import importlib.util
import uuid
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import Any, get_args

import pytest
import sqlalchemy as sa
from pydantic import ValidationError
from sqlalchemy import create_engine, event, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

import protea.infrastructure.orm.models  # noqa: F401  (registers every table)
from protea.core import queues
from protea.core.cut_photo.members import (
    ADMISSION_FLAGS,
    ADMISSION_ROUTES,
    BREADTH_LEVELS,
    MEMBER_FLAG_COMBINATIONS,
    FlagRule,
    admission_route,
    breadth_level_of,
)
from protea.core.operation_catalog import build_operation_registry
from protea.core.operations import declare_photo_member_set as operation
from protea.core.operations.declare_photo_member_set import (
    AdmissionFlag,
    DeclarePhotoMemberSetOperation,
    DeclarePhotoMemberSetPayload,
    PhotoMemberSetRefused,
    photo_members_key_for,
)
from protea.core.query_population import population_of
from protea.core.sampling import hash_order_key, hash_ordered
from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto
from protea.infrastructure.orm.models.query import query_set as model
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from tests.helpers.cut_photos import Member, make_job, make_photo, member, sequence_ids
from tests.helpers.windows import make_window
from tests.test_method_tables import alembic_config, fresh_database  # noqa: F401

_REPO = Path(__file__).resolve().parents[1]
_MIGRATION = (
    _REPO / "alembic/versions/a0cc2a412ec0_a_query_set_can_be_photo_members_or_a_window_sample.py"
)
_spec = importlib.util.spec_from_file_location("migration_query_set_kinds", _MIGRATION)
assert _spec and _spec.loader
migration = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(migration)

NAME = "declare_photo_member_set"
PHOTO = "b98ba58b-fbb3-43cd-a7c6-412a657b826f"


def _flags(*on: str) -> dict[str, bool]:
    return {flag: flag in on for flag in ADMISSION_FLAGS}


# The admission route and the flag rules ---------------------------------------------


class TestTheAdmissionRoute:
    @pytest.mark.parametrize(
        ("on", "route"),
        [
            (("truth",), "experimental"),
            (("truth", "superset", "curated"), "experimental"),
            (("superset",), "reviewed"),
            (("superset", "curated"), "reviewed"),
            (("curated",), "curated_inference"),
            (("neighbour",), "uniref_neighbour"),
        ],
    )
    def test_it_is_the_first_flag_in_the_agreed_order(
        self, on: tuple[str, ...], route: str
    ) -> None:
        assert admission_route(_flags(*on)) == route

    def test_every_member_has_exactly_one(self) -> None:
        routes = Counter(
            admission_route(dict(zip(ADMISSION_FLAGS, combination, strict=True)))
            for combination in MEMBER_FLAG_COMBINATIONS
        )
        assert sum(routes.values()) == len(MEMBER_FLAG_COMBINATIONS) == 8
        assert set(routes) == set(ADMISSION_ROUTES.values())

    def test_a_member_with_no_flag_or_missing_one_is_refused(self) -> None:
        with pytest.raises(ValueError, match="no route"):
            admission_route(_flags())
        with pytest.raises(ValueError, match="missing"):
            admission_route({"truth": True})


class TestTheFlagRule:
    def test_the_flags_keep_the_agreed_order_whatever_the_payload_says(self) -> None:
        rule = FlagRule(any_of=("superset", "truth"))
        assert rule.any_of == ("truth", "superset")
        assert rule == FlagRule(any_of=("truth", "superset"))

    @pytest.mark.parametrize(
        ("kwargs", "complaint"),
        [
            ({"any_of": ("reviewed",)}, "admission flags"),
            ({"all_of": ("truth", "truth")}, "twice"),
            ({"any_of": ("truth",), "none_of": ("truth",)}, "also asks for"),
            ({"all_of": ("truth", "neighbour")}, "no photo member can satisfy"),
            ({"all_of": ("curated",), "none_of": ("truth", "superset", "curated")}, "also asks"),
        ],
    )
    def test_what_it_refuses(self, kwargs: dict[str, Any], complaint: str) -> None:
        with pytest.raises(ValueError, match=complaint):
            FlagRule(**kwargs)

    def test_its_truth_table_is_the_three_lists(self) -> None:
        rule = FlagRule(any_of=("truth", "curated"), all_of=("superset",), none_of=("neighbour",))
        for combination in MEMBER_FLAG_COMBINATIONS:
            flags = dict(zip(ADMISSION_FLAGS, combination, strict=True))
            expected = (flags["truth"] or flags["curated"]) and flags["superset"]
            assert rule.admits(flags) is bool(expected and not flags["neighbour"])

    def test_the_breadth_levels_are_cumulative_and_end_at_the_photo(self) -> None:
        levels = [rule.admitted_combinations() for rule in BREADTH_LEVELS.values()]
        assert all(a < b for a, b in zip(levels, levels[1:], strict=False))
        assert BREADTH_LEVELS["every-member"].admits_every_member
        assert FlagRule(any_of=ADMISSION_FLAGS).admits_every_member

    def test_a_level_is_named_by_what_it_chooses_not_by_its_spelling(self) -> None:
        assert breadth_level_of(FlagRule(any_of=("truth",))) == "experimental"
        assert breadth_level_of(FlagRule(all_of=("truth",))) == "experimental"
        assert breadth_level_of(FlagRule(none_of=("neighbour",))) == "plus-curated-inference"
        assert breadth_level_of(FlagRule(all_of=("truth", "superset"))) is None

    def test_it_reads_as_a_phrase(self) -> None:
        assert FlagRule().in_words() == "every member"
        rule = FlagRule(all_of=("superset",), none_of=("truth",))
        assert rule.in_words() == "the members entered by superset, not entered by truth"
        assert rule.declared() == {"any_of": [], "all_of": ["superset"], "none_of": ["truth"]}

    def test_the_payload_names_the_same_flags(self) -> None:
        assert get_args(AdmissionFlag) == ADMISSION_FLAGS


class TestTheHashOrder:
    def test_the_key_is_the_sha256_of_the_joined_text(self) -> None:
        expected = hashlib.sha256(b"ladder-toy-heads-1|photo|P12345").hexdigest()
        assert hash_order_key("ladder-toy-heads-1", "photo", "P12345") == expected

    def test_the_order_is_ascending_and_distinct(self) -> None:
        ordered = hash_ordered(["B", "A", "B", "C"], "seed", "scope")
        assert sorted(ordered) == ["A", "B", "C"]
        keys = [hash_order_key("seed", "scope", a) for a in ordered]
        assert keys == sorted(keys)

    def test_a_scope_or_a_seed_moves_the_draw(self) -> None:
        accessions = [f"P{i:05d}" for i in range(200)]
        first = hash_ordered(accessions, "seed", "window-a")[:20]
        assert first != hash_ordered(accessions, "seed", "window-b")[:20]
        assert first != hash_ordered(accessions, "other", "window-a")[:20]
        assert first == hash_ordered(reversed(accessions), "seed", "window-a")[:20]


# The payload, the routing and the schema ------------------------------------------


def _payload(**over: Any) -> dict[str, Any]:
    return {"cut_photo_id": PHOTO, "any_of": ["truth"], "output_name": "donors-211", **over}


class TestThePayload:
    def test_a_level_and_a_sample(self) -> None:
        p = DeclarePhotoMemberSetPayload.model_validate(
            _payload(any_of=["superset", "truth"], sample={"size": 20000, "seed": "s"})
        )
        assert p.photo_id == uuid.UUID(PHOTO)
        assert p.rule == FlagRule(any_of=("truth", "superset"))
        assert p.sample is not None and (p.sample.size, p.sample.seed) == (20000, "s")
        assert p.dry_run is False

    @pytest.mark.parametrize(
        "over",
        [
            {"any_of": ["truth"], "none_of": ["truth"]},
            {"all_of": ["truth", "neighbour"]},
            {"any_of": []},
            {"any_of": ["truth", "superset", "curated", "neighbour"]},
            {"sample": {"size": 0, "seed": "s"}},
            {"sample": {"size": 10, "seed": ""}},
            {"sample": {"size": 10}},
            {"output_name": "a/b"},
            {"output_name": "  "},
            {"cut_photo_id": "photo-211"},
            {"route": "experimental"},
        ],
    )
    def test_what_it_refuses(self, over: dict[str, Any]) -> None:
        with pytest.raises(ValidationError):
            DeclarePhotoMemberSetPayload.model_validate(_payload(**over))

    def test_every_member_is_a_sample_or_nothing(self) -> None:
        p = DeclarePhotoMemberSetPayload.model_validate(
            _payload(any_of=[], sample={"size": 5, "seed": "s"})
        )
        assert p.rule.admits_every_member

    def test_the_summary(self) -> None:
        op = DeclarePhotoMemberSetOperation()
        assert (
            op.summarize_payload(_payload(dry_run=True))
            == "members of photo b98ba58b by flag (dry run)"
        )
        assert op.summarize_payload({}) == ""


def test_it_runs_as_a_job_of_the_general_queue() -> None:
    spec = build_operation_registry().spec(NAME)
    assert (spec.domain, spec.kind, spec.queue, spec.purity) == (
        "universe",
        "job",
        queues.JOBS,
        "database",
    )
    assert spec.publishes_to == () and spec.family is None


def test_the_artifact_has_one_spelling() -> None:
    photo = uuid.uuid4()
    key = photo_members_key_for(photo, "e" * 64)
    assert key == f"query-sets/photo-members/{photo}/{'e' * 64}/accessions.txt"


def test_the_query_set_still_passes_the_sets_gate() -> None:
    spec = importlib.util.spec_from_file_location("_check_sets", _REPO / "scripts/check_sets.py")
    assert spec and spec.loader
    guard = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard)
    verdict = guard._audit(Base.metadata.tables["query_set"])
    assert all(ok for _, ok in verdict.values())
    assert "cut_photo_id" in verdict["declarado"][0]


class _Recorder:
    """Stands in for ``alembic.op`` and keeps the name of everything each call touches."""

    def __init__(self) -> None:
        self.made: list[tuple[str, str]] = []
        self.dropped: list[tuple[str, str]] = []
        self.checks: dict[str, str] = {}
        self.index_where: dict[str, str] = {}

    def add_column(self, table: str, column: sa.Column) -> None:
        self.made.append(("column", column.name))

    def drop_column(self, table: str, name: str) -> None:
        self.dropped.append(("column", name))

    def create_foreign_key(self, name: str, *_a: Any, **_kw: Any) -> None:
        self.made.append(("foreignkey", name))

    def create_check_constraint(self, name: str, _table: str, sql: str) -> None:
        self.made.append(("check", name))
        self.checks[name] = sql

    def create_index(self, name: str, _table: str, _cols: list[str], **kw: Any) -> None:
        self.made.append(("index", name))
        self.index_where[name] = str(kw["postgresql_where"])

    def drop_constraint(self, name: str, _table: str, type_: str) -> None:
        self.dropped.append((type_, name))

    def drop_index(self, name: str, **_kw: Any) -> None:
        self.dropped.append(("index", name))


class TestTheMigration:
    def test_it_follows_the_head_it_was_written_on(self) -> None:
        assert migration.revision == "a0cc2a412ec0"
        assert migration.down_revision == "f2e0fc2717f7"

    def test_it_writes_the_models_vocabulary_and_checks(self) -> None:
        assert migration.SOURCES == model.QUERY_SET_SOURCES
        for name in (
            "PHOTO_MEMBERS_DECLARED_CHECK",
            "WINDOW_SAMPLE_DECLARED_CHECK",
            "PHOTO_ON_MEMBER_SETS_CHECK",
            "PHOTO_MEMBERS_PREDICATE",
        ):
            assert getattr(migration, name) == getattr(model, name), name

    def test_the_downgrade_drops_what_the_upgrade_made(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        recorder = _Recorder()
        monkeypatch.setattr(migration, "op", recorder)
        migration.upgrade()
        made = set(recorder.made)
        assert recorder.checks["ck_query_set_source_kind"] == (
            "source_kind IN (" + ", ".join(f"'{v}'" for v in model.QUERY_SET_SOURCES) + ")"
        )
        assert recorder.index_where == {
            "uq_query_set_photo_members_content": model.PHOTO_MEMBERS_PREDICATE
        }
        migration.downgrade()
        # The vocabulary check is dropped and made again with the earlier values.
        assert set(recorder.dropped) == made
        assert "photo_members" not in recorder.checks["ck_query_set_source_kind"]

    def test_the_model_declares_the_same_names(self) -> None:
        table = Base.metadata.tables["query_set"]
        checks = {c.name for c in table.constraints if isinstance(c, sa.CheckConstraint)}
        assert {
            "ck_query_set_photo_members_are_declared",
            "ck_query_set_window_sample_is_declared",
            "ck_query_set_photo_on_member_sets",
        } <= checks
        index = next(i for i in table.indexes if i.name == "uq_query_set_photo_members_content")
        assert index.unique and [c.name for c in index.columns] == [
            "cut_photo_id",
            "content_sha256",
        ]
        fk = next(fk for fk in table.foreign_keys if fk.parent.name == "cut_photo_id")
        assert (fk.target_fullname, fk.ondelete, fk.name) == (
            "cut_photo.id",
            "RESTRICT",
            "fk_query_set_cut_photo_id",
        )


# On Postgres ----------------------------------------------------------------------


@pytest.fixture()
def pg(postgres_url: str) -> Iterator[Session]:
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    Base.metadata.drop_all(engine)
    engine.dispose()


def _members(copies: int = 1) -> list[Member]:
    """Every combination of flags, ``copies`` members each, and three without a sequence."""
    letters = {"truth": "t", "superset": "s", "curated": "c", "neighbour": "n"}
    found = []
    for number, combination in enumerate(MEMBER_FLAG_COMBINATIONS):
        flags = "".join(
            letters[f] for f, on in zip(ADMISSION_FLAGS, combination, strict=True) if on
        )
        for copy in range(copies):
            accession = f"A{number}{copy:04d}"
            found.append(member(accession, flags, residues="MK" + "A" * (number + copy)))
    found.append(member("Z0001", "t", residues=None))
    found.append(member("Z0002", "ts", residues=None, orphan_md5=True))
    found.append(member("Z0003", "n", residues=None))
    return found


@pytest.fixture()
def stored(monkeypatch: pytest.MonkeyPatch) -> dict[str, bytes]:
    kept: dict[str, bytes] = {}

    def put(key: str, blob: bytes) -> str:
        kept[key] = blob
        return f"memory://{key}"

    monkeypatch.setattr(operation, "publish", put)
    return kept


def _run(session: Session, photo: CutPhoto, **over: Any) -> dict[str, Any]:
    payload = {
        "cut_photo_id": str(photo.id),
        "output_name": "donors",
        "_job_id": str(make_job(session, NAME)),
        **over,
    }
    result = DeclarePhotoMemberSetOperation().execute(session, payload, emit=lambda *a, **k: None)
    session.flush()
    return result.result


def _reference(members: list[Member], rule: FlagRule) -> list[str]:
    """What the rule chooses, read member by member in Python: those with a sequence row."""
    return sorted(
        m.accession
        for m in members
        if m.residues is not None and rule.admits({f: f in m.flags for f in ADMISSION_FLAGS})
    )


def _entries(session: Session, query_set_id: str) -> dict[str, int]:
    rows = session.execute(
        select(QuerySetEntry.accession, QuerySetEntry.sequence_id).where(
            QuerySetEntry.query_set_id == uuid.UUID(query_set_id)
        )
    ).all()
    return {accession: sequence_id for accession, sequence_id in rows}


@pytest.mark.integration
def test_a_dry_run_counts_names_and_writes_nothing(pg: Session, stored: dict[str, bytes]) -> None:
    members = _members()
    photo = make_photo(pg, members)
    for label, rule in BREADTH_LEVELS.items():
        if label == "every-member":
            continue
        result = _run(pg, photo, dry_run=True, **{k: list(v) for k, v in rule.declared().items()})
        chosen = _reference(members, rule)
        assert result["entries"] == result["members_with_sequence"] == len(chosen)
        assert result["content_sha256"] == population_of(chosen)["sha256"]
        assert result["breadth_level"] == label
        assert result["already_declared"] is None
        expected_missing = sorted(
            m.accession
            for m in members
            if m.residues is None and rule.admits({f: f in m.flags for f in ADMISSION_FLAGS})
        )
        assert result["without_sequence_accessions"] == expected_missing
    assert pg.execute(select(sa.func.count(QuerySet.id))).scalar_one() == 0
    assert pg.execute(select(sa.func.count(QuerySetEntry.id))).scalar_one() == 0
    assert stored == {}


@pytest.mark.integration
def test_the_set_based_insert_writes_what_a_reference_reading_gives(
    pg: Session, stored: dict[str, bytes]
) -> None:
    members = _members(copies=3)
    photo = make_photo(pg, members)
    rule = FlagRule(any_of=("truth", "superset"))
    result = _run(pg, photo, any_of=["truth", "superset"], output_name="donors-level-2")

    chosen = _reference(members, rule)
    ids = sequence_ids(pg, photo)
    assert _entries(pg, result["query_set_id"]) == {a: ids[a] for a in chosen}
    row = pg.get(QuerySet, uuid.UUID(result["query_set_id"]))
    assert row is not None
    assert (row.source_kind, row.cut_photo_id, row.evaluation_set_id) == (
        "photo_members",
        photo.id,
        None,
    )
    body = stored[photo_members_key_for(photo.id, row.content_sha256)]
    assert body == "".join(f"{a}\n" for a in chosen).encode()
    assert row.content_sha256 == hashlib.sha256(body).hexdigest() == population_of(chosen)["sha256"]
    assert row.name == f"donors-level-2-{row.content_sha256[:12]}"
    assert row.job_id is not None and row.code_revision
    routes = Counter(
        admission_route({f: f in m.flags for f in ADMISSION_FLAGS})
        for m in members
        if m.accession in set(chosen)
    )
    assert result["by_route"] == {
        route: routes.get(route, 0) for route in ADMISSION_ROUTES.values()
    }
    assert row.description is not None and row.description.isascii()
    assert "breadth level experimental-or-reviewed" in row.description
    assert "Z0001" in row.description and "Z0002" in row.description
    assert result["without_sequence"] == 2


def _statements(session: Session, photo: CutPhoto) -> int:
    """The statements one declaration issues, the job row the helper writes first included."""
    seen: list[str] = []

    def count(*_args: Any, **_kw: Any) -> None:
        seen.append("statement")

    engine = session.get_bind()
    event.listen(engine, "before_cursor_execute", count)
    try:
        _run(session, photo, any_of=["truth", "curated"])
    finally:
        event.remove(engine, "before_cursor_execute", count)
    return len(seen)


def _renamed(found: Member, prefix: str) -> Member:
    return Member(prefix + found.accession, found.flags, found.residues, found.orphan_md5)


@pytest.mark.integration
def test_the_statements_do_not_grow_with_the_members(pg: Session, stored: dict[str, bytes]) -> None:
    small_members = _members(copies=1)
    large_members = [_renamed(m, "L") for m in _members(copies=10)]
    small = make_photo(pg, small_members, cut=160)
    large = make_photo(pg, large_members, cut=177)
    assert _statements(pg, small) == _statements(pg, large)
    rule = FlagRule(any_of=("truth", "curated"))
    written = Counter(e.query_set_id for e in pg.execute(select(QuerySetEntry)).scalars())
    assert sorted(written.values()) == [
        len(_reference(small_members, rule)),
        len(_reference(large_members, rule)),
    ]
    assert len(_reference(large_members, rule)) == 10 * len(_reference(small_members, rule))


@pytest.mark.integration
def test_a_sample_is_the_first_members_in_hash_order(pg: Session, stored: dict[str, bytes]) -> None:
    members = _members(copies=6)
    photo = make_photo(pg, members)
    rule = FlagRule(any_of=("truth",))
    chosen = _reference(members, rule)
    sample = {"size": 7, "seed": "ladder-toy-heads-1"}
    result = _run(pg, photo, any_of=["truth"], sample=sample)

    expected = hash_ordered(chosen, "ladder-toy-heads-1", str(photo.id))[:7]
    assert set(_entries(pg, result["query_set_id"])) == set(expected)
    assert result["members_with_sequence"] == len(chosen) and result["entries"] == 7
    again = _run(pg, photo, any_of=["truth"], sample=sample, dry_run=True)
    assert again["content_sha256"] == result["content_sha256"]
    assert again["already_declared"] == result["query_set_id"]
    other = _run(pg, photo, any_of=["truth"], sample={"size": 7, "seed": "another"}, dry_run=True)
    assert other["content_sha256"] != result["content_sha256"]
    # Every member of the photo can be sampled, though it is not a set without a sample.
    every = _run(pg, photo, sample=sample, dry_run=True)
    with_sequence = sorted(m.accession for m in members if m.residues is not None)
    expected_every = hash_ordered(with_sequence, "ladder-toy-heads-1", str(photo.id))[:7]
    assert every["content_sha256"] == population_of(expected_every)["sha256"]


@pytest.mark.integration
def test_what_cannot_be_declared_is_refused_before_writing(
    pg: Session, stored: dict[str, bytes]
) -> None:
    photo = make_photo(pg, _members())
    unsequenced = make_photo(pg, [member("Y0001", "t", residues=None)], cut=160)
    first = _run(pg, photo, any_of=["truth"])
    # The same members under another spelling of the rule are the same set.
    with pytest.raises(PhotoMemberSetRefused, match="already has these members"):
        _run(pg, photo, all_of=["truth"])
    with pytest.raises(PhotoMemberSetRefused, match="every one of them"):
        _run(pg, photo, any_of=["truth"], sample={"size": 10_000, "seed": "s"})
    with pytest.raises(PhotoMemberSetRefused, match="no member with a sequence"):
        _run(pg, unsequenced, any_of=["truth"])
    with pytest.raises(PhotoMemberSetRefused, match="no cut photo"):
        _run(pg, photo, any_of=["truth"], cut_photo_id=str(uuid.uuid4()))
    assert pg.execute(select(sa.func.count(QuerySet.id))).scalar_one() == 1
    assert list(stored) == [photo_members_key_for(photo.id, first["content_sha256"])]


def _row(session: Session, **fields: Any) -> QuerySet:
    declared = {
        "name": "a set",
        "description": "a set",
        "artifact_uri": "memory://set",
        "content_sha256": "a" * 64,
        "job_id": make_job(session, NAME),
        **fields,
    }
    row = QuerySet(**declared)
    session.add(row)
    return row


@pytest.mark.integration
@pytest.mark.parametrize(
    ("fields", "constraint"),
    [
        ({"source_kind": "photo_members"}, "ck_query_set_photo_members_are_declared"),
        (
            {"source_kind": "photo_members", "cut_photo_id": "photo", "artifact_uri": None},
            "ck_query_set_photo_members_are_declared",
        ),
        (
            {
                "source_kind": "photo_members",
                "cut_photo_id": "photo",
                "evaluation_set_id": "window",
            },
            "ck_query_set_photo_members_are_declared",
        ),
        ({"source_kind": "window_sample"}, "ck_query_set_window_sample_is_declared"),
        (
            {"source_kind": "window_sample", "evaluation_set_id": "window", "job_id": None},
            "ck_query_set_window_sample_is_declared",
        ),
        (
            {"source_kind": "corpus_population", "cut_photo_id": "photo"},
            "ck_query_set_photo_on_member_sets",
        ),
    ],
)
def test_an_undeclared_row_of_either_kind_is_refused(
    pg: Session, fields: dict[str, Any], constraint: str
) -> None:
    photo = make_photo(pg, _members())
    window = make_window(pg)
    resolved = {
        key: (photo.id if value == "photo" else window if value == "window" else value)
        for key, value in fields.items()
    }
    _row(pg, **resolved)
    with pytest.raises(IntegrityError, match=constraint):
        pg.flush()


@pytest.mark.integration
def test_one_photo_and_one_set_of_bytes_are_one_set(pg: Session) -> None:
    photo = make_photo(pg, _members())
    _row(pg, source_kind="photo_members", cut_photo_id=photo.id)
    pg.flush()
    _row(pg, source_kind="photo_members", cut_photo_id=photo.id)
    with pytest.raises(IntegrityError, match="uq_query_set_photo_members_content"):
        pg.flush()


@pytest.mark.integration
def test_the_revision_upgrades_and_downgrades_against_postgres(
    alembic_config: Any,  # noqa: F811
    fresh_database: str,  # noqa: F811
) -> None:
    from alembic import command

    engine = create_engine(fresh_database)
    names = {
        "ck_query_set_photo_members_are_declared",
        "ck_query_set_window_sample_is_declared",
        "ck_query_set_photo_on_member_sets",
    }
    try:
        command.upgrade(alembic_config, migration.revision)
        inspector = sa.inspect(engine)
        assert "cut_photo_id" in {c["name"] for c in inspector.get_columns("query_set")}
        assert names <= {c["name"] for c in inspector.get_check_constraints("query_set")}
        index = next(
            i
            for i in inspector.get_indexes("query_set")
            if i["name"] == "uq_query_set_photo_members_content"
        )
        assert index["unique"] and index["dialect_options"]["postgresql_where"]
        command.downgrade(alembic_config, migration.down_revision)
        inspector = sa.inspect(engine)
        assert "cut_photo_id" not in {c["name"] for c in inspector.get_columns("query_set")}
        assert not names & {c["name"] for c in inspector.get_check_constraints("query_set")}
        command.upgrade(alembic_config, migration.revision)
        with engine.begin() as conn:
            _a_window_sample(conn)
        with pytest.raises(sa.exc.IntegrityError, match="ck_query_set_source_kind"):
            command.downgrade(alembic_config, migration.down_revision)
    finally:
        engine.dispose()


def _a_window_sample(conn: sa.Connection) -> None:
    """A declared window sample, written in SQL so it does not depend on the models' head."""
    snapshot, corpus, window, job = (uuid.uuid4() for _ in range(4))
    conn.execute(
        sa.text(
            "INSERT INTO ontology_snapshot (id, obo_url, obo_version)"
            " VALUES (:id, 'file:///none', 'query-set-kinds')"
        ),
        {"id": snapshot},
    )
    conn.execute(
        sa.text(
            "INSERT INTO annotation_set (id, source, ontology_snapshot_id, meta)"
            " VALUES (:id, 'goa', :snapshot, CAST('{}' AS jsonb))"
        ),
        {"id": corpus, "snapshot": snapshot},
    )
    conn.execute(
        sa.text(
            "INSERT INTO evaluation_set (id, old_annotation_set_id, new_annotation_set_id,"
            " pivot_snapshot_id, old_native_snapshot_id, new_native_snapshot_id, stats)"
            " VALUES (:id, :corpus, :corpus, :s, :s, :s, CAST('{}' AS jsonb))"
        ),
        {"id": window, "corpus": corpus, "s": snapshot},
    )
    conn.execute(
        sa.text(
            "INSERT INTO job (id, operation, queue_name, status, payload, meta)"
            " VALUES (:id, 'declare_window_sample', 'protea.jobs', 'SUCCEEDED',"
            " CAST('{}' AS jsonb), CAST('{}' AS jsonb))"
        ),
        {"id": job},
    )
    conn.execute(
        sa.text(
            "INSERT INTO query_set (id, name, description, source_kind, evaluation_set_id,"
            " artifact_uri, content_sha256, job_id)"
            " VALUES (:id, 'sample', 'a sample', 'window_sample', :window, 'memory://s',"
            " :sha, :job)"
        ),
        {"id": uuid.uuid4(), "window": window, "sha": "b" * 64, "job": job},
    )
