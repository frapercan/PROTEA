"""A query set says where its population came from, and the schema insists.

THE DEFECT. ``query_set`` was the only one of the six set tables with NO
outgoing foreign key at all, so the population every number is measured over
declared its derivation in free text:

    "Las 40.531 dianas de la ventana ea0f4334 (projected, live_in_both_ends),
    exportadas por el job a9b1c60e con sha256 c4f15a254272..."

Every fact needed to attribute a number, in a sentence. And two of the four
real sets are named almost the same (``dianas-valid-211-227`` and
``...-titular``) while coming from DIFFERENT windows, ``ea94e57e`` and
``ea0f4334``, with 63.734 targets against 40.531. A name is not a declaration.

So the tests here are about what can no longer be WRITTEN, not about what a
docstring asks for. A rule enforced by a CHECK is a rule; a rule written in a
docstring is a hope, and this table is where that difference showed.
"""

from __future__ import annotations

import ast
import pathlib
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from protea.infrastructure.orm.base import Base
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.job import Job
from protea.infrastructure.orm.models.query.query_set import QUERY_SET_SOURCES, QuerySet

SHA = "c4f15a254272ec08bcd1c8e5667163ed546dee58d0d977ef7bb807560313122d"
OTRO_SHA = "e48b5ef866ce46dd733f175be9d3442a74c4c63bcb730574b6fa9c18d88efd62"

_MIGRACION = (
    pathlib.Path(__file__).resolve().parents[1]
    / "alembic/versions/c5e8d1a30b72_query_set_declares_its_population.py"
)


# --------------------------------------------------------------- sin base de datos


def _nombres_tocados(funcion: str) -> set[str]:
    """Every string literal the named migration function passes to an ``op.*`` call.

    The upgrade and the downgrade must name the SAME objects. A downgrade that
    drops something the upgrade never made, or leaves something it did, is a
    migration that cannot be rolled back, and nothing else in this repo would
    say so until somebody tried it on a real database.
    """
    arbol = ast.parse(_MIGRACION.read_text())
    objetivo = next(
        n for n in ast.walk(arbol) if isinstance(n, ast.FunctionDef) and n.name == funcion
    )
    nombres: set[str] = set()
    for nodo in ast.walk(objetivo):
        if not isinstance(nodo, ast.Call) or not isinstance(nodo.func, ast.Attribute):
            continue
        accion = nodo.func.attr
        if not accion.startswith(("add_", "drop_", "create_")):
            continue
        # add_column/drop_column name the column; the rest name the object
        # first, so the first string literal is the name in both shapes.
        literales = [
            a.value for a in nodo.args if isinstance(a, ast.Constant) and isinstance(a.value, str)
        ]
        if accion in ("add_column", "drop_column"):
            if accion == "drop_column" and len(literales) >= 2:
                nombres.add(literales[1])
            else:
                columna = next(
                    (
                        k.value
                        for a in nodo.args
                        if isinstance(a, ast.Call)
                        for k in a.args
                        if isinstance(k, ast.Constant) and isinstance(k.value, str)
                    ),
                    None,
                )
                if columna:
                    nombres.add(columna)
        elif literales:
            nombres.add(literales[0])
    return nombres


def test_the_downgrade_names_exactly_what_the_upgrade_made():
    subida = _nombres_tocados("upgrade")
    bajada = _nombres_tocados("downgrade")

    # ``query_set`` appears as the table in both, which is fine; what must
    # match is everything else.
    assert subida - {"query_set"} == bajada - {"query_set"}
    # And it is not vacuously true: the migration does make things.
    assert len(subida) > 6


def test_the_vocabulary_is_closed_and_says_what_it_cannot_say():
    """``legacy_undeclared`` is a VALUE, and that is the decision being pinned.

    A NULL cannot tell "nobody has written this yet" from "we looked and it
    cannot be established", and those are different facts about a set. The
    four rows that predate the column are the second case.
    """
    assert "legacy_undeclared" in QUERY_SET_SOURCES
    assert set(QUERY_SET_SOURCES) == {
        "evaluation_targets",
        "fasta_upload",
        "quick_annotation",
        # Added on 2026-11-08, and the list is hard-coded on purpose so that a
        # fifth kind has to come with a line here: the corpus's own proteins,
        # which is neither an upload nor a window's delta, and which no value
        # of the first four could say.
        "corpus_population",
        # The sixth, also 2026-11-08, and the hard-coded list did its job: this
        # test failed when the value was added and the line was not. One of
        # ADR-D57's two frozen vocabulary exam populations, over Swiss-Prot
        # 2022_03. It is not the corpus, because it is a sample with a
        # selection and a confirmation half, and it has no evaluation window at
        # all, which is also why these exams do not fit ADR-D55's exploration.
        "exam_population",
        "legacy_undeclared",
    }


# ------------------------------------------------------------------ contra Postgres


@pytest.fixture()
def pg_session(postgres_url: str):
    engine = create_engine(postgres_url, future=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session(engine, future=True) as session:
        yield session
    Base.metadata.drop_all(engine)


def _window(session: Session) -> uuid.UUID:
    snapshot = OntologySnapshot(
        obo_version=f"releases/{uuid.uuid4().hex[:8]}", obo_url="file:///dev/null"
    )
    session.add(snapshot)
    session.flush()
    aset = AnnotationSet(source="goa", ontology_snapshot_id=snapshot.id)
    session.add(aset)
    session.flush()
    row = EvaluationSet(
        old_annotation_set_id=aset.id,
        new_annotation_set_id=aset.id,
        pivot_snapshot_id=snapshot.id,
        old_native_snapshot_id=snapshot.id,
        new_native_snapshot_id=snapshot.id,
        truth_profile="projected",
        toi_rule="live_in_both_ends",
        not_codes="experimental",
        stats={},
    )
    session.add(row)
    session.flush()
    return row.id


def _job(session: Session) -> uuid.UUID:
    job = Job(operation="export_evaluation_targets", queue_name="protea.jobs", status="SUCCEEDED")
    session.add(job)
    session.flush()
    return job.id


def _derivado(session: Session, **cambios):
    """A fully declared derived set, with any field overridden."""
    campos = {
        "name": "targets-ea0f4334-c4f15a254272",
        "description": "the 40.531 delta targets of window ea0f4334",
        "source_kind": "evaluation_targets",
        "evaluation_set_id": _window(session),
        "artifact_uri": "s3://protea/eval_targets/x/targets-c4f15a254272.fasta",
        "content_sha256": SHA,
        "job_id": _job(session),
        "code_revision": "beafbd4f2e4141d59fa89e7587ab74355191b955",
    }
    campos.update(cambios)
    qs = QuerySet(**campos)
    session.add(qs)
    return qs


@pytest.mark.parametrize("falta", ["evaluation_set_id", "content_sha256", "job_id", "description"])
def test_a_derived_set_missing_any_of_its_four_facts_is_refused(pg_session, falta):
    """The four facts a derived population needs to be attributable.

    Dropping ``content_sha256`` also drops the uri, because the OTHER
    constraint forbids a locator with no identity, and that is the point: the
    two rules compose instead of leaving a gap between them.
    """
    cambios = {falta: None}
    if falta == "content_sha256":
        cambios["artifact_uri"] = None
    _derivado(pg_session, **cambios)

    with pytest.raises(IntegrityError):
        pg_session.flush()


def test_a_fully_declared_derived_set_is_accepted(pg_session):
    qs = _derivado(pg_session)
    pg_session.flush()

    assert qs.source_kind == "evaluation_targets"
    assert qs.content_sha256 == SHA


def test_an_unknown_source_kind_is_refused(pg_session):
    _derivado(pg_session, source_kind="whatever")

    with pytest.raises(IntegrityError):
        pg_session.flush()


def test_a_uri_without_its_sha_is_refused(pg_session):
    """A locator with no identity is what made the ground truth unverifiable."""
    _derivado(pg_session, source_kind="fasta_upload", content_sha256=None)

    with pytest.raises(IntegrityError):
        pg_session.flush()


def test_a_sha_without_a_uri_is_ALLOWED(pg_session):
    """The asymmetry is deliberate and this test is what pins it.

    An uploaded FASTA is hashed and not kept, so its bytes have an identity
    and no location. That set can still prove which population it is, which is
    the property the column exists for. The converse -- a location with no
    identity -- cannot.
    """
    qs = _derivado(pg_session, source_kind="fasta_upload", artifact_uri=None)
    pg_session.flush()

    assert qs.artifact_uri is None
    assert qs.content_sha256 == SHA


def test_the_same_window_and_bytes_cannot_be_declared_twice(pg_session):
    """One window plus one set of bytes is ONE population.

    Two rows would be two names for one thing, which is the defect this table
    already had with ``dianas-valid-211-227`` and its titular.
    """
    primera = _derivado(pg_session)
    pg_session.flush()
    _derivado(
        pg_session,
        name="otro-nombre-para-lo-mismo",
        evaluation_set_id=primera.evaluation_set_id,
        content_sha256=SHA,
    )

    with pytest.raises(IntegrityError):
        pg_session.flush()


def test_one_window_can_have_two_different_populations(pg_session):
    """Different categories of the same window are different bytes, so not a clash."""
    primera = _derivado(pg_session)
    pg_session.flush()
    segunda = _derivado(
        pg_session,
        name="targets-solo-nk",
        evaluation_set_id=primera.evaluation_set_id,
        content_sha256=OTRO_SHA,
    )
    pg_session.flush()

    assert segunda.id != primera.id


def test_a_window_with_a_query_set_cannot_be_deleted_quietly(pg_session):
    """RESTRICT and not SET NULL: nulling it would silently UNDECLARE the set."""
    from sqlalchemy import text

    qs = _derivado(pg_session)
    pg_session.flush()

    with pytest.raises(IntegrityError):
        pg_session.execute(
            text("DELETE FROM evaluation_set WHERE id = :v"), {"v": qs.evaluation_set_id}
        )
        pg_session.flush()


def test_a_dirty_revision_fits_in_the_column(pg_session):
    """``code_revision()`` returns ``"<40 hex>+dirty"``, which is 46 characters.

    A ``String(40)`` column refuses the INSERT in exactly the case the column
    exists to record: a set built from a tree that was not committed. So the
    column is 64, matching ``evaluation_set.code_revision``, and this test is
    what keeps it that way. The desktop session caught the original.
    """
    sucia = "beafbd4f2e4141d59fa89e7587ab74355191b955+dirty"
    assert len(sucia) == 46

    qs = _derivado(pg_session, code_revision=sucia)
    pg_session.flush()

    assert qs.code_revision == sucia


def test_the_job_that_built_a_declared_set_cannot_be_deleted_quietly(pg_session):
    """RESTRICT and not SET NULL, which is a composition bug the peer found.

    The CHECK requires ``job_id`` for a derived set. With SET NULL, deleting
    that job would null the column, trip the CHECK, and fail with a message
    naming a constraint and explaining nothing. RESTRICT fails on the foreign
    key instead, which says what actually happened.
    """
    from sqlalchemy import text

    qs = _derivado(pg_session)
    pg_session.flush()

    with pytest.raises(IntegrityError) as exc:
        pg_session.execute(text("DELETE FROM job WHERE id = :v"), {"v": qs.job_id})
        pg_session.flush()

    # The error is about the reference, not about the CHECK.
    assert "fk_query_set_job_id" in str(exc.value) or "foreign key" in str(exc.value).lower()
