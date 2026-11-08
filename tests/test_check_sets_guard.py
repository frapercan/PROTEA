"""The set guard can refuse, and refuses for the right reason.

A guard that cannot refuse is worse than no guard, because the report reads as
though it had checked. So these drive `_audit` with synthetic tables instead of
the live schema: the gate is ABOUT the schema, so testing it against the schema
would make the test change with every migration and assert nothing stable.

Two of these exist because the first draft of the gate got them wrong:

* it read failure off ``value.isupper()`` and a prefix list, so a criterion
  whose message happened to start lowercase passed silently;
* it matched a sha to a uri by stem only, and reported
  ``information_accretion_set`` as unverifiable when its one ``artifact_uri``
  is covered by ``content_sha256``. A gate that fails a table doing the right
  thing gets switched off.
"""

from __future__ import annotations

import importlib.util
import pathlib

import pytest
from sqlalchemy import Column, ForeignKey, Integer, MetaData, String, Table

GUARD = pathlib.Path(__file__).resolve().parents[1] / "scripts/check_sets.py"


def _guard():
    """The script, loaded BY PATH: ``scripts/`` is not an importable package."""
    spec = importlib.util.spec_from_file_location("_check_sets", GUARD)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture()
def guard():
    return _guard()


def _tabla(nombre: str, *columnas: Column) -> Table:
    return Table(nombre, MetaData(), Column("id", Integer, primary_key=True), *columnas)


class TestDeclarado:
    def test_a_set_with_no_foreign_key_fails(self, guard) -> None:
        t = _tabla("x_set", Column("name", String))
        texto, cumple = guard._audit(t)["declarado"]
        assert not cumple and "SIN NINGUNA FK" in texto

    def test_job_id_alone_is_not_a_derivation(self, guard) -> None:
        # ``job_id`` is provenance. A set whose only FK is its job says who
        # made it and not what it is made OF, which is the whole criterion.
        t = _tabla("x_set", Column("job_id", Integer, ForeignKey("job.id")))
        assert not guard._audit(t)["declarado"][1]

    def test_a_real_derivation_passes(self, guard) -> None:
        t = _tabla("x_set", Column("corpus_id", Integer, ForeignKey("annotation_set.id")))
        texto, cumple = guard._audit(t)["declarado"]
        assert cumple and texto == "corpus_id"


class TestDescrito:
    def test_no_column_to_describe_it_in_fails(self, guard) -> None:
        assert not guard._audit(_tabla("x_set"))["descrito"][1]

    def test_a_description_column_passes(self, guard) -> None:
        t = _tabla("x_set", Column("description", String))
        assert guard._audit(t)["descrito"] == ("description", True)

    def test_a_meta_bag_does_not_count(self, guard) -> None:
        # ``annotation_set`` and ``prediction_set`` both carry prose in
        # ``meta`` on every row, 75 of 75 and 23 of 23. An unconstrained bag
        # is not a description: nothing says what belongs in it.
        t = _tabla("x_set", Column("meta", String))
        assert not guard._audit(t)["descrito"][1]


class TestVerificable:
    def test_a_uri_with_no_sha_fails_and_names_it(self, guard) -> None:
        t = _tabla("x_set", Column("groundtruth_uri", String))
        texto, cumple = guard._audit(t)["verificable"]
        assert not cumple and "groundtruth_uri" in texto

    def test_one_uri_answered_by_a_generic_content_sha_passes(self, guard) -> None:
        # The information_accretion_set shape, and the false positive the
        # first draft produced.
        t = _tabla("x_set", Column("artifact_uri", String), Column("content_sha256", String))
        assert guard._audit(t)["verificable"][1]

    def test_several_uris_and_one_generic_sha_cannot_pass(self, guard) -> None:
        # Nothing would say which of the three the digest covers.
        t = _tabla(
            "x_set",
            Column("train_uri", String),
            Column("eval_uri", String),
            Column("manifest_uri", String),
            Column("content_sha256", String),
        )
        texto, cumple = guard._audit(t)["verificable"]
        assert not cumple
        assert "train_uri" in texto and "eval_uri" in texto and "manifest_uri" in texto

    def test_each_uri_with_its_own_stem_matched_sha_passes(self, guard) -> None:
        t = _tabla(
            "x_set",
            Column("train_uri", String), Column("train_sha256", String),
            Column("eval_uri", String), Column("eval_sha", String),
        )
        assert guard._audit(t)["verificable"][1]

    def test_a_set_that_points_at_no_bytes_has_nothing_to_verify(self, guard) -> None:
        assert guard._audit(_tabla("x_set"))["verificable"][1]


class TestProducidoPor:
    def test_the_job_without_the_revision_fails(self, guard) -> None:
        # The rules a row declares say what it is by DECLARATION; the code
        # says what it is by IMPLEMENTATION. On 2026-11-07 a fix to the
        # synonym map changed a whole class of windows with every identity
        # field untouched, so the job alone does not pin the delta.
        t = _tabla("x_set", Column("job_id", Integer, ForeignKey("job.id")))
        texto, cumple = guard._audit(t)["producido_por"]
        assert not cumple and "REVISION" in texto

    def test_both_together_pass(self, guard) -> None:
        t = _tabla(
            "x_set",
            Column("job_id", Integer, ForeignKey("job.id")),
            Column("code_revision", String),
        )
        assert guard._audit(t)["producido_por"][1]

    def test_neither_says_so(self, guard) -> None:
        assert not guard._audit(_tabla("x_set"))["producido_por"][1]


class TestTheVerdictIsNotReadOffTheSpelling:
    def test_a_failure_whose_message_is_lowercase_still_fails(self, guard) -> None:
        # What the first draft got wrong: it inferred failure from
        # ``value.isupper()`` and a prefix list. ``_falla`` now reads the
        # boolean the check decided.
        assert guard._falla({"x": ("todo bien en minusculas", False)})

    def test_a_pass_whose_message_shouts_still_passes(self, guard) -> None:
        assert not guard._falla({"x": ("ESTO ESTA BIEN", True)})


class TestTheRegistryAndTheSchemaAgree:
    def test_every_declared_set_is_still_a_table(self, guard) -> None:
        # The other direction, a `*_set` table missing from EXPECTED, is an
        # error the gate reports; this one would make the gate audit nothing.
        import protea.infrastructure.orm.models  # noqa: F401
        from protea.infrastructure.orm.base import Base

        faltan = sorted(set(guard.EXPECTED) - set(Base.metadata.tables))
        assert not faltan, f"declaradas y ya no son tablas: {faltan}"

    def test_dataset_is_audited_even_though_its_name_lacks_the_suffix(self, guard) -> None:
        # The bug the first draft had: detection by `_set` suffix alone left
        # `dataset` out of its own report, which is the failure mode the gate
        # exists to prevent.
        assert "dataset" in guard.EXPECTED
        assert not "dataset".endswith("_set")

    def test_every_declared_set_says_what_it_is(self, guard) -> None:
        for nombre, que_es in guard.EXPECTED.items():
            assert len(que_es) > 30, f"{nombre} no esta descrito, solo nombrado"
