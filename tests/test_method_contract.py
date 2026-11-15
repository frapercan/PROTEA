"""A method's id is derived from its spec, and the spec refuses what has no single spelling.

ADR-D60, decision 3: a method has an identity of its own, ``uuid5`` over the
canonical JSON of its ``MethodSpec``. Three properties make that identity worth
having, and each is pinned here:

* the namespace and the id of one fixed spec do not move, so an id computed
  today is the id any machine computes tomorrow;
* what a caller cannot mean does not move the id: the order keys were written
  in, the order of the pins and the order and spelling of their references;
* every field that does mean something moves it.

THE FIXTURE IS HETEROGENEOUS ON PURPOSE. A spec built from defaults (no pins,
an empty recipe, every flag false) passes a derivation that ignores half its
fields, so the one below carries two pins with different shapes, a nested
recipe with an integer, a float, a boolean, a list and a non-ASCII string.
"""

from __future__ import annotations

import ast
import hashlib
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

from protea.core.contracts.method import (
    DATED_REFERENCES,
    METHOD_NAMESPACE,
    RUNTIMES,
    WINDOW_ROLES,
    Family,
    InstanceCut,
    MethodSpec,
    Pin,
    TrainingIdentity,
    TrainingWindow,
    canonical_json,
    derive_method_id,
    row_reference,
)

_SNAPSHOT = uuid.UUID("6b78af68-eb01-477e-a599-fcde11ff0135")
_WEIGHTS = uuid.UUID("0f6e2c2a-6c8e-4a55-9e7b-3e5c9d1a2b3c")
_PLM_SHA = "a" * 64
_RERANKER_SHA = "b" * 64

#: The canonical JSON of :func:`_spec`, written out so a change to the encoding
#: shows up as a diff of this string and not only as a different id.
_GOLDEN_JSON = (
    '{"code":"knn-ankh-211","explains":false,"family":"embedding-reranker",'
    '"instance":{"ontology_snapshot_id":"6b78af68-eb01-477e-a599-fcde11ff0135","release":211},'
    '"pins":[{"refs":["model_weights:0f6e2c2a-6c8e-4a55-9e7b-3e5c9d1a2b3c",'
    '"s3://protea/weights/ankh-base.tar"],"role":"plm","sha256":"' + _PLM_SHA + '"},'
    '{"refs":[],"role":"reranker","sha256":"' + _RERANKER_SHA + '"}],'
    '"recipe":{"alpha":0.25,"donors":{"codes":["EXP","IDA"],"reviewed_only":false},'
    '"k":5,"metric":"cosine","note":"se\\u00f1al"},"runtime":"native"}'
)
_GOLDEN_ID = uuid.UUID("e1270617-461e-5afc-a66f-a6839ec22c22")
_GOLDEN_SHA256 = "c1256d1b5f1d62ae3bc8a9faaa60430e1c4c9fafc7c773798f67c6b645c81a78"


def _recipe() -> dict[str, Any]:
    return {
        "k": 5,
        "metric": "cosine",
        "donors": {"codes": ["EXP", "IDA"], "reviewed_only": False},
        "alpha": 0.25,
        "note": "se\u00f1al",
    }


def _pins() -> tuple[Pin, ...]:
    return (
        Pin("reranker", _RERANKER_SHA),
        Pin("plm", _PLM_SHA, (f"model_weights:{_WEIGHTS}", "s3://protea/weights/ankh-base.tar")),
    )


def _spec(**over: Any) -> MethodSpec:
    fields: dict[str, Any] = {
        "family": Family.EMBEDDING_RERANKER,
        "code": "knn-ankh-211",
        "instance": InstanceCut(211, _SNAPSHOT),
        "recipe": _recipe(),
        "pins": _pins(),
        "runtime": "native",
        "explains": False,
    }
    fields.update(over)
    return MethodSpec(**fields)


class TestTheIdDoesNotMove:
    def test_the_namespace_is_the_pinned_literal_and_how_it_was_made(self) -> None:
        assert METHOD_NAMESPACE == uuid.UUID("c034d65c-55e2-5fe7-8594-0dbc9ee317bd")
        assert METHOD_NAMESPACE == uuid.uuid5(uuid.NAMESPACE_URL, "https://protea/method")

    def test_a_fixed_spec_has_a_fixed_canonical_form_and_id(self) -> None:
        spec = _spec()
        assert spec.canonical_json() == _GOLDEN_JSON
        assert spec.method_id == _GOLDEN_ID
        assert spec.spec_sha256 == _GOLDEN_SHA256

    def test_the_id_is_uuid5_over_the_canonical_json(self) -> None:
        spec = _spec()
        assert spec.method_id == uuid.uuid5(METHOD_NAMESPACE, spec.canonical_json())
        assert spec.method_id.version == 5
        assert derive_method_id(spec) == spec.method_id

    def test_the_digest_is_the_sha256_of_the_same_bytes(self) -> None:
        spec = _spec()
        expected = hashlib.sha256(spec.canonical_json().encode("ascii")).hexdigest()
        assert spec.spec_sha256 == expected


class TestTheCanonicalForm:
    def test_keys_are_sorted_separators_compact_and_text_ascii(self) -> None:
        assert canonical_json({"b": 1, "a": "\u00f1", "c": [True, None]}) == (
            '{"a":"\\u00f1","b":1,"c":[true,null]}'
        )

    def test_the_order_keys_were_written_in_does_not_move_the_id(self) -> None:
        reversed_recipe = dict(reversed(list(_recipe().items())))
        assert _spec(recipe=reversed_recipe).method_id == _GOLDEN_ID

    def test_the_order_of_the_pins_does_not_move_the_id(self) -> None:
        assert _spec(pins=tuple(reversed(_pins()))).method_id == _GOLDEN_ID

    def test_the_order_and_spelling_of_references_do_not_move_the_id(self) -> None:
        # The same weights row written with capitals and braces is one row, so
        # it is one reference and one method.
        respelled = Pin(
            "plm",
            _PLM_SHA,
            ("s3://protea/weights/ankh-base.tar", f"model_weights:{{{str(_WEIGHTS).upper()}}}"),
        )
        spec = _spec(pins=(Pin("reranker", _RERANKER_SHA), respelled))
        assert spec.method_id == _GOLDEN_ID

    def test_an_integer_and_a_float_are_two_recipes(self) -> None:
        # 5 and 5.0 have two JSON spellings, so they are two methods, and the
        # specs are unequal although Python holds 5 == 5.0. A reader who wants
        # one method has to write one spelling.
        as_float = _spec(recipe={**_recipe(), "k": 5.0})
        assert as_float.method_id != _GOLDEN_ID
        assert as_float != _spec()
        assert _spec(recipe={**_recipe(), "k": True}) != _spec(recipe={**_recipe(), "k": 1})


@pytest.mark.parametrize(
    "change",
    [
        {"family": Family.WORLD_MODEL},
        {"code": "knn-ankh-227"},
        {"instance": InstanceCut(227, _SNAPSHOT)},
        {"instance": InstanceCut(211, uuid.UUID("c6c7f402-3f59-4f8b-8a44-fc8cfa1264b9"))},
        {"recipe": {**_recipe(), "k": 6}},
        {"recipe": {**_recipe(), "donors": {"codes": ["EXP"], "reviewed_only": False}}},
        {"pins": (Pin("reranker", "c" * 64), _pins()[1])},
        {"pins": (Pin("booster", _RERANKER_SHA), _pins()[1])},
        {"pins": (Pin("reranker", _RERANKER_SHA, ("s3://protea/boosters/b.txt",)), _pins()[1])},
        {"pins": _pins()[1:]},
        {"runtime": "container"},
        {"explains": True},
    ],
    ids=lambda change: next(iter(change)),
)
def test_every_field_of_the_spec_moves_the_id(change: dict[str, Any]) -> None:
    moved = _spec(**change)
    assert moved.method_id != _GOLDEN_ID
    assert moved.spec_sha256 != _GOLDEN_SHA256


class TestTheSpecCannotChangeUnderItsId:
    def test_mutating_the_source_recipe_does_not_reach_the_spec(self) -> None:
        source = _recipe()
        spec = _spec(recipe=source)
        source["k"] = 99
        source["donors"]["codes"].append("TAS")
        assert spec.method_id == _GOLDEN_ID

    def test_the_stored_recipe_is_read_only(self) -> None:
        spec = _spec()
        with pytest.raises(TypeError):
            spec.recipe["k"] = 6  # type: ignore[index]
        assert spec.recipe["donors"]["codes"] == ("EXP", "IDA")

    def test_equal_specs_are_equal_and_hash_alike(self) -> None:
        assert _spec() == _spec(pins=tuple(reversed(_pins())))
        assert hash(_spec()) == hash(_spec(recipe=dict(reversed(list(_recipe().items())))))


@pytest.mark.parametrize(
    "recipe",
    [
        {1: "an integer key collides with the string '1'"},
        {"x": float("nan")},
        {"x": float("inf")},
        {"x": uuid.UUID(int=7)},
        {"x": {1, 2}},
    ],
    ids=["int-key", "nan", "inf", "uuid", "set"],
)
def test_a_value_with_no_single_json_spelling_is_refused(recipe: dict[Any, Any]) -> None:
    with pytest.raises(ValueError):
        _spec(recipe=recipe)


@pytest.mark.parametrize(
    "change",
    [
        {"code": "Knn Ankh"},
        {"code": ""},
        {"code": "x" * 65},
        {"family": "arm"},
        {"runtime": "docker"},
        {"explains": 1},
        {"recipe": ["not", "a", "mapping"]},
        {"pins": (Pin("plm", _PLM_SHA), Pin("plm", _PLM_SHA, ("s3://elsewhere",)))},
        {"pins": ("plm",)},
    ],
    ids=[
        "code-with-capitals-and-space",
        "empty-code",
        "code-too-long",
        "unknown-family",
        "unknown-runtime",
        "explains-not-bool",
        "recipe-not-mapping",
        "pin-twice",
        "pin-not-a-pin",
    ],
)
def test_a_spec_that_declares_something_impossible_is_refused(change: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        _spec(**change)


class TestThePartsRefuseOnTheirOwn:
    @pytest.mark.parametrize("release", [0, -1, True, "211"])
    def test_an_instance_release_is_a_positive_ordinal(self, release: Any) -> None:
        with pytest.raises(ValueError):
            InstanceCut(release, _SNAPSHOT)

    def test_the_instance_snapshot_is_a_uuid(self) -> None:
        assert InstanceCut(211, str(_SNAPSHOT)).ontology_snapshot_id == _SNAPSHOT
        with pytest.raises(ValueError):
            InstanceCut(211, "not-a-snapshot")

    @pytest.mark.parametrize(
        "pin",
        [
            {"role": "PLM", "sha256": _PLM_SHA},
            {"role": "plm", "sha256": "abc"},
            {"role": "plm", "sha256": _PLM_SHA.upper()},
            {"role": "plm", "sha256": _PLM_SHA, "refs": "s3://a-string-not-a-tuple"},
            {"role": "plm", "sha256": _PLM_SHA, "refs": ("annotation_set:not-a-uuid",)},
            {"role": "plm", "sha256": _PLM_SHA, "refs": ("s3://a", "s3://a")},
            {"role": "plm", "sha256": _PLM_SHA, "refs": ("with space",)},
        ],
        ids=[
            "role-capitals",
            "short-sha",
            "upper-sha",
            "refs-string",
            "malformed-dated-ref",
            "repeated-ref",
            "ref-with-space",
        ],
    )
    def test_a_malformed_pin_is_refused(self, pin: dict[str, Any]) -> None:
        with pytest.raises(ValueError):
            Pin(**pin)

    def test_a_dated_reference_names_its_row_and_a_locator_names_none(self) -> None:
        assert set(DATED_REFERENCES) == {"annotation_set", "evaluation_set", "model_weights"}
        assert row_reference(f"evaluation_set:{_WEIGHTS}") == ("evaluation_set", _WEIGHTS)
        assert row_reference("s3://protea/weights/ankh-base.tar") is None
        assert row_reference("sha256:" + _PLM_SHA) is None

    def test_the_closed_vocabularies(self) -> None:
        assert {f.value for f in Family} == {
            "embedding-reranker",
            "world-model",
            "explainable-units",
        }
        assert RUNTIMES == ("native", "container")
        assert WINDOW_ROLES == ("fit", "holdout")


def _identity(**over: Any) -> TrainingIdentity:
    fields: dict[str, Any] = {
        "windows": (
            TrainingWindow(uuid.UUID(int=1), "fit"),
            TrainingWindow(uuid.UUID(int=2), "fit"),
        ),
        "holdout": TrainingWindow(uuid.UUID(int=3), "holdout"),
        "data": ("d" * 64,),
        "code_revision": "c83f327a1b2c3d4e5f60718293a4b5c6d7e8f901",
        "sibling_revisions": {"protea-method": "a120f613b9d2", "protea-contracts": "74d329ff22a7"},
        "evaluator_revision": "203af0a28f0bb6f8cd0588c0d6d02ebf251eccce",
        "label_free": False,
        "pretrained_exceptions": ("ankh-base",),
    }
    fields.update(over)
    return TrainingIdentity(**fields)


class TestATrainingIdentity:
    def test_it_names_every_window_it_read_labels_from(self) -> None:
        identity = _identity()
        assert [w.role for w in identity.all_windows()] == ["fit", "fit", "holdout"]
        assert identity.all_windows()[-1].evaluation_set_id == uuid.UUID(int=3)

    def test_a_label_free_training_names_no_window(self) -> None:
        lexicon = _identity(windows=(), holdout=None, label_free=True)
        assert lexicon.all_windows() == ()
        with pytest.raises(ValueError, match="label-free"):
            _identity(label_free=True)

    def test_a_training_that_read_labels_names_its_windows(self) -> None:
        with pytest.raises(ValueError, match="must name the windows"):
            _identity(windows=(), holdout=None)

    @pytest.mark.parametrize(
        "change",
        [
            {"windows": (TrainingWindow(uuid.UUID(int=1), "holdout"),)},
            {"holdout": TrainingWindow(uuid.UUID(int=3), "fit")},
            {"holdout": TrainingWindow(uuid.UUID(int=1), "holdout")},
            {"windows": ("not-a-window",)},
            {"data": ("not-a-digest",)},
            {"code_revision": ""},
            {"evaluator_revision": "two words"},
            {"sibling_revisions": {"protea-method": ""}},
            {"pretrained_exceptions": ("",)},
            {"label_free": "no"},
        ],
        ids=[
            "fit-list-holds-a-holdout",
            "holdout-with-fit-role",
            "one-window-twice",
            "not-a-window",
            "bad-digest",
            "empty-revision",
            "revision-with-space",
            "empty-sibling-revision",
            "empty-exception",
            "label-free-not-bool",
        ],
    )
    def test_an_impossible_identity_is_refused(self, change: dict[str, Any]) -> None:
        with pytest.raises(ValueError):
            _identity(**change)

    def test_a_window_role_is_closed(self) -> None:
        with pytest.raises(ValueError):
            TrainingWindow(uuid.UUID(int=1), "validation")  # type: ignore[arg-type]

    def test_the_sibling_revisions_cannot_change_after_the_fact(self) -> None:
        source = {"protea-method": "a120f613b9d2"}
        identity = _identity(sibling_revisions=source)
        source["protea-method"] = "changed"
        assert identity.sibling_revisions["protea-method"] == "a120f613b9d2"


def test_the_contract_imports_nothing_outside_the_standard_library() -> None:
    """The contract travels to nodes that hold no database stack.

    Checked on the module's own imports. Importing it through the package still
    runs ``protea/core/contracts/__init__.py``, which is not this module's to
    decide (ADR-D60 plan, B1).
    """
    source = Path(__file__).resolve().parents[1] / "protea/core/contracts/method.py"
    imported: set[str] = set()
    for node in ast.walk(ast.parse(source.read_text())):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert imported, "the walk found no import at all; the test reads the wrong file"
    assert imported - {"__future__"} <= set(sys.stdlib_module_names)
