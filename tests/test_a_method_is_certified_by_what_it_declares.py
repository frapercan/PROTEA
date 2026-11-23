"""A method's prediction set is scored only on a frame that starts after everything it read.

ADR-D60, decision 5, lists among the evaluator's refusals "a method whose
instance, or any of whose windows or pins, ends after the frame starts", and
decision 3 says the leakage guard reads what a method declares. Before methods
had rows, ``refuse_uncertifiable_encoding`` returned in silence for a set
without a configuration, so a method's set would have been scored on any frame.

THE CASE THAT MATTERS IS THE AUTHOR'S RULE OF 2026-11-15, "nothing learned
after 211": a method validated at the 211 instance may train on windows that
end at 211 and no later, and its validation frame is 211 to 227. A window that
ends at 227 is the 227 instance's training data, and scoring it on 211 to 227
measures memory. The windows below are the campaign's own: 160 to 194, 177 to
205, 194 to 211 and 211 to 227.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

from protea.core._evaluation_leakage import refuse_uncertifiable_encoding
from protea.core.leakage_guard import LeakageRefusal

_RELEASES = (160, 177, 194, 205, 211, 227, 230)
#: One annotation set per release, by a uuid derived from the release.
_SET = {release: uuid.UUID(int=release) for release in _RELEASES}
#: The training windows, by evaluation set id.
_WINDOW = {
    (160, 194): uuid.UUID(int=1_160_194),
    (177, 205): uuid.UUID(int=1_177_205),
    (194, 211): uuid.UUID(int=1_194_211),
    (211, 227): uuid.UUID(int=1_211_227),
}
_METHOD = uuid.UUID("5d1f9a3e-7c2b-5e4f-8a6d-0b9c8e7f6a51")
_WEIGHTS = uuid.UUID("0f6e2c2a-6c8e-4a55-9e7b-3e5c9d1a2b3c")


class _Row:
    def __init__(self, **kw: Any) -> None:
        self.__dict__.update(kw)


class _Session:
    """Enough of a Session to answer the gets the guard makes."""

    def __init__(self, rows: dict[tuple[str, Any], Any]) -> None:
        self._rows = rows

    def get(self, model: type, pk: Any) -> Any:
        return self._rows.get((model.__name__, pk))


def _window(start: int, end: int, role: str = "fit") -> _Row:
    return _Row(evaluation_set_id=_WINDOW[(start, end)], role=role)


def _pin(role: str, *refs: str) -> _Row:
    return _Row(role=role, sha256=("a" if role == "plm" else "b") * 64, refs=list(refs))


def _rows(
    *,
    instance: int = 211,
    windows: tuple[_Row, ...] = (),
    pins: tuple[_Row, ...] = (),
    weights_cut: int | None = None,
) -> dict[tuple[str, Any], Any]:
    rows: dict[tuple[str, Any], Any] = {
        ("AnnotationSet", _SET[r]): _Row(source_version=str(r)) for r in _RELEASES
    }
    rows.update(
        {
            ("EvaluationSet", eid): _Row(
                old_annotation_set_id=_SET[start], new_annotation_set_id=_SET[end]
            )
            for (start, end), eid in _WINDOW.items()
        }
    )
    rows[("Method", _METHOD)] = _Row(
        code="booster-211",
        instance_release=instance,
        training_windows=list(windows),
        pins=list(pins),
    )
    rows[("ModelWeights", _WEIGHTS)] = _Row(
        trained_on_annotation_set_id=None if weights_cut is None else _SET[weights_cut]
    )
    return rows


def _score(
    rows: dict[tuple[str, Any], Any],
    *,
    frame: tuple[int, int] = (211, 227),
    config: str | None = None,
) -> None:
    """Run the guard for the method's set on ``frame``."""
    refuse_uncertifiable_encoding(
        _Session(rows),  # type: ignore[arg-type]
        _Row(embedding_config_id=config, method_id=_METHOD),  # type: ignore[arg-type]
        _Row(old_annotation_set_id=_SET[frame[0]], new_annotation_set_id=_SET[frame[1]]),  # type: ignore[arg-type]
    )


class TestTheWindows:
    def test_a_window_that_ends_after_the_frame_starts_is_refused(self) -> None:
        # The 227 instance's training data, offered to the 211 validation frame.
        rows = _rows(windows=(_window(177, 205), _window(211, 227)))
        with pytest.raises(LeakageRefusal) as refused:
            _score(rows)
        message = str(refused.value)
        assert f"fit window {_WINDOW[(211, 227)]} ends at release 227" in message
        assert "at or before release 211" in message
        # The clean window is not named: a refusal lists what is wrong.
        assert str(_WINDOW[(177, 205)]) not in message

    def test_windows_that_end_where_the_frame_starts_pass(self) -> None:
        # The 211 instance as the author fixed it: three windows ending by 211.
        _score(_rows(windows=(_window(160, 194), _window(177, 205), _window(194, 211, "holdout"))))

    def test_a_holdout_is_dated_like_a_fit_window(self) -> None:
        # Selection reads labels too, so selecting on 211 to 227 and then
        # scoring on it is the same leak by another door.
        rows = _rows(windows=(_window(194, 211), _window(211, 227, "holdout")))
        with pytest.raises(LeakageRefusal, match="holdout window"):
            _score(rows)

    def test_the_same_method_is_clean_on_a_later_frame(self) -> None:
        # The 227 instance, trained through 211 to 227, read on 227 to 230.
        rows = _rows(instance=227, windows=(_window(194, 211), _window(211, 227)))
        _score(rows, frame=(227, 230))

    def test_a_window_that_cannot_be_dated_is_refused(self) -> None:
        stray = _Row(evaluation_set_id=uuid.UUID(int=99), role="fit")
        with pytest.raises(LeakageRefusal, match="cannot be dated"):
            _score(_rows(windows=(stray,)))


class TestTheInstance:
    def test_an_instance_cut_after_the_frame_start_is_refused(self) -> None:
        with pytest.raises(LeakageRefusal, match="its instance cut ends at release 227"):
            _score(_rows(instance=227))

    def test_an_instance_cut_at_the_frame_start_passes(self) -> None:
        _score(_rows(instance=211))


class TestThePins:
    def test_a_pin_through_a_late_annotation_set_is_refused(self) -> None:
        rows = _rows(pins=(_pin("term-vectors", f"annotation_set:{_SET[227]}"),))
        with pytest.raises(LeakageRefusal, match="term-vectors pin bbbbbbbbbbbb"):
            _score(rows)

    def test_a_pin_through_a_window_is_dated_by_where_the_window_ends(self) -> None:
        late = _rows(pins=(_pin("reranker", f"evaluation_set:{_WINDOW[(211, 227)]}"),))
        with pytest.raises(LeakageRefusal, match="ends at release 227"):
            _score(late)
        _score(_rows(pins=(_pin("reranker", f"evaluation_set:{_WINDOW[(194, 211)]}"),)))

    def test_a_weights_row_dates_a_pin_by_the_cut_it_was_fitted_on(self) -> None:
        pin = _pin("plm", f"model_weights:{_WEIGHTS}")
        with pytest.raises(LeakageRefusal, match="ends at release 227"):
            _score(_rows(pins=(pin,), weights_cut=227))
        _score(_rows(pins=(pin,), weights_cut=211))

    def test_a_weights_row_that_declares_no_cut_makes_no_claim(self) -> None:
        # A pretrained backbone registered by digest: NULL is "not fitted", as
        # in embedding_config, and refusing it would refuse every PLM pin.
        _score(_rows(pins=(_pin("plm", f"model_weights:{_WEIGHTS}"),), weights_cut=None))

    def test_a_dated_reference_to_a_row_that_is_gone_is_refused(self) -> None:
        gone = _pin("plm", f"model_weights:{uuid.UUID(int=12345)}")
        with pytest.raises(LeakageRefusal, match="cannot be dated"):
            _score(_rows(pins=(gone,)))

    def test_a_pin_that_names_only_locators_is_not_refused(self) -> None:
        pin = _pin("plm", "s3://protea/weights/ankh-base.tar", "hf:ElnaggarLab/ankh-base")
        _score(_rows(pins=(pin,)))


def test_every_late_declaration_is_named_in_one_refusal() -> None:
    rows = _rows(
        instance=227,
        windows=(_window(211, 227),),
        pins=(_pin("term-vectors", f"annotation_set:{_SET[230]}"),),
    )
    with pytest.raises(LeakageRefusal) as refused:
        _score(rows)
    message = str(refused.value)
    assert "its instance cut ends at release 227" in message
    assert f"fit window {_WINDOW[(211, 227)]} ends at release 227" in message
    assert "ends at release 230" in message


class TestWhatCannotBeCertified:
    def test_a_method_on_a_frame_without_release_ordinals_is_refused(self) -> None:
        # Silent for a configuration, because older evaluation sets have no
        # ordinals; a method is new, and nothing it declares can be certified.
        rows = _rows()
        rows[("AnnotationSet", _SET[211])] = _Row(source_version="v1")
        with pytest.raises(LeakageRefusal, match="release ordinals"):
            _score(rows)

    def test_a_method_the_record_does_not_hold_is_refused(self) -> None:
        rows = _rows()
        del rows[("Method", _METHOD)]
        with pytest.raises(LeakageRefusal, match="not in the record"):
            _score(rows)


class TestAMethodWithAConfiguration:
    """An embedding-reranker method carries both, and is held to both."""

    @staticmethod
    def _with_config(rows: dict[tuple[str, Any], Any], backend: str, cut: int | None) -> None:
        rows[("EmbeddingConfig", "cfg")] = _Row(
            model_backend=backend,
            model_name="arm",
            trained_on_annotation_set_id=None if cut is None else _SET[cut],
        )

    def test_a_clean_method_on_a_late_encoding_is_refused_by_the_encoding(self) -> None:
        rows = _rows(windows=(_window(194, 211),))
        self._with_config(rows, "learned-code", 227)
        with pytest.raises(LeakageRefusal, match="encoding 'arm'"):
            _score(rows, config="cfg")

    def test_a_late_method_on_a_pretrained_backbone_is_refused_by_the_method(self) -> None:
        rows = _rows(windows=(_window(211, 227),))
        self._with_config(rows, "ankh", None)
        with pytest.raises(LeakageRefusal, match="method 'booster-211'"):
            _score(rows, config="cfg")

    def test_both_clean_pass(self) -> None:
        rows = _rows(windows=(_window(194, 211),))
        self._with_config(rows, "learned-code", 211)
        _score(rows, config="cfg")


def test_a_set_without_a_method_reads_exactly_as_before() -> None:
    # Every set written before the method tables: a configuration and NULL
    # here. The frame without ordinals stays silent for it, as it always was.
    rows = _rows()
    rows[("EmbeddingConfig", "cfg")] = _Row(
        model_backend="learned-code", model_name="arm", trained_on_annotation_set_id=_SET[227]
    )
    legacy = _Row(embedding_config_id="cfg", method_id=None)
    session = _Session(rows)
    with pytest.raises(LeakageRefusal, match="encoding 'arm'"):
        refuse_uncertifiable_encoding(
            session,  # type: ignore[arg-type]
            legacy,  # type: ignore[arg-type]
            _Row(old_annotation_set_id=_SET[211], new_annotation_set_id=_SET[227]),  # type: ignore[arg-type]
        )
    rows[("AnnotationSet", _SET[211])] = _Row(source_version="v1")
    refuse_uncertifiable_encoding(
        session,  # type: ignore[arg-type]
        legacy,  # type: ignore[arg-type]
        _Row(old_annotation_set_id=_SET[211], new_annotation_set_id=_SET[227]),  # type: ignore[arg-type]
    )
