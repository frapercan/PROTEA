"""The Substrate node's denominator, expanded.

The node publishes a ratio, and a ratio is where this record is least
readable: "1 of 13" names neither the representation in use nor the twelve it
was picked over. ``build_representations`` expands that denominator off the
same rows the node counted, and these tests hold it to four rules.

* The three counts it prints are the node's own ratio. A section that could
  disagree with the node beside it would be worse than no section.
* A configuration with no stored embedding is ``unbuilt``, and stays out of
  the built count. It is not an untried alternative, it is an unbuilt one, and
  counting it would overstate what was passed over.
* An absent ``param_count`` survives as null and orders nothing. A column
  missing from part of a table cannot rank it.
* Coverage is refused rather than guessed. With chunking on a stored row is a
  chunk and not a sequence, and a plausible wrong coverage is
  indistinguishable from a right one.
"""

from __future__ import annotations

from typing import Any

from protea.api.routers.graph import build_representations


def _substrate(**over: Any) -> dict[str, Any]:
    """One row shaped like the substrate read, with everything present."""
    row: dict[str, Any] = {
        "id": "ec-1",
        "label": "esm2_650m",
        "display_name": "esm2_650m",
        "model_name": "facebook/esm2_t33_650M_UR50D",
        "model_backend": "esm",
        "family": "esm2",
        "param_count": 652353941,
        "layer_indices": "[0]",
        "layer_agg": "mean",
        "pooling": "mean",
        "normalize": "true",
        "normalize_residues": "false",
        "max_length": "2048",
        "use_chunking": "false",
        "stored": 528294,
        "trained_on_id": None,
        "trained_on_source": None,
        "trained_on_version": None,
        "trained_on_date": None,
        "in_use": True,
        "producible": True,
    }
    row.update(over)
    return row


def _record(substrates: list[dict[str, Any]], **over: Any) -> dict[str, list[dict[str, Any]]]:
    record: dict[str, list[dict[str, Any]]] = {
        "substrates": substrates,
        "corpus": [{"sequences": 528294}],
        "prediction_sets": [],
        "results": [],
    }
    record.update(over)
    return record


def test_counts_restate_the_node_ratio() -> None:
    reps = build_representations(
        _record(
            [
                _substrate(),
                _substrate(id="ec-2", label="ankh_base", in_use=False, producible=True),
                _substrate(id="ec-3", label="unbuilt", in_use=False, producible=False, stored=0),
            ]
        )
    )
    assert reps["total"] == 3
    # Built excludes the one with no stored embedding, exactly as the node's
    # denominator does.
    assert reps["built"] == 2
    assert reps["retrieved"] == 1
    assert reps["corpus_sequences"] == 528294


def test_states_are_assigned_and_grouped_in_order() -> None:
    reps = build_representations(
        _record(
            [
                _substrate(id="ec-3", label="zeta", in_use=False, producible=False, stored=0),
                _substrate(id="ec-2", label="ankh_base", in_use=False, producible=True),
                _substrate(id="ec-1", label="esm2_650m"),
            ]
        )
    )
    assert [r["state"] for r in reps["rows"]] == ["retrieved", "built", "unbuilt"]
    assert [r["label"] for r in reps["rows"]] == ["esm2_650m", "ankh_base", "zeta"]


def test_an_unbuilt_configuration_is_listed_rather_than_dropped() -> None:
    """It is outside the denominator and still on the page.

    Dropping it would hide the difference between a level nobody chose and a
    level that does not exist, which is the difference the state exists to say.
    """
    reps = build_representations(
        _record(
            [_substrate(id="ec-3", label="never-built", in_use=False, producible=False, stored=0)]
        )
    )
    assert reps["built"] == 0
    assert [r["label"] for r in reps["rows"]] == ["never-built"]
    assert reps["rows"][0]["state"] == "unbuilt"
    assert reps["rows"][0]["embeddings_stored"] == 0


def test_absent_param_count_stays_absent_and_orders_nothing() -> None:
    reps = build_representations(
        _record(
            [
                _substrate(id="ec-1", label="big", param_count=None, in_use=False),
                _substrate(id="ec-2", label="small", param_count=7841868, in_use=False),
            ]
        )
    )
    assert [r["param_count"] for r in reps["rows"]] == [None, 7841868]
    # Alphabetical inside the group, not by the size column.
    assert [r["label"] for r in reps["rows"]] == ["big", "small"]


def test_coverage_is_the_stored_rows_over_the_corpus() -> None:
    reps = build_representations(_record([_substrate(stored=528234)]))
    row = reps["rows"][0]
    assert row["embeddings_stored"] == 528234
    assert row["coverage"] is not None
    assert row["coverage"] < 1.0


def test_coverage_is_refused_when_a_row_is_not_a_sequence() -> None:
    reps = build_representations(_record([_substrate(use_chunking="true")]))
    assert reps["rows"][0]["use_chunking"] is True
    assert reps["rows"][0]["coverage"] is None


def test_coverage_is_refused_when_the_corpus_is_unknown() -> None:
    reps = build_representations(_record([_substrate()], corpus=[]))
    assert reps["corpus_sequences"] is None
    assert reps["rows"][0]["coverage"] is None


def test_a_fitted_encoding_names_the_release_it_was_fitted_against() -> None:
    reps = build_representations(
        _record(
            [
                _substrate(
                    id="ec-4",
                    label="rung2-dense",
                    in_use=False,
                    trained_on_id="as-220",
                    trained_on_source="goa",
                    trained_on_version="220",
                    trained_on_date="2024-04-16",
                ),
                _substrate(id="ec-1", label="esm2_650m", in_use=False),
            ]
        )
    )
    by_label = {r["label"]: r for r in reps["rows"]}
    assert by_label["rung2-dense"]["trained_on"] == {
        "annotation_set_id": "as-220",
        "source": "goa",
        "version": "220",
        "published_at": "2024-04-16",
    }
    # Null is "not fitted", a statement about a pretrained backbone rather than
    # a value nobody recorded.
    assert by_label["esm2_650m"]["trained_on"] is None


def test_use_is_counted_off_the_rows_the_graph_already_read() -> None:
    reps = build_representations(
        _record(
            [_substrate(), _substrate(id="ec-2", label="ankh_base", in_use=False)],
            prediction_sets=[
                {"id": "ps-1", "embedding_config_id": "ec-1"},
                {"id": "ps-2", "embedding_config_id": "ec-1"},
            ],
            results=[
                {"id": "r-1", "embedding_config_id": "ec-1"},
                {"id": "r-2", "embedding_config_id": "ec-1"},
                {"id": "r-3", "embedding_config_id": "ec-1"},
            ],
        )
    )
    by_label = {r["label"]: r for r in reps["rows"]}
    assert (by_label["esm2_650m"]["prediction_sets"], by_label["esm2_650m"]["results"]) == (2, 3)
    assert (by_label["ankh_base"]["prediction_sets"], by_label["ankh_base"]["results"]) == (0, 0)


def test_an_empty_record_reports_nothing_rather_than_inventing_a_row() -> None:
    reps = build_representations({})
    assert reps == {
        "corpus_sequences": None,
        "total": 0,
        "built": 0,
        "retrieved": 0,
        "rows": [],
    }
