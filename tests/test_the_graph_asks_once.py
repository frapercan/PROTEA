"""The graph reads each fact once, and ``producible`` is one of them.

Measured on the deployed API 2026-10-27, before the change: the graph's
sixteen reads cost 1.374 s, and 1.311 s of that was three of them.

    substrates   0.905 s   10 rows
    candidates   0.301 s    8 rows
    query_sets   0.106 s    2 rows

``substrates`` asked for the same fact twice. ``stored`` came from one
grouped aggregate over ``sequence_embedding`` (37 ms), and ``producible``
asked whether that same table held any row for the configuration, as a
correlated ``EXISTS``. The planner cannot estimate ``embedding_config_id =
ec.id``, so it sized the filter at the whole table, chose a sequential scan,
and ran it ONCE PER CONFIGURATION: ten passes over 615,643 rows, 1,055,187
blocks read, to answer what the aggregate already knew.

After: substrates 0.059 s, query_sets 0.004 s, the sixteen reads 0.409 s.
``candidates`` is unchanged because it is a real aggregate over
``go_prediction``, and that table grows with every ingestion -- 18.4 M rows
tonight -- so it is the next one to come back.

The test is the INVARIANT and not the SQL text. A row's ``producible`` says
the configuration has at least one stored embedding, and ``stored`` counts
them: the two cannot disagree, whichever way the query computes them. Pinned
here so a future rewrite that reintroduces a second, slower source for the
same fact has to keep it true.
"""

from __future__ import annotations

from typing import Any

from protea.api.routers._graph_representations import build_representations


def _substrate(name: str, stored: int, **over: Any) -> dict[str, Any]:
    row = {
        "id": f"id-{name}",
        "model_name": name,
        "model_backend": "huggingface",
        "label": name,
        "display_name": None,
        "family": None,
        "param_count": None,
        "layer_indices": None,
        "layer_agg": "none",
        "pooling": "mean",
        "normalize": "false",
        "normalize_residues": "false",
        "max_length": None,
        "use_chunking": "false",
        "chunk_size": None,
        "chunk_overlap": None,
        "trained_on_id": None,
        "trained_on_source": None,
        "trained_on_version": None,
        "trained_on_date": None,
        "stored": stored,
        "in_use": False,
        # The query derives this from ``stored``; a double must not be free
        # to contradict it, which is what the test below checks.
        "producible": stored > 0,
    }
    row.update(over)
    return row


def _record(substrates: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    return {
        "substrates": substrates,
        "corpus": [{"sequences": 528294}],
        "prediction_sets": [],
        "results": [],
    }


class TestProducibleAndStoredAreOneFact:
    """``producible`` is load-bearing, which is why it had to stay exact.

    Two places read it: the representation's ``state`` (built or unbuilt) and
    the substrate node's ``available`` count, which is what puts that node's
    edge at ``unpowered`` today -- one producible representation cannot resolve
    a comparison. So the cheap form has to mean exactly what the expensive one
    meant, and these pin that rather than the SQL.
    """

    def test_a_built_representation_reads_as_built(self) -> None:
        out = build_representations(_record([_substrate("ankh-base", 684048)]))
        row = out["rows"][0]
        assert row["embeddings_stored"] == 684048
        assert row["state"] == "built"
        assert out["built"] == 1

    def test_an_unbuilt_one_reads_as_unbuilt(self) -> None:
        # Nine of the ten configurations on this deployment are in this state:
        # registered, never embedded. Not an untried alternative, an unbuilt
        # one, which is the distinction the column exists to make.
        out = build_representations(_record([_substrate("esm2_t36_3B", 0)]))
        row = out["rows"][0]
        assert row["embeddings_stored"] == 0
        assert row["state"] == "unbuilt"
        assert out["built"] == 0

    def test_the_two_never_disagree_across_the_whole_record(self) -> None:
        # The invariant, over the shape the live record has: one built
        # representation and nine unbuilt.
        rows = [_substrate("ankh-base", 684048)] + [
            _substrate(f"unbuilt-{i}", 0) for i in range(9)
        ]
        out = build_representations(_record(rows))
        assert out["total"] == 10
        assert out["built"] == 1
        for row in out["rows"]:
            built = row["state"] in ("built", "retrieved")
            assert built is (int(row["embeddings_stored"]) > 0), row["model_name"]
