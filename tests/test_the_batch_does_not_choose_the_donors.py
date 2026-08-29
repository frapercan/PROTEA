"""A stored neighbourhood is a function of the query and the bank.

It stopped being one. PROTEA pre-searched each aspect at k+1 dropping by
accession, while the method asked for k plus a margin measured from the
bank and dropped by sequence. The method therefore asked for more than
the pre-search had delivered, and its own aspect bank is built from the
narrowed annotation dict, which is the union of the WHOLE chunk's hits.
So each dropped twin was refilled from whatever else that chunk happened
to contain.

Measured on the real pool before the fix: 304 queries received at least
one donor that was not one of their own pre-search hits, 264 of them
having a bank twin, 887 donor slots. Re-running the identical payload
with a different batch composition changed the stored rows for those
queries, and every substituted donor also lost its pair features, because
those are keyed by the pair and built only from the query's own hits.

The property is the one worth testing, not the mechanism: the same query
must get the same donors whoever it shares a batch with.
"""

from __future__ import annotations


def test_the_margin_and_the_drop_agree_between_the_two_searches() -> None:
    """The property that makes the batch irrelevant, checked where it lives.

    Both searches now take their depth from ``extra_neighbours_for`` and
    their drop from ``without_own_sequence``. If they ever stop doing so,
    the method reaches past what the pre-search delivered and refills from
    the rest of the chunk.
    """
    import ast
    import pathlib

    src = pathlib.Path(
        "protea/core/operations/predict_go_terms/_aspect_helpers.py"
    ).read_text()
    tree = ast.parse(src)
    inner = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "_knn_one_aspect"
    )
    body = ast.unparse(inner)
    assert "extra_neighbours_for" in body, (
        "the aspect pre-search no longer takes its depth from the same "
        "function the method uses, so the two horizons can differ again"
    )
    assert "without_own_sequence" in body, (
        "the aspect pre-search no longer drops by sequence, so it hands back "
        "twins the method will drop and then refill from the batch"
    )
    assert "search_k_for" not in body and "without_self" not in body, (
        "the accession-based exclusion is back in the aspect pre-search"
    )


def test_the_sequence_map_is_read_before_the_pre_search() -> None:
    """Building it from the pre-search's hits would be circular, and the
    pre-search needs it to know how deep to ask."""
    import ast
    import pathlib

    src = pathlib.Path(
        "protea/core/operations/predict_go_terms/_aspect_helpers.py"
    ).read_text()
    tree = ast.parse(src)
    fn = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "_build_aspect_adapter_inputs"
    )
    lines = ast.unparse(fn).splitlines()
    keys_at = next(i for i, ln in enumerate(lines) if "_sequence_keys_for" in ln)
    search_at = next(i for i, ln in enumerate(lines) if "_AspectKnnPreSearch.run" in ln)
    assert keys_at < search_at, (
        "the sequence map is built after the pre-search, so the pre-search "
        "cannot use it and the two searches diverge again"
    )
