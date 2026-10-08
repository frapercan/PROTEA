"""The accession prefilter changes how fast a GOA load runs and nothing else.

``_stream_and_store`` hands the GOA plugin an ``accept`` predicate, so a line
naming a protein outside the corpus is rejected on its raw columns and never
becomes a ``GoaAnnotationRecord``. Everything a load leaves behind has to come
out identical: the rows it inserts, the events it emits, the commits it makes
and every counter of its report. Those counters are the evidence that a release
loaded correctly (pages, lines, inserted, the three rejection buckets), so a
prefilter that moved one line from one page to the next would change what the
evidence says without changing a single row.

So each case runs the same GAF three ways and compares all four outputs:

- ``reference``: the loop as it stood before the prefilter, copied below;
- ``off``: the current loop with ``prefilter=False``;
- ``on``: the current loop as production runs it.

No DB and no network: the HTTP body is a byte string, and the session is a
mock whose ``execute`` calls are compiled back into the rows they insert.
"""

from __future__ import annotations

import io
import random
import uuid
from typing import Any
from unittest.mock import MagicMock, patch

import protea_sources.goa as goa_plugin_module
import pytest
from sqlalchemy.dialects import postgresql

from protea.core.operations._goa_load_report import _GoaPageTotals
from protea.core.operations.load_goa_annotations import (
    LoadGOAAnnotationsOperation,
    LoadGOAAnnotationsPayload,
    _GoaStoreCtx,
)

_ANNOTATION_SET_ID = uuid.UUID("00000000-0000-0000-0000-00000000a001")


def _gaf_line(
    accession: str,
    go_id: str = "GO:0000001",
    qualifier: str = "enables",
    evidence: str = "IDA",
    db_ref: str = "PMID:1",
    with_from: str = "",
    date: str = "20240101",
    assigned_by: str = "UniProt",
) -> str:
    cols = ["UniProtKB"] + [""] * 16
    cols[1] = accession
    cols[3] = qualifier
    cols[4] = go_id
    cols[5] = db_ref
    cols[6] = evidence
    cols[7] = with_from
    cols[13] = date
    cols[14] = assigned_by
    return "\t".join(cols)


def _reference_stream_and_store(
    op: LoadGOAAnnotationsOperation,
    session: Any,
    p: LoadGOAAnnotationsPayload,
    store_ctx: _GoaStoreCtx,
    emit: Any,
) -> _GoaPageTotals:
    """``_stream_and_store`` before the prefilter, from develop at a38486d.

    Kept verbatim on purpose, so this file compares against the loop every
    release so far was loaded with, not against a second reading of the new code.
    ``_flush_page`` is the current one; with ``dropped`` left at its default of
    0 it does what it did then.
    """
    totals = _GoaPageTotals()
    buffer: list[Any] = []
    for record in op._stream_gaf(p, emit):
        totals.lines += 1
        if p.total_limit is not None and totals.inserted >= p.total_limit:
            emit(
                "load_goa_annotations.limit_reached",
                None,
                {"total_limit": p.total_limit},
                "warning",
            )
            break
        buffer.append(record)
        if len(buffer) >= p.page_size:
            op._flush_page(session, buffer, store_ctx, totals, emit)
            if p.commit_every_page:
                session.commit()
    if buffer:
        op._flush_page(session, buffer, store_ctx, totals, emit=None)
    return totals


def _inserted_rows(session: MagicMock) -> list[dict[str, Any]]:
    """The rows of every INSERT the load executed, in execution order."""
    rows = []
    for call in session.execute.call_args_list:
        stmt = call.args[0]
        rows.append(stmt.compile(dialect=postgresql.dialect()).params)
    return rows


def _run(
    mode: str,
    gaf_text: str,
    universe: set[str],
    go_terms: dict[str, int],
    *,
    page_size: int,
    total_limit: int | None = None,
    commit_every_page: bool = True,
) -> dict[str, Any]:
    op = LoadGOAAnnotationsOperation()
    session = MagicMock()
    events: list[tuple[str, Any, str]] = []

    def emit(event: str, _message: Any, fields: Any, level: str) -> None:
        events.append((event, dict(fields) if fields else fields, level))

    payload: dict[str, Any] = {
        "ontology_snapshot_id": str(uuid.uuid4()),
        "gaf_url": "https://example.com/goa_uniprot_all.gaf",
        "source_version": "v1",
        "page_size": page_size,
        "commit_every_page": commit_every_page,
    }
    if total_limit is not None:
        payload["total_limit"] = total_limit
    p = LoadGOAAnnotationsPayload(**payload)
    store_ctx = _GoaStoreCtx(
        annotation_set_id=_ANNOTATION_SET_ID,
        admissible_accessions=set(universe),
        go_term_map=dict(go_terms),
    )

    resp = MagicMock()
    resp.raw = io.BytesIO(gaf_text.encode("utf-8"))
    resp.raise_for_status = MagicMock()
    with patch("protea_sources.goa.requests.get", return_value=resp):
        if mode == "reference":
            totals = _reference_stream_and_store(op, session, p, store_ctx, emit)
        else:
            totals = op._stream_and_store(session, p, store_ctx, emit, prefilter=(mode == "on"))

    return {
        "totals": totals,
        "events": events,
        "commits": session.commit.call_count,
        "rows": _inserted_rows(session),
    }


def _assert_same_load(
    gaf_text: str, universe: set[str], go_terms: dict[str, int], **kw: Any
) -> None:
    reference = _run("reference", gaf_text, universe, go_terms, **kw)
    off = _run("off", gaf_text, universe, go_terms, **kw)
    on = _run("on", gaf_text, universe, go_terms, **kw)
    for key in ("totals", "events", "commits", "rows"):
        assert off[key] == reference[key], f"prefilter=False changed {key}"
        assert on[key] == reference[key], f"the prefilter changed {key}"


# ---------------------------------------------------------------------------
# A GAF built to hit every edge the page arithmetic has
# ---------------------------------------------------------------------------

_OURS = {"P00001", "P00002", "P00003"}
_GO = {"GO:0000001": 1, "GO:0000002": 2}


def _edge_gaf() -> str:
    lines = [
        "!gaf-version: 2.2",
        "",
        "short\tline",
        _gaf_line("P00001"),
        _gaf_line("P00001"),  # exact repeat: duplicate_in_batch, or ON CONFLICT
        _gaf_line(" P00002 "),  # padded: the record strips it, so must the filter
        _gaf_line(""),  # empty accession: not_in_universe
        _gaf_line("   "),  # blank accession: not_in_universe
        _gaf_line("P00003", go_id="GO:9999999"),  # ours, unknown term
    ]
    # A run of foreign proteins long enough to fill whole pages on its own.
    lines += [_gaf_line(f"Q{n:05d}") for n in range(25)]
    lines += [
        _gaf_line("P00002", evidence="IEA"),
        "!a comment between records",
        _gaf_line("Q99999"),
        _gaf_line("P00003", db_ref="PMID:2"),
        _gaf_line("P00001"),  # a repeat in a LATER page: left to ON CONFLICT
    ]
    lines += [_gaf_line(f"Q{n:05d}") for n in range(25, 31)]  # foreign tail
    return "\n".join(lines) + "\n"


@pytest.mark.parametrize("page_size", [1, 2, 3, 7, 10, 40, 10000, 200000])
@pytest.mark.parametrize("total_limit", [None, 1, 2, 4])
@pytest.mark.parametrize("commit_every_page", [True, False])
def test_edge_gaf_loads_identically(
    page_size: int, total_limit: int | None, commit_every_page: bool
) -> None:
    _assert_same_load(
        _edge_gaf(),
        _OURS,
        _GO,
        page_size=page_size,
        total_limit=total_limit,
        commit_every_page=commit_every_page,
    )


def test_a_page_of_foreign_proteins_still_flushes_commits_and_reports() -> None:
    """Pinned on its own because it is the case the prefilter could most easily
    skip: a page with nothing to insert. Without the prefilter it was a page of
    records, flushed, committed and reported like any other, and the
    ``page_done`` sequence is the progress record of a load."""
    gaf = "\n".join([_gaf_line(f"Q{n:05d}") for n in range(6)] + [_gaf_line("P00001")]) + "\n"
    on = _run("on", gaf, _OURS, _GO, page_size=3)
    pages = [
        fields for event, fields, _ in on["events"] if event == "load_goa_annotations.page_done"
    ]
    assert [p["total_lines"] for p in pages] == [3, 6]
    assert [p["total_inserted"] for p in pages] == [0, 0]
    assert on["commits"] == 2
    assert on["totals"].pages == 3
    assert on["totals"].lines == 7
    assert on["totals"].not_in_universe == 6
    assert on["totals"].inserted == 1


# ---------------------------------------------------------------------------
# Random GAFs: shapes nobody thought to write down
# ---------------------------------------------------------------------------


def _random_gaf(rng: random.Random) -> tuple[str, set[str], dict[str, int]]:
    ours = [f"P{n:05d}" for n in range(8)]
    theirs = [f"Q{n:05d}" for n in range(60)]
    go_known = {f"GO:{n:07d}": n + 1 for n in range(5)}
    go_unknown = ["GO:0000100", "GO:0000101"]

    def record(accession: str) -> str:
        return _gaf_line(
            accession,
            go_id=rng.choice(list(go_known) + go_unknown)
            if rng.random() < 0.15
            else rng.choice(list(go_known)),
            qualifier=rng.choice(["enables", "involved_in", ""]),
            evidence=rng.choice(["IDA", "IEA", "IMP"]),
            db_ref=rng.choice(["PMID:1", "PMID:2", "GO_REF:0000033", ""]),
            with_from=rng.choice(["", "InterPro:IPR000001"]),
            assigned_by=rng.choice(["UniProt", "InterPro"]),
        )

    lines: list[str] = []
    target = rng.randint(50, 900)
    while len(lines) < target:
        roll = rng.random()
        if roll < 0.03:
            lines.append(f"!comment {rng.random()}")
        elif roll < 0.05:
            lines.append("")
        elif roll < 0.07:
            lines.append("too\tfew\tcolumns")
        elif roll < 0.12:
            lines.extend(record(rng.choice(theirs)) for _ in range(rng.randint(10, 120)))
        else:
            if rng.random() < 0.25:
                accession = rng.choice(ours)
            elif rng.random() < 0.03:
                accession = rng.choice(["", " ", "  "])
            else:
                accession = rng.choice(theirs)
            pad = rng.choice(["", "", "", " ", "  "])
            lines.append(record(f"{pad}{accession}{pad[::-1]}"))
    return "\n".join(lines) + "\n", set(ours), go_known


@pytest.mark.parametrize("seed", range(60))
def test_random_gaf_loads_identically(seed: int) -> None:
    rng = random.Random(seed)
    gaf, universe, go_terms = _random_gaf(rng)
    _assert_same_load(
        gaf,
        universe,
        go_terms,
        page_size=rng.choice([1, 2, 5, 17, 64, 250, 10000]),
        total_limit=rng.choice([None, None, None, 1, 7, 40]),
        commit_every_page=rng.random() < 0.8,
    )


# ---------------------------------------------------------------------------
# And it is actually wired: a rejected line builds no record
# ---------------------------------------------------------------------------


def test_the_prefilter_builds_records_only_for_the_corpus(monkeypatch: pytest.MonkeyPatch) -> None:
    """The equality tests above would also pass if ``accept`` were never handed
    to the plugin, and the load would quietly lose the whole point. So count
    the records the plugin builds."""
    built: list[str] = []
    real_record = goa_plugin_module.GoaAnnotationRecord

    def counting_record(**fields: Any) -> Any:
        built.append(fields["accession"])
        return real_record(**fields)

    monkeypatch.setattr(goa_plugin_module, "GoaAnnotationRecord", counting_record)
    gaf = _edge_gaf()

    _run("off", gaf, _OURS, _GO, page_size=7)
    every_record = len(built)
    built.clear()
    _run("on", gaf, _OURS, _GO, page_size=7)

    assert built, "the prefilter rejected the corpus too"
    assert all(accession.strip() in _OURS for accession in built)
    assert len(built) < every_record
    assert len(built) == sum(1 for a in _edge_gaf_accessions() if a.strip() in _OURS)


def _edge_gaf_accessions() -> list[str]:
    return [
        line.split("\t")[1]
        for line in _edge_gaf().splitlines()
        if line and not line.startswith("!") and len(line.split("\t")) >= 15
    ]
