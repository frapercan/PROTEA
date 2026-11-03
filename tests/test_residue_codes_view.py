"""``GET /embeddings/residue-codes``: what the embeddings page reads while a residue pass runs (ADR-D51).

WHY THIS EXISTS. A corpus pass of ``encode_residue_vocabulary`` runs for most of a
day on the GPU node. Its coordinator's row says how the batches were dispatched,
and on 2026-11-03 a coordinator that ran the code before #1091 closed SUCCEEDED
two seconds after it started, so the jobs page drew a full green bar over a pass
at 22 %. The index is the only place that knows how much of the corpus has a
code, and this endpoint is how the page reads it.

These tests pin the route's wiring and its cache, and the arithmetic the page
shows (coverage, what is left, the hours it takes at the last hour's pace). The
counts themselves are SQL and are pinned against Postgres in
``test_residue_codes_view_pg.py``.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.cache import invalidate as _cache_invalidate
from protea.api.routers import embeddings as embeddings_router
from protea.services.residue_codes_service import _reach, _summary


@pytest.fixture(autouse=True)
def _reset_router_cache():
    _cache_invalidate()
    yield
    _cache_invalidate()


def _client() -> TestClient:
    app = FastAPI()
    app.state.session_factory = MagicMock()
    app.state.amqp_url = "amqp://guest:guest@localhost/"
    app.include_router(embeddings_router.router)
    return TestClient(app)


SUMMARIES = [
    {
        "embedding_config_id": "22166300-a178-599e-835c-afe1c007feb5",
        "name": "residue-vocabulary:06fed75ca76d17be:f3a7fe16",
        "index": {"sequences": 150480, "residues": 52000000, "shards": 1172},
        "coverage": 0.22,
    }
]


def test_the_route_returns_what_the_service_computed() -> None:
    with patch.object(
        embeddings_router, "_compute_residue_codes", return_value=SUMMARIES
    ) as compute:
        response = _client().get("/embeddings/residue-codes")
    assert response.status_code == 200
    assert response.json() == SUMMARIES
    compute.assert_called_once()


def test_a_page_that_polls_does_not_scan_the_index_on_every_request() -> None:
    with patch.object(
        embeddings_router, "_compute_residue_codes", return_value=SUMMARIES
    ) as compute:
        client = _client()
        for _ in range(3):
            assert client.get("/embeddings/residue-codes").status_code == 200
    assert compute.call_count == 1


def test_the_source_sizes_are_counted_once_while_the_counts_refresh() -> None:
    """The source size is most of the cost and a constant during a pass: an hour's cache,
    while the counts that move keep their 30 seconds."""
    from protea.api.cache import invalidate

    with (
        patch.object(embeddings_router, "_compute_residue_source_sizes", return_value={}) as sizes,
        patch.object(
            embeddings_router, "residue_code_summaries", return_value=SUMMARIES
        ) as summaries,
    ):
        client = _client()
        assert client.get("/embeddings/residue-codes").status_code == 200
        invalidate(embeddings_router.RESIDUE_CODES_CACHE_KEY)
        assert client.get("/embeddings/residue-codes").status_code == 200
    assert summaries.call_count == 2
    assert sizes.call_count == 1


def test_no_residue_vocabulary_is_an_empty_list_not_an_error() -> None:
    with patch.object(embeddings_router, "_compute_residue_codes", return_value=[]):
        response = _client().get("/embeddings/residue-codes")
    assert response.status_code == 200
    assert response.json() == []


def _index(sequences: int, last_hour: int) -> dict:
    return {
        "sequences": sequences,
        "residues": 0,
        "entries": 0,
        "shards": 0,
        "first_written_at": None,
        "last_written_at": None,
        "sequences_last_hour": last_hour,
    }


def test_reach_reads_coverage_remaining_and_hours_at_the_last_hours_pace() -> None:
    reach = _reach(_index(150_480, 36_000), 683_887)
    assert reach["coverage"] == pytest.approx(150_480 / 683_887)
    assert reach["remaining"] == 683_887 - 150_480
    assert reach["eta_hours"] == pytest.approx((683_887 - 150_480) / 36_000)


def test_reach_says_nothing_it_cannot_know() -> None:
    """No source size: no coverage. No rows in the last hour: no pace, so no estimate."""
    assert _reach(_index(10, 0), None) == {"coverage": None, "remaining": None, "eta_hours": None}
    stalled = _reach(_index(10, 0), 100)
    assert stalled["remaining"] == 90
    assert stalled["eta_hours"] is None


def test_a_finished_pass_has_nothing_left_and_no_estimate() -> None:
    done = _reach(_index(683_887, 0), 683_887)
    assert done["coverage"] == 1.0
    assert done["remaining"] == 0
    assert done["eta_hours"] is None


def test_a_configuration_without_codes_is_reported_with_zero_counts() -> None:
    source_id = uuid.uuid4()
    config = SimpleNamespace(
        id=uuid.uuid4(),
        display_name=None,
        model_name="residue-vocabulary:0123456789abcdef:f3a7fe16",
        description="per-residue code",
        layer_indices=[2, 10, 22, 40, 48],
        created_at=datetime(2026, 11, 3, tzinfo=UTC),
        derived_from_embedding_config_id=source_id,
    )
    source = SimpleNamespace(display_name="Ankh base", model_name="ElnaggarLab/ankh-base")
    summary = _summary(config, None, {source_id: source}, {source_id: 683_887}, None)
    assert summary["name"] == config.model_name
    assert summary["index"]["sequences"] == 0
    assert summary["source"] == {
        "embedding_config_id": str(source_id),
        "name": "Ankh base",
        "sequences": 683_887,
    }
    assert summary["coverage"] == 0.0
    assert summary["remaining"] == 683_887
    assert summary["job"] is None
