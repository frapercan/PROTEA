"""Equality proof for the single-pair reranked-score endpoint.

The one hard correctness rule for ``GET /scoring/prediction-sets/{id}/score``
is that the value it returns for a ``(accession, go_term)`` pair is the
SAME number the bulk ``rerank.tsv`` stream emits for that row. Both paths
funnel through the identical :func:`score_predictions_with_reranker`
booster pass; the single-pair helper simply selects one row. These tests
drive both paths over one mocked session + booster and assert the floats
match exactly (and that the value round-trips through the TSV's ``.6f``
rendering byte-for-byte).

No real database is touched: the DB session and the LightGBM booster are
mocked, mirroring ``tests/test_scoring_router.py``.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch
from uuid import uuid4

import numpy as np
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from protea.api.routers.scoring import router
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.reranker_model import RerankerModel
from protea.services.scoring_service import (
    iter_reranked_predictions_tsv,
    score_predictions_with_reranker,
    score_single_pair_with_reranker,
)

# ---------------------------------------------------------------------------
# Fixtures: a small prediction set + a deterministic (mocked) booster
# ---------------------------------------------------------------------------

# Three distinct (accession, go_id, aspect) rows and the raw scores the
# mocked booster returns for them, in the SAME row order the materialiser
# yields them. These are the ground truth both paths must agree on.
_ROWS = [
    ("P12345", "GO:0003674", "F"),
    ("P12345", "GO:0008150", "P"),
    ("Q99999", "GO:0005575", "C"),
]
_SCORES = np.array([0.912345, 0.734500, 0.418000])


def _make_pred(accession: str) -> MagicMock:
    pred = MagicMock()
    pred.protein_accession = accession
    pred.distance = 0.1
    pred.ref_protein_accession = "R00001"
    pred.qualifier = "enables"
    pred.evidence_code = "IDA"
    # Remaining feature columns are irrelevant: the booster is mocked.
    for attr in (
        "identity_nw", "similarity_nw", "alignment_score_nw", "gaps_pct_nw",
        "alignment_length_nw", "identity_sw", "similarity_sw", "alignment_score_sw",
        "gaps_pct_sw", "alignment_length_sw", "length_query", "length_ref",
        "query_taxonomy_id", "ref_taxonomy_id", "taxonomic_lca", "taxonomic_distance",
        "taxonomic_common_ancestors", "taxonomic_relation", "vote_count", "k_position",
        "go_term_frequency", "ref_annotation_density", "neighbor_distance_std",
    ):
        setattr(pred, attr, 0.0)
    return pred


@contextmanager
def _mock_scope(session):
    yield session


def _wire_session(session, set_id, reranker_id):
    """Route session.get + the materialiser query to the fixture rows."""

    def get_side(model, _id):
        if model is PredictionSet:
            return MagicMock()
        if model is RerankerModel:
            rm = MagicMock(spec=RerankerModel)
            rm.id = reranker_id
            return rm
        return None

    session.get.side_effect = get_side
    tuples = [(_make_pred(acc), go, aspect) for acc, go, aspect in _ROWS]
    (
        session.query.return_value.join.return_value.filter.return_value.yield_per.return_value
    ) = tuples


@pytest.fixture()
def wired():
    """A mocked session + patched booster shared by both scoring paths."""
    session = MagicMock()
    set_id, reranker_id = uuid4(), uuid4()
    _wire_session(session, set_id, reranker_id)
    with (
        patch(
            "protea.services.scoring_service.load_booster",
            return_value=MagicMock(),
        ),
        patch(
            "protea.services._scoring_pipeline_helpers._reranker_predict",
            return_value=_SCORES,
        ),
    ):
        yield session, set_id, reranker_id


# ---------------------------------------------------------------------------
# The equality proof
# ---------------------------------------------------------------------------


class TestSinglePairEqualsBulk:
    def test_single_pair_matches_bulk_tsv_for_every_row(self, wired):
        session, set_id, reranker_id = wired

        # Bulk path: exactly what GET /…/rerank.tsv streams.
        df = score_predictions_with_reranker(
            session, prediction_set_id=set_id, reranker_id=reranker_id
        )
        tsv = b"".join(iter_reranked_predictions_tsv(df)).decode()
        lines = tsv.strip().split("\n")
        header = lines[0].split("\t")
        acc_i = header.index("protein_accession")
        go_i = header.index("go_id")
        score_i = header.index("reranker_score")
        bulk_cell = {
            (r[acc_i], r[go_i]): r[score_i]
            for r in (line.split("\t") for line in lines[1:])
        }

        # Single-pair path for the same triples.
        for accession, go_term, _aspect in _ROWS:
            single = score_single_pair_with_reranker(
                session,
                prediction_set_id=set_id,
                reranker_id=reranker_id,
                accession=accession,
                go_term=go_term,
            )
            # 1. Exact float identity against the bulk DataFrame value.
            df_val = float(
                df[(df["protein_accession"] == accession) & (df["go_id"] == go_term)][
                    "reranker_score"
                ].iloc[0]
            )
            assert single["reranker_score"] == df_val
            # 2. Byte-for-byte identical to the TSV cell rendering.
            assert f"{single['reranker_score']:.6f}" == bulk_cell[(accession, go_term)]

    def test_missing_pair_raises_not_found(self, wired):
        from protea.services.scoring_service import EntityNotFoundError

        session, set_id, reranker_id = wired
        with pytest.raises(EntityNotFoundError):
            score_single_pair_with_reranker(
                session,
                prediction_set_id=set_id,
                reranker_id=reranker_id,
                accession="P12345",
                go_term="GO:9999999",
            )


# ---------------------------------------------------------------------------
# Router surface: the endpoint returns the same value over HTTP
# ---------------------------------------------------------------------------


class TestSinglePairEndpoint:
    def _client(self, session):
        app = FastAPI()
        app.state.session_factory = MagicMock()
        app.include_router(router)
        return app

    def test_endpoint_returns_bulk_value(self, wired):
        session, set_id, reranker_id = wired
        df = score_predictions_with_reranker(
            session, prediction_set_id=set_id, reranker_id=reranker_id
        )
        expected = float(
            df[(df["protein_accession"] == "P12345") & (df["go_id"] == "GO:0003674")][
                "reranker_score"
            ].iloc[0]
        )

        app = self._client(session)
        with patch(
            "protea.api.routers.scoring.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            with TestClient(app) as c:
                resp = c.get(
                    f"/scoring/prediction-sets/{set_id}/score"
                    f"?reranker_id={reranker_id}&accession=P12345&go_term=GO:0003674"
                )
        assert resp.status_code == 200
        body = resp.json()
        assert body["protein_accession"] == "P12345"
        assert body["go_id"] == "GO:0003674"
        assert body["reranker_score"] == expected

    def test_endpoint_missing_pair_returns_404(self, wired):
        session, set_id, reranker_id = wired
        app = self._client(session)
        with patch(
            "protea.api.routers.scoring.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            with TestClient(app) as c:
                resp = c.get(
                    f"/scoring/prediction-sets/{set_id}/score"
                    f"?reranker_id={reranker_id}&accession=P12345&go_term=GO:9999999"
                )
        assert resp.status_code == 404

    def test_endpoint_prediction_set_not_found(self, wired):
        session, set_id, reranker_id = wired
        session.get.side_effect = None
        session.get.return_value = None
        app = self._client(session)
        with patch(
            "protea.api.routers.scoring.session_scope",
            side_effect=lambda _: _mock_scope(session),
        ):
            with TestClient(app) as c:
                resp = c.get(
                    f"/scoring/prediction-sets/{uuid4()}/score"
                    f"?reranker_id={uuid4()}&accession=P12345&go_term=GO:0003674"
                )
        assert resp.status_code == 404
