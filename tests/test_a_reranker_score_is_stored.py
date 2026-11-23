"""A reranker that scores candidates at prediction time stores its score.

The k-nearest-neighbour method can score its candidates with a reranker as it
predicts. The score landed on each candidate and was then dropped: no column
took it, so a run that named a reranker stored the same rows as a run that did
not, and the record said otherwise. It is stored now as the row's
``method_score``, the score the evaluator reads. The InterPro BP graft, which
combines InterPro's evidence with that score, runs only when there is one, and
the record says when it ran.
"""

from __future__ import annotations

import math
import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np

from protea.core.method_seal import METHOD_IDENTITY_FIELDS
from protea.core.operations import predict_go_terms as pgt
from protea.core.operations.predict_go_terms import _receipt
from protea.core.operations.predict_go_terms._batch_op import PredictGOTermsBatchOperation
from protea.core.operations.predict_go_terms._common import _row_from_prediction
from tests.test_predict_go_terms_coverage import _emit_capture, _payload
from tests.test_prediction_run_receipt import _payload as _receipt_payload

_PIPELINE = "protea.core.operations.predict_go_terms._post_knn_pipeline"


def _candidate(**over: Any) -> dict[str, Any]:
    return {
        "protein_accession": "Q1",
        "go_term_id": 7,
        "ref_protein_accession": "D1",
        "distance": 0.2,
        **over,
    }


class TestTheRowKeepsTheScore:
    def test_the_reranker_score_is_the_method_score(self) -> None:
        row = _row_from_prediction(_candidate(reranker_score=0.83), uuid.uuid4())
        assert row["method_score"] == 0.83

    def test_no_reranker_leaves_a_candidate(self) -> None:
        assert _row_from_prediction(_candidate(), uuid.uuid4())["method_score"] is None

    def test_a_score_that_is_not_a_number_is_not_stored_as_one(self) -> None:
        row = _row_from_prediction(_candidate(reranker_score=math.nan), uuid.uuid4())
        assert row["method_score"] is None


def _run(reranker_stats: dict[str, Any] | None) -> tuple[MagicMock, list[tuple[str, dict]]]:
    """Run the post-kNN pipeline with the graft on and a reranker that returns ``reranker_stats``."""
    op = PredictGOTermsBatchOperation()
    ctx = pgt._BatchExecCtx(
        p=_payload(reranker_model_id=str(uuid.uuid4())),
        parent_job_id=uuid.uuid4(),
        prediction_set_id=uuid.uuid4(),
        embedding_config_id=uuid.uuid4(),
        annotation_set_id=uuid.uuid4(),
    )
    knn = pgt._KnnResult(
        prediction_dicts=[_candidate()],
        v6_ctx=None,
        query_batch=pgt._QueryBatch(
            valid_accessions=["Q1"],
            query_embeddings=np.array([[0.1]], dtype=np.float32),
        ),
    )
    emit, events = _emit_capture()
    graft = MagicMock(side_effect=lambda _s, _snap, _acc, dicts, _e: dicts)
    with (
        patch.object(op._reranker_scorer, "apply", return_value=reranker_stats),
        patch(f"{_PIPELINE}._interpro_bp_graft_enabled", return_value=True),
        patch(
            "protea.core.operations.predict_go_terms._interpro_graft.apply_interpro_bp_graft",
            graft,
        ),
        patch(f"{_PIPELINE}._honour_denials", side_effect=lambda _s, _c, rows, _e: rows),
    ):
        op._run_post_knn_pipeline(MagicMock(), ctx, knn, {}, emit)
    return graft, events


class TestTheGraftNeedsAScore:
    def test_it_runs_on_top_of_a_reranker(self) -> None:
        graft, events = _run({"applied": True, "rows": 1})
        graft.assert_called_once()
        assert not [name for name, _ in events if name.endswith("interpro_graft_skipped")]

    def test_without_one_it_is_skipped_and_says_so(self) -> None:
        graft, events = _run(None)
        graft.assert_not_called()
        assert ("predict_go_terms_batch.interpro_graft_skipped") in [name for name, _ in events]


def _tuning(on: bool, weight: float = 0.4) -> Any:
    return SimpleNamespace(
        serve=SimpleNamespace(interpro_bp_graft=on, interpro_bp_graft_weight=weight)
    )


class TestTheRecordSaysWhenTheGraftRan:
    def test_beside_a_reranker_with_the_graft_on(self) -> None:
        payload = _receipt_payload(reranker_model_id=str(uuid.uuid4()))
        with patch("protea.config.tuning.get_tuning", return_value=_tuning(True)):
            receipt = _receipt.run_receipt(payload, uuid.uuid4())
        assert receipt["interpro_bp_graft"] == {"weight": 0.4}

    def test_not_without_a_reranker(self) -> None:
        with patch("protea.config.tuning.get_tuning", return_value=_tuning(True)):
            receipt = _receipt.run_receipt(_receipt_payload(), uuid.uuid4())
        assert "interpro_bp_graft" not in receipt

    def test_not_when_the_graft_is_off(self) -> None:
        payload = _receipt_payload(reranker_model_id=str(uuid.uuid4()))
        with patch("protea.config.tuning.get_tuning", return_value=_tuning(False)):
            receipt = _receipt.run_receipt(payload, uuid.uuid4())
        assert "interpro_bp_graft" not in receipt

    def test_the_graft_is_part_of_the_method(self) -> None:
        assert "interpro_bp_graft" in METHOD_IDENTITY_FIELDS
