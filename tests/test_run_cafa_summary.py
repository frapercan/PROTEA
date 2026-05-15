"""Unit tests for the per-job CAFA summary helpers.

These helpers back ``RunCafaEvaluationOperation.summarize_payload``
and must never raise: missing payload keys, bad UUIDs, and absent FK
rows all fall back to UUID prefixes or empty strings.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

from protea.core.operations import _run_cafa_summary as mod


class TestBitsFromPrediction:
    def test_empty_id_returns_empty_list(self) -> None:
        assert mod._bits_from_prediction(None, None) == []
        assert mod._bits_from_prediction("", None) == []

    def test_no_session_returns_uuid_prefix(self) -> None:
        pid = "abcdef0123456789abcdef0123456789"
        bits = mod._bits_from_prediction(pid, None)
        assert bits == [f"pred={pid[:8]}"]

    def test_bad_uuid_falls_back_to_prefix(self) -> None:
        # session.get is never reached because uuid.UUID raises first.
        bits = mod._bits_from_prediction("not-a-uuid", MagicMock())
        assert bits == ["pred=not-a-uu"]

    def test_missing_prediction_set_falls_back_to_prefix(self) -> None:
        session = MagicMock()
        session.get.return_value = None
        pid = str(uuid.uuid4())
        bits = mod._bits_from_prediction(pid, session)
        assert bits == [f"pred={pid[:8]}"]

    def test_prediction_with_no_embedding_config_yields_no_bits(self) -> None:
        session = MagicMock()
        pred = MagicMock()
        pred.embedding_config_id = None
        session.get.return_value = pred
        assert mod._bits_from_prediction(str(uuid.uuid4()), session) == []

    def test_missing_embedding_config_yields_no_bits(self) -> None:
        session = MagicMock()
        pred = MagicMock()
        pred.embedding_config_id = uuid.uuid4()
        # First call returns prediction, second call (for the config) returns None.
        session.get.side_effect = [pred, None]
        assert mod._bits_from_prediction(str(uuid.uuid4()), session) == []

    def test_returns_display_name_when_present(self) -> None:
        session = MagicMock()
        pred = MagicMock()
        pred.embedding_config_id = uuid.uuid4()
        cfg = MagicMock()
        cfg.display_name = "esm2-650m"
        cfg.model_name = "esm2"
        session.get.side_effect = [pred, cfg]
        bits = mod._bits_from_prediction(str(uuid.uuid4()), session)
        assert bits == ["esm2-650m"]

    def test_falls_back_to_model_name_when_no_display_name(self) -> None:
        session = MagicMock()
        pred = MagicMock()
        pred.embedding_config_id = uuid.uuid4()
        cfg = MagicMock()
        cfg.display_name = None
        cfg.model_name = "esm2"
        session.get.side_effect = [pred, cfg]
        bits = mod._bits_from_prediction(str(uuid.uuid4()), session)
        assert bits == ["esm2"]

    def test_falls_back_to_id_prefix_when_no_display_or_model_name(self) -> None:
        session = MagicMock()
        pred = MagicMock()
        pred.embedding_config_id = uuid.uuid4()
        cfg = MagicMock()
        cfg.display_name = None
        cfg.model_name = None
        cfg.id = uuid.uuid4()
        session.get.side_effect = [pred, cfg]
        bits = mod._bits_from_prediction(str(uuid.uuid4()), session)
        assert bits == [str(cfg.id)[:8]]


class TestBitsFromEvaluation:
    def test_empty_id_returns_empty_list(self) -> None:
        assert mod._bits_from_evaluation(None, MagicMock()) == []

    def test_no_session_returns_uuid_prefix(self) -> None:
        eid = "fedcba9876543210"
        bits = mod._bits_from_evaluation(eid, None)
        assert bits == [f"eval={eid[:8]}"]

    def test_bad_uuid_returns_empty_list(self) -> None:
        # The bad-UUID path resolves to ev=None, which yields [].
        assert mod._bits_from_evaluation("not-a-uuid", MagicMock()) == []

    def test_missing_evaluation_set_returns_empty(self) -> None:
        session = MagicMock()
        session.get.return_value = None
        assert mod._bits_from_evaluation(str(uuid.uuid4()), session) == []

    def test_renders_old_to_new_version(self) -> None:
        session = MagicMock()
        ev = MagicMock()
        ev.old_source_version = "2024-01"
        ev.new_source_version = "2024-06"
        session.get.return_value = ev
        bits = mod._bits_from_evaluation(str(uuid.uuid4()), session)
        assert bits == ["eval=2024-01→2024-06"]

    def test_missing_versions_show_as_question_mark(self) -> None:
        session = MagicMock()
        ev = MagicMock()
        ev.old_source_version = None
        ev.new_source_version = None
        session.get.return_value = ev
        bits = mod._bits_from_evaluation(str(uuid.uuid4()), session)
        assert bits == ["eval=?→?"]


class TestBitsFromScoring:
    def test_empty_id_returns_empty_list(self) -> None:
        assert mod._bits_from_scoring(None, MagicMock()) == []

    def test_no_session_returns_uuid_prefix(self) -> None:
        sid = "0123456789abcdef"
        bits = mod._bits_from_scoring(sid, None)
        assert bits == [f"scoring={sid[:8]}"]

    def test_bad_uuid_returns_empty(self) -> None:
        assert mod._bits_from_scoring("not-uuid", MagicMock()) == []

    def test_missing_scoring_config_returns_empty(self) -> None:
        session = MagicMock()
        session.get.return_value = None
        assert mod._bits_from_scoring(str(uuid.uuid4()), session) == []

    def test_returns_scoring_name(self) -> None:
        session = MagicMock()
        sc = MagicMock()
        sc.name = "default-bp"
        session.get.return_value = sc
        bits = mod._bits_from_scoring(str(uuid.uuid4()), session)
        assert bits == ["scoring=default-bp"]


class TestBitsFromPayloadFlags:
    def test_no_flags_returns_empty(self) -> None:
        assert mod._bits_from_payload_flags({}) == []

    def test_reranker_flag_present(self) -> None:
        bits = mod._bits_from_payload_flags({"reranker_model_id": "id"})
        assert bits == ["+reranker"]

    def test_max_distance_flag_present(self) -> None:
        bits = mod._bits_from_payload_flags({"max_distance": 0.45})
        assert bits == ["max_d=0.45"]

    def test_max_distance_zero_still_renders(self) -> None:
        # 0.0 is a legitimate value (not "missing"); the helper guards
        # only against ``is None``.
        bits = mod._bits_from_payload_flags({"max_distance": 0})
        assert bits == ["max_d=0"]

    def test_both_flags_combined(self) -> None:
        bits = mod._bits_from_payload_flags({"reranker_model_id": "x", "max_distance": 0.3})
        assert bits == ["+reranker", "max_d=0.3"]


class TestSummarizePayload:
    def test_empty_payload_returns_empty_string(self) -> None:
        assert mod.summarize_payload({}, None) == ""

    def test_joins_bits_with_middle_dot(self) -> None:
        payload = {
            "prediction_set_id": "deadbeefdeadbeefdeadbeefdeadbeef",
            "evaluation_set_id": "cafebabecafebabecafebabecafebabe",
            "scoring_config_id": "12345678123456781234567812345678",
            "reranker_model_id": "x",
            "max_distance": 0.5,
        }
        out = mod.summarize_payload(payload, session=None)
        # All five bits join with " · ".
        parts = out.split(" · ")
        assert parts == [
            "pred=deadbeef",
            "eval=cafebabe",
            "scoring=12345678",
            "+reranker",
            "max_d=0.5",
        ]
