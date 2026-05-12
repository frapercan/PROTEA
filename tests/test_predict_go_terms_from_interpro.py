"""Unit tests for ``PredictGOTermsFromInterProOperation`` (IP.4b-PRED).

No DB or network — the SQLAlchemy session is a ``MagicMock`` and the
join result is hand-rolled. Covers payload validation, the
per-protein vote aggregation, and registry wiring. The Postgres
round-trip / smoke test lives in
``tests/test_predict_go_terms_from_interpro_pg.py``.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest

from protea.core.contracts.operation import OperationResult
from protea.core.operations.predict_go_terms_from_interpro import (
    _ALGORITHM_TAG,
    PredictGOTermsFromInterProOperation,
    PredictGOTermsFromInterProPayload,
    _RunCounters,
)


def _noop_emit(*_args, **_kwargs) -> None:
    return None


def _minimal_payload(**overrides) -> dict:
    base: dict = {
        "embedding_config_id": str(uuid.uuid4()),
        "annotation_set_id": str(uuid.uuid4()),
        "ontology_snapshot_id": str(uuid.uuid4()),
        "source_version": "InterPro-104.0",
        "query_accessions": ["P12345"],
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Payload validation
# ---------------------------------------------------------------------------


class TestPayload:
    def test_valid_minimal(self) -> None:
        p = PredictGOTermsFromInterProPayload.model_validate(_minimal_payload())
        assert p.source_version == "InterPro-104.0"
        assert p.query_accessions == ["P12345"]
        assert p.chunk_size == 1000
        assert p.ipr_release_floor is None

    def test_strips_whitespace(self) -> None:
        p = PredictGOTermsFromInterProPayload.model_validate(
            _minimal_payload(source_version="  v1  ", ipr_release_floor="  IPR-99 ")
        )
        assert p.source_version == "v1"
        assert p.ipr_release_floor == "IPR-99"

    def test_query_set_id_alone_is_valid(self) -> None:
        p = PredictGOTermsFromInterProPayload.model_validate(
            _minimal_payload(query_accessions=None, query_set_id=str(uuid.uuid4()))
        )
        assert p.query_accessions is None
        assert p.query_set_id is not None

    def test_no_query_inputs_rejected(self) -> None:
        with pytest.raises(ValueError):
            PredictGOTermsFromInterProPayload.model_validate(
                _minimal_payload(query_accessions=None, query_set_id=None)
            )

    def test_empty_source_version_rejected(self) -> None:
        with pytest.raises(ValueError):
            PredictGOTermsFromInterProPayload.model_validate(
                _minimal_payload(source_version="   ")
            )

    def test_empty_accessions_list_rejected(self) -> None:
        with pytest.raises(ValueError):
            PredictGOTermsFromInterProPayload.model_validate(
                _minimal_payload(query_accessions=[])
            )

    def test_zero_chunk_size_rejected(self) -> None:
        with pytest.raises(ValueError):
            PredictGOTermsFromInterProPayload.model_validate(
                _minimal_payload(chunk_size=0)
            )

    def test_empty_query_set_id_rejected(self) -> None:
        with pytest.raises(ValueError):
            PredictGOTermsFromInterProPayload.model_validate(
                _minimal_payload(query_set_id=" ")
            )


# ---------------------------------------------------------------------------
# Vote aggregation (pure function — no DB)
# ---------------------------------------------------------------------------


class TestAggregate:
    def _ps_id(self) -> uuid.UUID:
        return uuid.uuid4()

    def test_single_voter_distance_one(self) -> None:
        """One IPR mapping → vote_count=1 → distance=1.0."""
        op = PredictGOTermsFromInterProOperation()
        counters = _RunCounters()
        rows = [("P1", "IPR000001", "GO:0005515", 42, "IPR-104.0", "IEA")]
        out = list(op._aggregate(rows, self._ps_id(), counters))
        assert len(out) == 1
        pred = out[0]
        assert pred["protein_accession"] == "P1"
        assert pred["go_term_id"] == 42
        assert pred["ref_protein_accession"] == "IPR000001"
        assert pred["distance"] == 1.0
        assert pred["evidence_code"] == "IEA"
        assert pred["vote_count"] == 1
        assert counters.candidate_pairs == 1
        assert counters.proteins_with_predictions == 1

    def test_multiple_voters_lower_distance(self) -> None:
        """3 distinct IPR entries voting for the same GO → vote_count=3, distance=1/3."""
        op = PredictGOTermsFromInterProOperation()
        counters = _RunCounters()
        rows = [
            ("P1", "IPR000003", "GO:0005515", 42, "IPR-104.0", "IEA"),
            ("P1", "IPR000001", "GO:0005515", 42, "IPR-104.0", "IEA"),
            ("P1", "IPR000002", "GO:0005515", 42, "IPR-104.0", "IEA"),
        ]
        out = list(op._aggregate(rows, self._ps_id(), counters))
        assert len(out) == 1
        pred = out[0]
        assert pred["vote_count"] == 3
        assert pred["distance"] == pytest.approx(1.0 / 3.0)
        # Stable: smallest lexicographic IPR wins the ref slot.
        assert pred["ref_protein_accession"] == "IPR000001"

    def test_duplicate_ipr_collapses_vote(self) -> None:
        """Same IPR appearing twice (e.g. two domains on the protein) still counts once."""
        op = PredictGOTermsFromInterProOperation()
        counters = _RunCounters()
        rows = [
            ("P1", "IPR000001", "GO:0005515", 42, "IPR-104.0", "IEA"),
            ("P1", "IPR000001", "GO:0005515", 42, "IPR-104.0", "IEA"),
        ]
        out = list(op._aggregate(rows, self._ps_id(), counters))
        assert len(out) == 1
        assert out[0]["vote_count"] == 1

    def test_distinct_proteins_emit_distinct_rows(self) -> None:
        op = PredictGOTermsFromInterProOperation()
        counters = _RunCounters()
        rows = [
            ("P1", "IPR000001", "GO:0005515", 42, "IPR-104.0", "IEA"),
            ("P2", "IPR000001", "GO:0005515", 42, "IPR-104.0", "IEA"),
        ]
        out = list(op._aggregate(rows, self._ps_id(), counters))
        proteins = {r["protein_accession"] for r in out}
        assert proteins == {"P1", "P2"}
        assert counters.proteins_with_predictions == 2

    def test_evidence_fallback_iea_when_all_null(self) -> None:
        op = PredictGOTermsFromInterProOperation()
        counters = _RunCounters()
        rows = [("P1", "IPR000001", "GO:0005515", 42, "IPR-104.0", None)]
        out = list(op._aggregate(rows, self._ps_id(), counters))
        assert out[0]["evidence_code"] == "IEA"

    def test_evidence_first_non_null_wins(self) -> None:
        op = PredictGOTermsFromInterProOperation()
        counters = _RunCounters()
        rows = [
            ("P1", "IPR000002", "GO:0005515", 42, "IPR-104.0", None),
            ("P1", "IPR000001", "GO:0005515", 42, "IPR-104.0", "CURATED"),
        ]
        out = list(op._aggregate(rows, self._ps_id(), counters))
        assert out[0]["evidence_code"] == "CURATED"

    def test_empty_rows_yields_nothing(self) -> None:
        op = PredictGOTermsFromInterProOperation()
        counters = _RunCounters()
        out = list(op._aggregate([], self._ps_id(), counters))
        assert out == []
        assert counters.candidate_pairs == 0


# ---------------------------------------------------------------------------
# Bulk insert + chunking
# ---------------------------------------------------------------------------


class TestBulkInsert:
    @staticmethod
    def _mock_session(inserted_per_call: list[int]) -> MagicMock:
        session = MagicMock()
        results = []
        for n in inserted_per_call:
            r = MagicMock()
            r.fetchall.return_value = [(i,) for i in range(n)]
            results.append(r)
        session.execute.side_effect = results
        return session

    def test_single_chunk(self) -> None:
        rows = [
            {
                "prediction_set_id": uuid.uuid4(),
                "protein_accession": "P1",
                "go_term_id": 42,
                "ref_protein_accession": "IPR000001",
                "distance": 1.0,
                "evidence_code": "IEA",
                "vote_count": 1,
            }
        ]
        session = self._mock_session([1])
        n = PredictGOTermsFromInterProOperation._bulk_insert(session, rows, 1000)
        assert n == 1
        session.execute.assert_called_once()

    def test_chunked(self) -> None:
        rows = [
            {
                "prediction_set_id": uuid.uuid4(),
                "protein_accession": f"P{i}",
                "go_term_id": 42,
                "ref_protein_accession": "IPR000001",
                "distance": 1.0,
                "evidence_code": "IEA",
                "vote_count": 1,
            }
            for i in range(5)
        ]
        session = self._mock_session([2, 2, 1])
        n = PredictGOTermsFromInterProOperation._bulk_insert(session, rows, 2)
        assert n == 5
        assert session.execute.call_count == 3

    def test_zero_inserted_when_all_conflict(self) -> None:
        rows = [
            {
                "prediction_set_id": uuid.uuid4(),
                "protein_accession": "P1",
                "go_term_id": 42,
                "ref_protein_accession": "IPR000001",
                "distance": 1.0,
                "evidence_code": "IEA",
                "vote_count": 1,
            }
        ]
        session = self._mock_session([0])
        n = PredictGOTermsFromInterProOperation._bulk_insert(session, rows, 1000)
        assert n == 0


# ---------------------------------------------------------------------------
# summarize_payload
# ---------------------------------------------------------------------------


class TestSummarizePayload:
    def test_handles_empty_payload(self) -> None:
        assert PredictGOTermsFromInterProOperation().summarize_payload({}) == ""

    def test_contains_source_version(self) -> None:
        out = PredictGOTermsFromInterProOperation().summarize_payload(
            {"source_version": "InterPro-104.0"}
        )
        assert "i2g=InterPro-104.0" in out

    def test_contains_accession_count(self) -> None:
        out = PredictGOTermsFromInterProOperation().summarize_payload(
            {
                "source_version": "v1",
                "query_accessions": ["P1", "P2", "P3"],
            }
        )
        assert "n=3" in out

    def test_contains_ipr_floor(self) -> None:
        out = PredictGOTermsFromInterProOperation().summarize_payload(
            {"source_version": "v1", "ipr_release_floor": "InterProScan-5.66"}
        )
        assert "ipr>=InterProScan-5.66" in out


# ---------------------------------------------------------------------------
# Catalog registration
# ---------------------------------------------------------------------------


def test_registered_in_catalog() -> None:
    from protea.core.operation_catalog import build_operation_registry

    registry = build_operation_registry()
    op = registry.get("predict_go_terms_from_interpro")
    assert op is not None
    assert op.name == "predict_go_terms_from_interpro"
    assert "InterPro" in op.description


def test_algorithm_tag_constant() -> None:
    """The algorithm tag is the contract surface for ensemble routers."""
    assert _ALGORITHM_TAG == "interpro_propagation"


# ---------------------------------------------------------------------------
# execute() with mocked session (no real DB)
# ---------------------------------------------------------------------------


class TestExecuteShortCircuit:
    """Exercise the execute() short-circuits that don't need a real DB."""

    def test_no_queries_returns_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When the resolved accession set is empty, we return zero work."""
        op = PredictGOTermsFromInterProOperation()
        session = MagicMock()
        # The three FK validators all hit session.get() — return non-None
        # so they don't ValueError. Then resolve to no accessions.
        session.get.return_value = MagicMock()

        monkeypatch.setattr(
            PredictGOTermsFromInterProOperation,
            "_resolve_query_accessions",
            staticmethod(lambda *args, **kwargs: []),
        )

        result = op.execute(
            session,
            _minimal_payload(query_set_id=str(uuid.uuid4())),
            emit=_noop_emit,
        )
        assert isinstance(result, OperationResult)
        assert result.result == {"proteins": 0, "predictions": 0}

    def test_missing_embedding_config_raises(self) -> None:
        op = PredictGOTermsFromInterProOperation()
        session = MagicMock()
        session.get.return_value = None
        with pytest.raises(ValueError, match="EmbeddingConfig"):
            op.execute(session, _minimal_payload(), emit=_noop_emit)
