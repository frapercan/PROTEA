"""Unit tests for the ADR D45 blob-feature value-provenance guard.

Two surfaces are covered:

- the pure descriptor + digest helpers in
  :mod:`protea.core.operations.predict_go_terms._blob_provenance`
  (deterministic, active-flag / config-marker sensitivity), and
- the scorer's ``record_blob_provenance`` warn-and-record path
  (:class:`RerankerScorer`): a matching / absent expected provenance emits
  the record event and NO warning, a mismatched expected provenance emits a
  LOUD warning and still PROCEEDS (never raises).

The guard is warn-only by design (D45): the expected marker is nullable, so
a hard refuse would take serving down. These tests pin exactly that.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from protea.core.operations.predict_go_terms._blob_provenance import (
    BLOB_FAMILIES,
    blob_provenance_descriptor,
    compute_blob_provenance_sha,
    live_blob_provenance,
)
from protea.core.operations.predict_go_terms._reranker_scorer import (
    BLOB_PROVENANCE_MISMATCH_REASON,
    RerankerScorer,
)


def _emit_capture():
    events: list[tuple[str, dict]] = []

    def _emit(name, _msg, fields, _sev):
        events.append((name, fields or {}))

    return _emit, events


def _noop_attach_aspect(_session, _dicts) -> None:
    return None


def _payload(**flags):
    """A payload stand-in exposing only the attributes the guard reads.

    ``record_blob_provenance`` / the descriptor helpers reach the payload
    exclusively through ``getattr`` with defaults, so a lightweight namespace
    is sufficient and keeps the test independent of the pydantic contract
    (which does not yet carry ``reranker_blob_feature_provenance``).
    """
    defaults = dict(
        compute_classifier=False,
        compute_self_prior=False,
        compute_association=False,
        compute_ia=False,
        ia_file=None,
        reranker_model_id="rm-1",
    )
    defaults.update(flags)
    return SimpleNamespace(**defaults)


class TestBlobProvenanceDescriptor:
    def test_default_run_all_families_inactive(self) -> None:
        desc = blob_provenance_descriptor(_payload())
        assert set(desc) == set(BLOB_FAMILIES)
        for family in BLOB_FAMILIES:
            assert desc[family] == {"active": False}

    def test_active_family_carries_producer_version(self) -> None:
        desc = blob_provenance_descriptor(_payload(compute_association=True))
        assert desc["association"]["active"] is True
        assert "producer_version" in desc["association"]
        # Inactive families stay minimal.
        assert desc["classifier"] == {"active": False}

    def test_ia_active_carries_ia_file_marker(self) -> None:
        desc = blob_provenance_descriptor(_payload(compute_ia=True, ia_file="ia_v230.tsv"))
        assert desc["ia"]["active"] is True
        assert desc["ia"]["ia_file"] == "ia_v230.tsv"


class TestBlobProvenanceSha:
    def test_digest_is_stable_and_short(self) -> None:
        sha = compute_blob_provenance_sha(blob_provenance_descriptor(_payload()))
        assert isinstance(sha, str)
        assert len(sha) == 12
        # Reproducible across calls.
        assert sha == compute_blob_provenance_sha(blob_provenance_descriptor(_payload()))

    def test_active_flag_moves_the_digest(self) -> None:
        """A blob producer flipping on must change the digest.

        This is the exact 0.3462-incident shape: association going from a
        no-op to populated. The value column name is unchanged (so
        ``feature_schema_sha`` would NOT move); the blob provenance digest
        MUST.
        """
        off, _ = live_blob_provenance(_payload(compute_association=False))
        on, _ = live_blob_provenance(_payload(compute_association=True))
        assert off != on

    def test_ia_file_marker_moves_the_digest(self) -> None:
        a, _ = live_blob_provenance(_payload(compute_ia=True, ia_file="ia_v227.tsv"))
        b, _ = live_blob_provenance(_payload(compute_ia=True, ia_file="ia_v230.tsv"))
        assert a != b


class TestRecordBlobProvenanceMatch:
    def test_records_event_and_no_warning_when_expected_absent(self) -> None:
        """Legacy booster (no recorded provenance): record, do not warn."""
        scorer = RerankerScorer(attach_aspect=_noop_attach_aspect)
        emit, events = _emit_capture()
        p = _payload(compute_association=True)

        live_sha = scorer.record_blob_provenance(p, emit)

        names = [name for name, _ in events]
        assert "reranker.blob_provenance" in names
        assert "reranker.blob_provenance_mismatch" not in names
        record = next(f for n, f in events if n == "reranker.blob_provenance")
        assert record["blob_provenance_sha"] == live_sha

    def test_no_warning_when_expected_matches(self) -> None:
        scorer = RerankerScorer(attach_aspect=_noop_attach_aspect)
        emit, events = _emit_capture()
        matching_sha, _ = live_blob_provenance(_payload(compute_ia=True, ia_file="ia.tsv"))
        p = _payload(
            compute_ia=True,
            ia_file="ia.tsv",
            reranker_blob_feature_provenance=matching_sha,
        )

        returned = scorer.record_blob_provenance(p, emit)

        assert returned == matching_sha
        names = [name for name, _ in events]
        assert "reranker.blob_provenance" in names
        assert "reranker.blob_provenance_mismatch" not in names


class TestRecordBlobProvenanceMismatch:
    def test_mismatch_warns_loudly_and_proceeds(self, caplog: pytest.LogCaptureFixture) -> None:
        """A recorded provenance that disagrees with the live one must emit a
        LOUD warning (structured event + stdlib warning) and STILL PROCEED
        (return the live digest, never raise). This is the 0.3462-incident
        catch the schema-sha guard cannot make."""
        scorer = RerankerScorer(attach_aspect=_noop_attach_aspect)
        emit, events = _emit_capture()
        # Booster recorded "association off" provenance; live run has it on.
        train_sha, _ = live_blob_provenance(_payload(compute_association=False))
        p = _payload(
            compute_association=True,
            reranker_blob_feature_provenance=train_sha,
            reranker_model_id="rm-mismatch",
        )
        live_sha, _ = live_blob_provenance(p)
        assert live_sha != train_sha  # precondition for the test to be meaningful

        caplog.set_level(
            "WARNING",
            logger="protea.core.operations.predict_go_terms._blob_provenance",
        )
        returned = scorer.record_blob_provenance(p, emit)

        # Still proceeds: returns the live digest, no exception.
        assert returned == live_sha

        mismatch = [(n, f) for n, f in events if n == "reranker.blob_provenance_mismatch"]
        assert len(mismatch) == 1
        _, fields = mismatch[0]
        assert fields["reason"] == BLOB_PROVENANCE_MISMATCH_REASON
        assert fields["expected_blob_provenance_sha"] == train_sha
        assert fields["live_blob_provenance_sha"] == live_sha
        assert fields["reranker_model_id"] == "rm-mismatch"

        # The record event is still emitted alongside the warning.
        assert any(n == "reranker.blob_provenance" for n, _ in events)

        # Loud stdlib warning produced for operators tailing logs.
        assert any(
            rec.levelname == "WARNING"
            and "blob-feature value provenance mismatch" in rec.getMessage()
            for rec in caplog.records
        )
