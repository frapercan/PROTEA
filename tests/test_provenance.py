"""Tests for :func:`protea.core.provenance.capture_provenance` (T3.11)."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

from protea.core.provenance import capture_provenance


class TestCaptureProvenance:
    def test_returns_expected_top_level_keys(self) -> None:
        payload = capture_provenance()

        # Every automatic key is present (values may be None where the
        # probe legitimately can't resolve; we still want the slot).
        assert {
            "protea_version",
            "protea_git_sha",
            "python_version",
            "platform",
            "hostname",
            "captured_at",
        } <= set(payload.keys())

    def test_python_version_is_a_non_empty_string(self) -> None:
        payload = capture_provenance()
        assert isinstance(payload["python_version"], str)
        assert payload["python_version"]

    def test_captured_at_parses_as_iso8601_utc(self) -> None:
        payload = capture_provenance()
        # ``datetime.fromisoformat`` round-trips both naive and tz-aware
        # ISO8601 timestamps from Python 3.11+.
        parsed = datetime.fromisoformat(payload["captured_at"])
        # We always emit a timezone-aware UTC stamp via utcnow().
        assert parsed.tzinfo is not None

    def test_extra_overlays_after_automatic_probes(self) -> None:
        # ``extra`` wins on collision: a caller-supplied hostname tag must
        # appear in the output even though the automatic probe also writes
        # the field.
        payload = capture_provenance(extra={"hostname": "test-rig", "role": "ablation"})
        assert payload["hostname"] == "test-rig"
        assert payload["role"] == "ablation"

    def test_returns_new_dict_per_call(self) -> None:
        first = capture_provenance(extra={"k": 1})
        second = capture_provenance(extra={"k": 2})
        assert first is not second
        assert first["k"] == 1
        assert second["k"] == 2

    def test_resilient_to_missing_distribution_metadata(self) -> None:
        # When ``importlib.metadata.version("protea")`` raises (editable
        # install without metadata, broken environment) the field falls
        # back to None instead of propagating the exception.
        with patch(
            "protea.core.provenance._resolve_protea_version", return_value=None
        ):
            payload = capture_provenance()
        assert payload["protea_version"] is None

    def test_resilient_to_non_git_checkout(self) -> None:
        # Outside a git checkout (or when ``git`` isn't on PATH) the
        # resolver returns None — the helper must surface that, never raise.
        with patch(
            "protea.core.provenance.resolve_protea_git_sha", return_value=None
        ):
            payload = capture_provenance()
        assert payload["protea_git_sha"] is None
