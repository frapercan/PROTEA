"""Gates for archiving the raw OBO behind an OntologySnapshot.

The load-bearing part is not the upload. It is the congruence check: the file
served at ``obo_url`` today is not necessarily the file that was parsed when the
snapshot was loaded, so archiving without comparing would record a different
ontology under an existing snapshot's identity, and every score already
published against that snapshot would silently change meaning.
"""

from __future__ import annotations

import pytest

from protea.core.operations.archive_ontology_snapshot import (
    ArchiveOntologySnapshotOperation,
    ArchiveOntologySnapshotPayload,
    OntologyDriftError,
    obo_key_for,
)

OBO = """format-version: 1.2
data-version: releases/2025-03-16

[Term]
id: GO:0000001
name: mitochondrion inheritance
namespace: biological_process
is_a: GO:0048308

[Term]
id: GO:0000002
name: mitochondrial genome maintenance
namespace: biological_process

[Term]
id: GO:0000005
name: obsolete ribosomal chaperone activity
is_obsolete: true

[Typedef]
id: part_of
name: part of
"""


class _Snapshot:
    """Minimal stand-in: the gate only reads these four attributes."""

    def __init__(self, version="releases/2025-03-16"):
        self.id = "11111111-1111-1111-1111-111111111111"
        self.obo_version = version
        self.obo_url = "https://example.invalid/go.obo"
        self.obo_uri = None
        self.obo_sha256 = None


class _Session:
    """Returns a fixed go_id set for the gate's term query."""

    def __init__(self, go_ids):
        self._go_ids = go_ids

    def execute(self, *_args, **_kwargs):
        return [(g,) for g in self._go_ids]


def _emit(*_args, **_kwargs):
    return None


# --------------------------------------------------------------------------
# Parser: it decides what the gate compares against
# --------------------------------------------------------------------------
def test_parser_extracts_live_terms_and_version() -> None:
    ids, version = ArchiveOntologySnapshotOperation._parse_ids_and_version(OBO)
    assert version == "releases/2025-03-16"
    assert ids == {"GO:0000001", "GO:0000002"}


def test_parser_excludes_obsolete_terms() -> None:
    """The database side of the comparison filters is_obsolete, so this must
    too, or every archive would look like it lost terms."""
    ids, _ = ArchiveOntologySnapshotOperation._parse_ids_and_version(OBO)
    assert "GO:0000005" not in ids


def test_parser_ignores_typedef_stanzas() -> None:
    ids, _ = ArchiveOntologySnapshotOperation._parse_ids_and_version(OBO)
    assert "part_of" not in ids


def test_parser_returns_none_version_when_header_absent() -> None:
    _, version = ArchiveOntologySnapshotOperation._parse_ids_and_version(
        "[Term]\nid: GO:0000001\n"
    )
    assert version is None


# --------------------------------------------------------------------------
# Congruence gate
# --------------------------------------------------------------------------
def _gate(db_ids, fetched_ids, version="releases/2025-03-16", max_drift=0.0):
    op = ArchiveOntologySnapshotOperation()
    return op._gate_congruence(
        _Session(db_ids), _Snapshot(), fetched_ids, version, max_drift, _emit
    )


def test_gate_passes_on_an_exact_match() -> None:
    stats = _gate({"GO:1", "GO:2"}, {"GO:1", "GO:2"})
    assert stats["missing_from_fetch"] == 0
    assert stats["drift_pct"] == 0.0


def test_gate_rejects_a_changed_data_version() -> None:
    with pytest.raises(OntologyDriftError, match="different release"):
        _gate({"GO:1"}, {"GO:1"}, version="releases/2026-01-23")


def test_gate_rejects_a_term_that_vanished_upstream() -> None:
    with pytest.raises(OntologyDriftError, match="absent from the file now served"):
        _gate({"GO:1", "GO:2"}, {"GO:1"})


def test_gate_tolerates_terms_added_upstream() -> None:
    """Additions do not invalidate the archive: every term the database holds
    is still present, so the loaded snapshot remains representable."""
    stats = _gate({"GO:1"}, {"GO:1", "GO:2"})
    assert stats["added_by_fetch"] == 1
    assert stats["missing_from_fetch"] == 0


def test_gate_honours_an_explicit_drift_allowance() -> None:
    db = {f"GO:{i}" for i in range(100)}
    fetched = db - {"GO:0"}
    with pytest.raises(OntologyDriftError):
        _gate(db, fetched)
    stats = _gate(db, fetched, max_drift=1.0)
    assert stats["drift_pct"] == pytest.approx(1.0)


def test_gate_default_allowance_is_exact() -> None:
    assert ArchiveOntologySnapshotPayload.model_validate(
        {"ontology_snapshot_id": "x"}
    ).max_term_drift_pct == 0.0


def test_gate_accepts_a_missing_version_header() -> None:
    """A file without data-version cannot contradict the snapshot, so the term
    comparison is the only evidence and is allowed to stand alone."""
    stats = _gate({"GO:1"}, {"GO:1"}, version=None)
    assert stats["db_terms"] == 1


# --------------------------------------------------------------------------
# Payload and key
# --------------------------------------------------------------------------
def test_payload_rejects_blank_id() -> None:
    with pytest.raises(ValueError):
        ArchiveOntologySnapshotPayload.model_validate({"ontology_snapshot_id": " "})


def test_archive_key_is_namespaced_and_gzipped() -> None:
    key = obo_key_for("abc-123")
    assert key == "ontology_snapshot/abc-123/go.obo.gz"
    # download_tsv decompresses on the .gz suffix, so the key must keep it.
    assert key.endswith(".gz")
