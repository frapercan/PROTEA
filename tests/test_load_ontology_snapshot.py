from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from protea.core.operations.load_ontology_snapshot import (
    LoadOntologySnapshotOperation,
    LoadOntologySnapshotPayload,
)

_OBO_SAMPLE = """\
format-version: 1.2
data-version: releases/2024-01-17

[Term]
id: GO:0003674
name: molecular_function
namespace: molecular_function
def: "Elemental activities." [GOC:pdt]

[Term]
id: GO:0008150
name: biological_process
namespace: biological_process

[Term]
id: GO:0005575
name: cellular_component
namespace: cellular_component
is_obsolete: true

[Term]
id: GO:0003824
name: catalytic activity
namespace: molecular_function
def: "Catalysis of a reaction." [GOC:pdt]

[Typedef]
id: part_of
"""

_noop_emit = lambda *_: None  # noqa: E731


class TestLoadOntologySnapshotPayload:
    def test_valid(self) -> None:
        p = LoadOntologySnapshotPayload.model_validate(
            {"obo_url": "http://example.org/go.obo"}
        )
        assert p.obo_url == "http://example.org/go.obo"
        assert p.timeout_seconds == 120

    def test_empty_url_raises(self) -> None:
        with pytest.raises(Exception):
            LoadOntologySnapshotPayload.model_validate({"obo_url": "   "})

    def test_missing_url_raises(self) -> None:
        with pytest.raises(Exception):
            LoadOntologySnapshotPayload.model_validate({})


class TestParseTerms:
    def _op(self) -> LoadOntologySnapshotOperation:
        return LoadOntologySnapshotOperation()

    def test_extracts_version(self) -> None:
        op = self._op()
        assert op._extract_version(_OBO_SAMPLE) == "releases/2024-01-17"

    def test_missing_version_raises(self) -> None:
        op = self._op()
        with pytest.raises(ValueError, match="data-version"):
            op._extract_version("format-version: 1.2\n\n[Term]\nid: GO:0000001\n")

    def test_parses_all_terms(self) -> None:
        op = self._op()
        terms = op._parse_terms(_OBO_SAMPLE)
        assert len(terms) == 4

    def test_aspect_mapping(self) -> None:
        op = self._op()
        terms = {t["go_id"]: t for t in op._parse_terms(_OBO_SAMPLE)}
        assert terms["GO:0003674"]["aspect"] == "F"
        assert terms["GO:0008150"]["aspect"] == "P"
        assert terms["GO:0005575"]["aspect"] == "C"

    def test_obsolete_flagged(self) -> None:
        op = self._op()
        terms = {t["go_id"]: t for t in op._parse_terms(_OBO_SAMPLE)}
        assert terms["GO:0005575"]["is_obsolete"] is True
        assert terms["GO:0003824"]["is_obsolete"] is False

    def test_definition_extracted(self) -> None:
        op = self._op()
        terms = {t["go_id"]: t for t in op._parse_terms(_OBO_SAMPLE)}
        assert "Elemental" in (terms["GO:0003674"]["definition"] or "")

    def test_typedef_not_included(self) -> None:
        op = self._op()
        terms = op._parse_terms(_OBO_SAMPLE)
        go_ids = {t["go_id"] for t in terms}
        assert "part_of" not in go_ids


class TestLoadOntologySnapshotExecute:
    def _mock_session(self, existing_snapshot=None, rel_count=0):
        session = MagicMock()
        session.get.return_value = existing_snapshot
        session.query.return_value.filter_by.return_value.first.return_value = existing_snapshot
        session.query.return_value.filter.return_value.scalar.return_value = rel_count
        return session

    def test_idempotent_if_version_exists(self) -> None:
        existing = MagicMock()
        existing.id = "existing-uuid"
        session = self._mock_session(existing_snapshot=existing, rel_count=42)

        with patch.object(
            LoadOntologySnapshotOperation, "_download", return_value=_OBO_SAMPLE
        ):
            op = LoadOntologySnapshotOperation()
            result = op.execute(
                session,
                {"obo_url": "http://example.org/go.obo"},
                emit=_noop_emit,
            )

        assert result.result["skipped"] is True
        assert result.result["obo_version"] == "releases/2024-01-17"
        session.add.assert_not_called()

    def test_inserts_snapshot_and_terms(self) -> None:
        session = self._mock_session(existing_snapshot=None)
        fake_snapshot = MagicMock()
        fake_snapshot.id = "new-uuid"

        def add_side_effect(obj):
            if isinstance(obj, __import__(
                "protea.infrastructure.orm.models.annotation.ontology_snapshot",
                fromlist=["OntologySnapshot"]
            ).OntologySnapshot):
                obj.id = "new-uuid"

        session.add.side_effect = add_side_effect

        with patch.object(
            LoadOntologySnapshotOperation, "_download", return_value=_OBO_SAMPLE
        ):
            op = LoadOntologySnapshotOperation()
            result = op.execute(
                session,
                {"obo_url": "http://example.org/go.obo"},
                emit=_noop_emit,
            )

        assert result.result["terms_inserted"] == 4
        assert result.result["obo_version"] == "releases/2024-01-17"
        assert "skipped" not in result.result
        # add_all is called twice: once for GOTerms, once for GOTermRelationships
        assert session.add_all.call_count == 2
        terms_call_args = session.add_all.call_args_list[0][0][0]
        assert len(terms_call_args) == 4
