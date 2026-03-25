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
        with pytest.raises(ValueError):
            LoadOntologySnapshotPayload.model_validate({"obo_url": "   "})

    def test_missing_url_raises(self) -> None:
        with pytest.raises(ValueError):
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


_OBO_WITH_RELATIONSHIPS = """\
format-version: 1.2
data-version: releases/2024-06-01

[Term]
id: GO:0008150
name: biological_process
namespace: biological_process
def: "Root biological process." [GOC:go_curators]

[Term]
id: GO:0009987
name: cellular process
namespace: biological_process
def: "Any process that is carried out at the cellular level." [GOC:go_curators]
is_a: GO:0008150 ! biological_process

[Term]
id: GO:0044237
name: cellular metabolic process
namespace: biological_process
def: "The chemical reactions involving a cell." [GOC:go_curators]
is_a: GO:0009987 ! cellular process
relationship: part_of GO:0008150 ! biological_process
"""


class TestParseTermsRelationships:
    """Tests for is_a and relationship: parsing (lines 275-287)."""

    def _op(self) -> LoadOntologySnapshotOperation:
        return LoadOntologySnapshotOperation()

    def test_is_a_relationship_parsed(self) -> None:
        op = self._op()
        terms = {t["go_id"]: t for t in op._parse_terms(_OBO_WITH_RELATIONSHIPS)}
        cellular = terms["GO:0009987"]
        assert ("is_a", "GO:0008150") in cellular["relationships"]

    def test_part_of_relationship_parsed(self) -> None:
        op = self._op()
        terms = {t["go_id"]: t for t in op._parse_terms(_OBO_WITH_RELATIONSHIPS)}
        metabolic = terms["GO:0044237"]
        assert ("part_of", "GO:0008150") in metabolic["relationships"]

    def test_multiple_relationships_on_single_term(self) -> None:
        op = self._op()
        terms = {t["go_id"]: t for t in op._parse_terms(_OBO_WITH_RELATIONSHIPS)}
        metabolic = terms["GO:0044237"]
        assert len(metabolic["relationships"]) == 2
        assert ("is_a", "GO:0009987") in metabolic["relationships"]
        assert ("part_of", "GO:0008150") in metabolic["relationships"]

    def test_root_term_has_no_relationships(self) -> None:
        op = self._op()
        terms = {t["go_id"]: t for t in op._parse_terms(_OBO_WITH_RELATIONSHIPS)}
        root = terms["GO:0008150"]
        assert root["relationships"] == []

    def test_all_supported_relationship_types(self) -> None:
        """Each of the 7 supported relationship types is captured."""
        op = self._op()
        for rt in [
            "part_of", "regulates", "negatively_regulates",
            "positively_regulates", "occurs_in", "capable_of",
            "capable_of_part_of",
        ]:
            obo = (
                "format-version: 1.2\ndata-version: releases/2024-01-01\n\n"
                "[Term]\nid: GO:0000001\nname: child\nnamespace: biological_process\n"
                f"relationship: {rt} GO:0000002 ! parent\n"
            )
            terms = op._parse_terms(obo)
            assert (rt, "GO:0000002") in terms[0]["relationships"], f"Failed for {rt}"

    def test_unsupported_relationship_type_ignored(self) -> None:
        op = self._op()
        obo = (
            "format-version: 1.2\ndata-version: releases/2024-01-01\n\n"
            "[Term]\nid: GO:0000001\nname: child\nnamespace: biological_process\n"
            "relationship: has_part GO:0000002 ! parent\n"
        )
        terms = op._parse_terms(obo)
        assert terms[0].get("relationships", []) == []

    def test_relationship_line_with_no_go_prefix_ignored(self) -> None:
        """relationship: part_of SOMETHING (not GO:) is skipped."""
        op = self._op()
        obo = (
            "format-version: 1.2\ndata-version: releases/2024-01-01\n\n"
            "[Term]\nid: GO:0000001\nname: child\nnamespace: biological_process\n"
            "relationship: part_of CHEBI:12345 ! not a GO term\n"
        )
        terms = op._parse_terms(obo)
        assert terms[0].get("relationships", []) == []

    def test_definition_without_quotes_gives_none(self) -> None:
        """def: line that doesn't match the quoted pattern yields None."""
        op = self._op()
        obo = (
            "format-version: 1.2\ndata-version: releases/2024-01-01\n\n"
            "[Term]\nid: GO:0000001\nname: test\nnamespace: biological_process\n"
            "def: no quotes here\n"
        )
        terms = op._parse_terms(obo)
        assert terms[0]["definition"] is None


class TestDownload:
    """Tests for _download (lines 202-207)."""

    def test_download_success(self) -> None:
        op = LoadOntologySnapshotOperation()
        payload = LoadOntologySnapshotPayload.model_validate(
            {"obo_url": "http://example.org/go.obo"}
        )
        emit = MagicMock()

        mock_resp = MagicMock()
        mock_resp.text = _OBO_SAMPLE
        mock_resp.raise_for_status = MagicMock()

        with patch(
            "protea.core.operations.load_ontology_snapshot.requests.get",
            return_value=mock_resp,
        ) as mock_get:
            result = op._download(payload, emit)

        assert result == _OBO_SAMPLE
        mock_get.assert_called_once_with(
            "http://example.org/go.obo", timeout=120, stream=True
        )
        # Should emit download_start and download_done
        assert emit.call_count == 2
        assert emit.call_args_list[0][0][0] == "load_ontology_snapshot.download_start"
        assert emit.call_args_list[1][0][0] == "load_ontology_snapshot.download_done"
        assert emit.call_args_list[1][0][2]["bytes"] == len(_OBO_SAMPLE)

    def test_download_http_error_propagates(self) -> None:
        import requests as req

        op = LoadOntologySnapshotOperation()
        payload = LoadOntologySnapshotPayload.model_validate(
            {"obo_url": "http://example.org/go.obo"}
        )
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = req.HTTPError("404 Not Found")

        with patch(
            "protea.core.operations.load_ontology_snapshot.requests.get",
            return_value=mock_resp,
        ):
            with pytest.raises(req.HTTPError):
                op._download(payload, MagicMock())

    def test_download_connection_error_propagates(self) -> None:
        import requests as req

        op = LoadOntologySnapshotOperation()
        payload = LoadOntologySnapshotPayload.model_validate(
            {"obo_url": "http://example.org/go.obo"}
        )

        with patch(
            "protea.core.operations.load_ontology_snapshot.requests.get",
            side_effect=req.ConnectionError("DNS failure"),
        ):
            with pytest.raises(req.ConnectionError):
                op._download(payload, MagicMock())


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

    def test_new_snapshot_inserts_relationships(self) -> None:
        """Lines 163-167: relationship GOTermRelationship objects are created for new snapshots."""
        session = self._mock_session(existing_snapshot=None)

        _id_counter = {"n": 0}

        def add_side_effect(obj):
            from protea.infrastructure.orm.models.annotation.ontology_snapshot import (
                OntologySnapshot,
            )
            if isinstance(obj, OntologySnapshot):
                obj.id = "snap-id"

        session.add.side_effect = add_side_effect

        def add_all_side_effect(items):
            """Simulate DB flush assigning IDs to GOTerm objects."""
            for item in items:
                from protea.infrastructure.orm.models.annotation.go_term import GOTerm
                if isinstance(item, GOTerm) and item.id is None:
                    _id_counter["n"] += 1
                    item.id = _id_counter["n"]

        session.add_all.side_effect = add_all_side_effect

        with patch.object(
            LoadOntologySnapshotOperation,
            "_download",
            return_value=_OBO_WITH_RELATIONSHIPS,
        ):
            op = LoadOntologySnapshotOperation()
            result = op.execute(
                session,
                {"obo_url": "http://example.org/go.obo"},
                emit=_noop_emit,
            )

        # 3 terms, 3 relationships (1 is_a on GO:0009987, 1 is_a + 1 part_of on GO:0044237)
        assert result.result["terms_inserted"] == 3
        assert result.result["relationships_inserted"] == 3
        # Second add_all call is the relationships
        rel_call_args = session.add_all.call_args_list[1][0][0]
        assert len(rel_call_args) == 3

    def test_new_snapshot_skips_relationship_with_missing_parent(self) -> None:
        """Lines 164-166: if parent GO ID not in go_id_to_db_id, relationship is skipped."""
        obo = (
            "format-version: 1.2\ndata-version: releases/2024-01-01\n\n"
            "[Term]\nid: GO:0000001\nname: child\nnamespace: biological_process\n"
            "is_a: GO:9999999 ! nonexistent parent\n"
        )
        session = self._mock_session(existing_snapshot=None)

        _id_counter = {"n": 0}

        def add_side_effect(obj):
            from protea.infrastructure.orm.models.annotation.ontology_snapshot import (
                OntologySnapshot,
            )
            if isinstance(obj, OntologySnapshot):
                obj.id = "snap-id"

        session.add.side_effect = add_side_effect

        def add_all_side_effect(items):
            for item in items:
                from protea.infrastructure.orm.models.annotation.go_term import GOTerm
                if isinstance(item, GOTerm) and item.id is None:
                    _id_counter["n"] += 1
                    item.id = _id_counter["n"]

        session.add_all.side_effect = add_all_side_effect

        with patch.object(
            LoadOntologySnapshotOperation, "_download", return_value=obo
        ):
            op = LoadOntologySnapshotOperation()
            result = op.execute(
                session,
                {"obo_url": "http://example.org/go.obo"},
                emit=_noop_emit,
            )

        # Parent GO:9999999 doesn't exist in terms, so relationship is skipped
        assert result.result["relationships_inserted"] == 0

    def test_emits_progress_events(self) -> None:
        session = self._mock_session(existing_snapshot=None)
        emit = MagicMock()

        with patch.object(
            LoadOntologySnapshotOperation, "_download", return_value=_OBO_SAMPLE
        ):
            op = LoadOntologySnapshotOperation()
            op.execute(session, {"obo_url": "http://x.org/go.obo"}, emit=emit)

        events = [c.args[0] for c in emit.call_args_list]
        assert "load_ontology_snapshot.start" in events
        assert "load_ontology_snapshot.version" in events
        assert "load_ontology_snapshot.parsed" in events
        assert "load_ontology_snapshot.done" in events

    def test_done_event_includes_elapsed(self) -> None:
        session = self._mock_session(existing_snapshot=None)
        emit = MagicMock()

        with patch.object(
            LoadOntologySnapshotOperation, "_download", return_value=_OBO_SAMPLE
        ):
            op = LoadOntologySnapshotOperation()
            result = op.execute(session, {"obo_url": "http://x.org/go.obo"}, emit=emit)

        assert "elapsed_seconds" in result.result
        assert result.result["elapsed_seconds"] >= 0

    def test_backfill_relationships_when_zero(self) -> None:
        """Lines 87-125: snapshot exists but has 0 relationships — backfill them."""
        existing = MagicMock()
        existing.id = "existing-uuid"

        call_idx = {"n": 0}

        def query_side_effect(*args):
            call_idx["n"] += 1
            m = MagicMock()
            if call_idx["n"] == 1:
                # OntologySnapshot filter_by query
                m.filter_by.return_value.first.return_value = existing
            elif call_idx["n"] == 2:
                # func.count(GOTermRelationship.id) → 0
                m.filter.return_value.scalar.return_value = 0
            elif call_idx["n"] == 3:
                # GOTerm (go_id, id) query for the backfill map
                m.filter.return_value.all.return_value = [
                    ("GO:0003674", 1),
                    ("GO:0008150", 2),
                    ("GO:0005575", 3),
                    ("GO:0003824", 4),
                ]
            return m

        session = MagicMock()
        session.query.side_effect = query_side_effect
        emit = MagicMock()

        with patch.object(
            LoadOntologySnapshotOperation, "_download", return_value=_OBO_SAMPLE
        ):
            op = LoadOntologySnapshotOperation()
            result = op.execute(
                session,
                {"obo_url": "http://example.org/go.obo"},
                emit=emit,
            )

        assert result.result["skipped"] is False
        assert result.result["ontology_snapshot_id"] == "existing-uuid"
        assert "relationships_inserted" in result.result
        session.add_all.assert_called_once()
        session.flush.assert_called_once()

        events = [c.args[0] for c in emit.call_args_list]
        assert "load_ontology_snapshot.backfill_relationships" in events
        assert "load_ontology_snapshot.backfill_done" in events

    def test_backfill_skips_unknown_go_ids(self) -> None:
        """Lines 103-107: during backfill, terms with no DB ID are skipped."""
        existing = MagicMock()
        existing.id = "existing-uuid"

        call_idx = {"n": 0}

        def query_side_effect(*args):
            call_idx["n"] += 1
            m = MagicMock()
            if call_idx["n"] == 1:
                m.filter_by.return_value.first.return_value = existing
            elif call_idx["n"] == 2:
                m.filter.return_value.scalar.return_value = 0
            elif call_idx["n"] == 3:
                # Return only one term — the others won't be in the map
                m.filter.return_value.all.return_value = [("GO:0003674", 1)]
            return m

        session = MagicMock()
        session.query.side_effect = query_side_effect

        with patch.object(
            LoadOntologySnapshotOperation, "_download", return_value=_OBO_SAMPLE
        ):
            op = LoadOntologySnapshotOperation()
            result = op.execute(
                session,
                {"obo_url": "http://example.org/go.obo"},
                emit=_noop_emit,
            )

        assert result.result["relationships_inserted"] == 0

    def test_invalid_payload_raises(self) -> None:
        op = LoadOntologySnapshotOperation()
        with pytest.raises(ValueError):
            op.execute(MagicMock(), {}, emit=_noop_emit)
