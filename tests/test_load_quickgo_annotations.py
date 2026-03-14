from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import pytest

from protea.core.operations.load_quickgo_annotations import (
    LoadQuickGOAnnotationsOperation,
    LoadQuickGOAnnotationsPayload,
)

_noop_emit = lambda *_: None  # noqa: E731
_SNAPSHOT_ID = str(uuid.uuid4())

# Simulates a QuickGO TSV response (header + 3 rows)
_QUICKGO_ROWS = [
    {
        "GENE PRODUCT DB": "UniProtKB",
        "GENE PRODUCT ID": "P12345",
        "SYMBOL": "GENE1",
        "QUALIFIER": "enables",
        "GO TERM": "GO:0003824",
        "GO ASPECT": "F",
        "ECO ID": "ECO:0000314",
        "REFERENCE": "PMID:123",
        "WITH/FROM": "",
        "TAXON ID": "9606",
        "ASSIGNED BY": "UniProt",
        "ANNOTATION EXTENSION": "",
        "DATE": "20240101",
    },
    {
        "GENE PRODUCT DB": "UniProtKB",
        "GENE PRODUCT ID": "Q67890",
        "SYMBOL": "GENE2",
        "QUALIFIER": "involved_in",
        "GO TERM": "GO:0008150",
        "GO ASPECT": "P",
        "ECO ID": "ECO:0000501",
        "REFERENCE": "PMID:456",
        "WITH/FROM": "",
        "TAXON ID": "9606",
        "ASSIGNED BY": "UniProt",
        "ANNOTATION EXTENSION": "",
        "DATE": "20240101",
    },
    {
        "GENE PRODUCT DB": "UniProtKB",
        "GENE PRODUCT ID": "XXXXXX",
        "SYMBOL": "UNKNOWN",
        "QUALIFIER": "enables",
        "GO TERM": "GO:0003824",
        "GO ASPECT": "F",
        "ECO ID": "ECO:0000314",
        "REFERENCE": "PMID:789",
        "WITH/FROM": "",
        "TAXON ID": "9606",
        "ASSIGNED BY": "UniProt",
        "ANNOTATION EXTENSION": "",
        "DATE": "20240101",
    },
]


class TestLoadQuickGOAnnotationsPayload:
    def test_valid_minimal(self) -> None:
        p = LoadQuickGOAnnotationsPayload.model_validate({
            "ontology_snapshot_id": _SNAPSHOT_ID,
            "source_version": "2026-01-11",
        })
        assert p.eco_mapping_url is None
        assert p.page_size == 10000

    def test_missing_snapshot_raises(self) -> None:
        with pytest.raises(Exception):
            LoadQuickGOAnnotationsPayload.model_validate({"source_version": "2026-01"})

    def test_empty_source_version_raises(self) -> None:
        with pytest.raises(Exception):
            LoadQuickGOAnnotationsPayload.model_validate({
                "ontology_snapshot_id": _SNAPSHOT_ID,
                "source_version": "",
            })


class TestStoreBuffer:
    def _op(self) -> LoadQuickGOAnnotationsOperation:
        return LoadQuickGOAnnotationsOperation()

    def test_skips_unknown_accession(self) -> None:
        op = self._op()
        session = MagicMock()
        inserted, skipped = op._store_buffer(
            session, _QUICKGO_ROWS, uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345"},
            go_term_map={"GO:0003824": 1, "GO:0008150": 2},
            eco_map={},
        )
        assert inserted == 1
        assert skipped == 2

    def test_skips_unknown_go_term(self) -> None:
        op = self._op()
        session = MagicMock()
        inserted, skipped = op._store_buffer(
            session, _QUICKGO_ROWS, uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345", "Q67890", "XXXXXX"},
            go_term_map={},
            eco_map={},
        )
        assert inserted == 0
        assert skipped == 3

    def test_inserts_all_valid(self) -> None:
        op = self._op()
        session = MagicMock()
        inserted, skipped = op._store_buffer(
            session, _QUICKGO_ROWS, uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345", "Q67890", "XXXXXX"},
            go_term_map={"GO:0003824": 1, "GO:0008150": 2},
            eco_map={},
        )
        assert inserted == 3
        assert skipped == 0
        session.execute.assert_called_once()

    def test_eco_mapping_applied(self) -> None:
        op = self._op()
        session = MagicMock()
        eco_map = {"ECO:0000314": "IDA", "ECO:0000501": "IEA"}
        inserted, _ = op._store_buffer(
            session, _QUICKGO_ROWS[:1], uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345"},
            go_term_map={"GO:0003824": 1},
            eco_map=eco_map,
        )
        assert inserted == 1
        call_stmt = session.execute.call_args[0][0]
        from sqlalchemy.dialects.postgresql import dialect as pg_dialect
        compiled = call_stmt.compile(dialect=pg_dialect())
        assert compiled.params["evidence_code_m0"] == "IDA"

    def test_raw_eco_stored_when_no_mapping(self) -> None:
        op = self._op()
        session = MagicMock()
        inserted, _ = op._store_buffer(
            session, _QUICKGO_ROWS[:1], uuid.UUID(_SNAPSHOT_ID),
            valid_accessions={"P12345"},
            go_term_map={"GO:0003824": 1},
            eco_map={},
        )
        assert inserted == 1
        call_stmt = session.execute.call_args[0][0]
        from sqlalchemy.dialects.postgresql import dialect as pg_dialect
        compiled = call_stmt.compile(dialect=pg_dialect())
        assert compiled.params["evidence_code_m0"] == "ECO:0000314"
