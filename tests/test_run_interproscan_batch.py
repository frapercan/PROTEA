"""Unit tests for the IP.3 ``run_interproscan_batch`` operation.

Unit coverage of the pure-Python pieces: payload validation, chunking,
FASTA writing, and the ``ipr_release_floor`` resume filter. The
postgres-backed round-trip + resume tests live in
``test_run_interproscan_batch_pg.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from protea.core.operation_catalog import build_operation_registry
from protea.core.operations.run_interproscan_batch import (
    RunInterProScanBatchOperation,
    RunInterProScanBatchPayload,
    _chunked,
    _write_fasta,
)


class TestPayloadValidation:
    """Validation rules for :class:`RunInterProScanBatchPayload`."""

    def test_query_set_id_only(self) -> None:
        p = RunInterProScanBatchPayload(query_set_id="abc-123")
        assert p.query_set_id == "abc-123"
        assert p.accessions is None

    def test_accessions_only(self) -> None:
        p = RunInterProScanBatchPayload(accessions=["P12345", "Q67890"])
        assert p.accessions == ["P12345", "Q67890"]
        assert p.query_set_id is None

    def test_rejects_both_inputs(self) -> None:
        with pytest.raises(ValidationError):
            RunInterProScanBatchPayload(query_set_id="abc", accessions=["P12345"])

    def test_rejects_neither_input(self) -> None:
        with pytest.raises(ValidationError):
            RunInterProScanBatchPayload()

    def test_rejects_empty_string_query_set_id(self) -> None:
        with pytest.raises(ValidationError):
            RunInterProScanBatchPayload(query_set_id="   ")

    def test_rejects_empty_accessions_list(self) -> None:
        with pytest.raises(ValidationError):
            RunInterProScanBatchPayload(accessions=[])

    def test_strips_whitespace_in_accessions(self) -> None:
        p = RunInterProScanBatchPayload(accessions=["  P12345  ", "Q67890"])
        assert p.accessions == ["P12345", "Q67890"]

    def test_default_chunk_size_is_fifty(self) -> None:
        p = RunInterProScanBatchPayload(accessions=["P12345"])
        assert p.chunk_size == 50

    def test_chunk_size_must_be_positive(self) -> None:
        with pytest.raises(ValidationError):
            RunInterProScanBatchPayload(accessions=["P12345"], chunk_size=0)

    def test_accepts_optional_extra_args(self) -> None:
        p = RunInterProScanBatchPayload(
            accessions=["P12345"],
            extra_args=["-iprlookup", "-goterms"],
        )
        assert p.extra_args == ["-iprlookup", "-goterms"]


class TestChunking:
    """Logic for splitting accession lists into per-run chunks."""

    def test_exact_multiple(self) -> None:
        items = ["a", "b", "c", "d"]
        result = list(_chunked(items, 2))
        assert result == [("a", "b"), ("c", "d")]

    def test_with_remainder(self) -> None:
        items = ["a", "b", "c", "d", "e"]
        result = list(_chunked(items, 2))
        assert result == [("a", "b"), ("c", "d"), ("e",)]

    def test_chunk_size_larger_than_input(self) -> None:
        items = ["a", "b"]
        result = list(_chunked(items, 50))
        assert result == [("a", "b")]

    def test_empty_input(self) -> None:
        assert list(_chunked([], 50)) == []

    def test_chunks_are_tuples(self) -> None:
        result = list(_chunked(["a", "b"], 2))
        assert all(isinstance(c, tuple) for c in result)


class TestFastaWriter:
    """:func:`_write_fasta` builds a minimal one-line-per-record FASTA."""

    def test_writes_records_for_each_accession(self, tmp_path: Path) -> None:
        target = tmp_path / "chunk.fasta"
        sequences = {"P12345": "MKTII", "Q67890": "MQYDP"}
        written = _write_fasta(["P12345", "Q67890"], sequences, target)
        assert written == 2
        content = target.read_text()
        assert content == ">P12345\nMKTII\n>Q67890\nMQYDP\n"

    def test_skips_missing_sequences(self, tmp_path: Path) -> None:
        target = tmp_path / "chunk.fasta"
        sequences = {"P12345": "MKTII"}
        written = _write_fasta(["P12345", "Q67890"], sequences, target)
        assert written == 1
        content = target.read_text()
        assert ">P12345" in content
        assert ">Q67890" not in content

    def test_skips_empty_sequences(self, tmp_path: Path) -> None:
        target = tmp_path / "chunk.fasta"
        sequences = {"P12345": "", "Q67890": "MQYDP"}
        written = _write_fasta(["P12345", "Q67890"], sequences, target)
        assert written == 1
        content = target.read_text()
        assert ">P12345" not in content
        assert ">Q67890" in content


class TestOperationMetadata:
    """Operation contract pieces independent of any DB."""

    def test_name(self) -> None:
        op = RunInterProScanBatchOperation()
        assert op.name == "run_interproscan_batch"

    def test_description_is_non_empty(self) -> None:
        op = RunInterProScanBatchOperation()
        assert op.description
        assert "InterProScan" in op.description

    def test_registered_in_catalog(self) -> None:
        registry = build_operation_registry()
        op = registry.get("run_interproscan_batch")
        assert isinstance(op, RunInterProScanBatchOperation)

    def test_summarize_payload_with_query_set(self) -> None:
        op = RunInterProScanBatchOperation()
        summary = op.summarize_payload({"query_set_id": "abc-123", "chunk_size": 25})
        assert "query_set=abc-123" in summary
        assert "chunk=25" in summary

    def test_summarize_payload_with_accessions(self) -> None:
        op = RunInterProScanBatchOperation()
        summary = op.summarize_payload({"accessions": ["P12345", "Q67890", "R11111"]})
        assert "accessions=3" in summary

    def test_summarize_payload_with_floor(self) -> None:
        op = RunInterProScanBatchOperation()
        summary = op.summarize_payload(
            {
                "query_set_id": "abc-123",
                "ipr_release_floor": "InterProScan-5.66-98.0",
            }
        )
        assert "floor=InterProScan-5.66-98.0" in summary
