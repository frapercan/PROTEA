"""Unit tests for ``protea.core._training_dump_loaders``.

The three helpers were extracted out of ``training_dump_helpers`` to
keep ``_preload_all_embeddings`` and ``_build_reference_from_cache``
under the §3 60-LOC method ceiling. These tests pin their wire shape
without standing up a real database (they exercise SQL composition +
result-row handling against a mocked SQLAlchemy ``Connection``).
"""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import numpy as np

from protea.core._training_dump_loaders import (
    _count_embeddings_with_dim,
    _load_annotation_aggregations,
    _stream_embeddings,
)

# ---------------------------------------------------------------------------
# _count_embeddings_with_dim
# ---------------------------------------------------------------------------


class TestCountEmbeddingsWithDim:
    def test_returns_total_and_dim(self) -> None:
        conn = MagicMock()
        conn.execute.return_value.one.return_value = (5, 1024)

        total, dim = _count_embeddings_with_dim(conn, uuid.uuid4())

        assert total == 5
        assert dim == 1024

    def test_dim_falls_back_to_960_when_probe_returns_null(self) -> None:
        conn = MagicMock()
        conn.execute.return_value.one.return_value = (3, None)

        total, dim = _count_embeddings_with_dim(conn, uuid.uuid4())

        assert total == 3
        assert dim == 960


# ---------------------------------------------------------------------------
# _stream_embeddings
# ---------------------------------------------------------------------------


class TestStreamEmbeddings:
    def test_fills_preallocated_matrix_from_string_rows(self) -> None:
        conn = MagicMock()
        rows = [
            ("P1", "[0.1, 0.2, 0.3]"),
            ("P2", "[0.4, 0.5, 0.6]"),
        ]
        conn.execute.return_value.yield_per.return_value = iter(rows)

        embeddings, accessions = _stream_embeddings(
            conn, uuid.uuid4(), total=2, dim=3, stream_chunk=10
        )

        assert embeddings.shape == (2, 3)
        assert embeddings.dtype == np.float16
        np.testing.assert_allclose(
            embeddings[0], np.array([0.1, 0.2, 0.3], dtype=np.float16)
        )
        np.testing.assert_allclose(
            embeddings[1], np.array([0.4, 0.5, 0.6], dtype=np.float16)
        )
        assert accessions == ["P1", "P2"]

    def test_handles_array_like_rows(self) -> None:
        conn = MagicMock()
        rows = [
            ("Q1", [1.0, 2.0]),
            ("Q2", [3.0, 4.0]),
        ]
        conn.execute.return_value.yield_per.return_value = iter(rows)

        embeddings, accessions = _stream_embeddings(
            conn, uuid.uuid4(), total=2, dim=2, stream_chunk=5
        )

        np.testing.assert_allclose(embeddings[0], np.array([1.0, 2.0], dtype=np.float16))
        np.testing.assert_allclose(embeddings[1], np.array([3.0, 4.0], dtype=np.float16))
        assert accessions == ["Q1", "Q2"]


# ---------------------------------------------------------------------------
# _load_annotation_aggregations
# ---------------------------------------------------------------------------


class TestLoadAnnotationAggregations:
    def test_groups_by_aspect_for_known_accessions(self) -> None:
        conn = MagicMock()
        # 3 rows: P1 has F + C, P2 has P. Q1 has P but is filtered out
        # because it is missing from acc_to_idx.
        rows = [
            ("P1", "F", "GO:0001", None, "EXP"),
            ("P1", "C", "GO:0002", None, "EXP"),
            ("P2", "P", "GO:0003", None, "TAS"),
            ("Q1", "P", "GO:0004", None, "TAS"),
        ]
        conn.execute.return_value.yield_per.return_value = iter(rows)
        acc_to_idx = {"P1": 0, "P2": 1}

        aspect_accs, aspect_go_map = _load_annotation_aggregations(
            conn, uuid.uuid4(), acc_to_idx
        )

        assert aspect_accs["F"] == {"P1"}
        assert aspect_accs["C"] == {"P1"}
        assert aspect_accs["P"] == {"P2"}
        # Q1 was filtered out; no aspect should claim it.
        assert "Q1" not in aspect_accs["P"]

        assert aspect_go_map["F"]["P1"][0]["go_term_id"] == "GO:0001"
        assert aspect_go_map["C"]["P1"][0]["go_term_id"] == "GO:0002"
        assert aspect_go_map["P"]["P2"][0]["go_term_id"] == "GO:0003"

    def test_empty_rows_yield_empty_aggregations(self) -> None:
        conn = MagicMock()
        conn.execute.return_value.yield_per.return_value = iter([])

        aspect_accs, aspect_go_map = _load_annotation_aggregations(
            conn, uuid.uuid4(), acc_to_idx={"P1": 0}
        )

        assert aspect_accs == {"P": set(), "F": set(), "C": set()}
        assert aspect_go_map == {"P": {}, "F": {}, "C": {}}

    def test_multiple_annotations_per_protein_aspect_collected_in_list(self) -> None:
        conn = MagicMock()
        rows = [
            ("P1", "F", "GO:0001", "enables", "EXP"),
            ("P1", "F", "GO:0042", "enables", "TAS"),
        ]
        conn.execute.return_value.yield_per.return_value = iter(rows)

        _, aspect_go_map = _load_annotation_aggregations(
            conn, uuid.uuid4(), acc_to_idx={"P1": 0}
        )

        f_records = aspect_go_map["F"]["P1"]
        assert len(f_records) == 2
        assert {r["go_term_id"] for r in f_records} == {"GO:0001", "GO:0042"}
