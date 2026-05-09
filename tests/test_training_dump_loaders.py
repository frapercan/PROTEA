"""Unit tests for ``protea.core._training_dump_loaders``.

The first three helpers (``_count_embeddings_with_dim``,
``_stream_embeddings``, ``_load_annotation_aggregations``) were
extracted out of ``_preload_all_embeddings`` /
``_build_reference_from_cache`` to keep those under the §3 60-LOC
method ceiling.

The next four (``_resolve_annotation_set_ids``,
``_check_reranker_name_collisions``, ``_load_ia_weights``,
``_load_go_maps``) were extracted out of
``TrainRerankerAutoOperation.execute`` for T2B.5 partial #1 to start
chipping the 670-LOC method down toward the §3 ceiling.

These tests pin each helper's wire shape without standing up a real
database (they exercise SQL composition + result-row handling against
a mocked SQLAlchemy ``Session`` / ``Connection``).
"""
from __future__ import annotations

import uuid
from unittest.mock import MagicMock

import numpy as np
import pytest

from protea.core._training_dump_loaders import (
    _check_reranker_name_collisions,
    _count_embeddings_with_dim,
    _load_annotation_aggregations,
    _load_go_maps,
    _load_ia_weights,
    _resolve_annotation_set_ids,
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


# ---------------------------------------------------------------------------
# T2B.5 partial #1 helpers (extracted from TrainRerankerAutoOperation.execute)
# ---------------------------------------------------------------------------


class TestResolveAnnotationSetIds:
    def test_returns_two_dicts_keyed_by_version(self) -> None:
        """``(version → set_id, version → native_snapshot_id)`` for every version."""
        aset160 = MagicMock()
        aset160.id = uuid.uuid4()
        aset160.ontology_snapshot_id = uuid.uuid4()
        aset170 = MagicMock()
        aset170.id = uuid.uuid4()
        aset170.ontology_snapshot_id = uuid.uuid4()

        session = MagicMock()
        # ``.filter(...).first()`` is called once per version.
        session.query.return_value.filter.return_value.first.side_effect = [
            aset160,
            aset170,
        ]

        v_to_set, v_to_native = _resolve_annotation_set_ids(session, "goa", [160, 170])

        assert v_to_set == {160: aset160.id, 170: aset170.id}
        assert v_to_native == {
            160: aset160.ontology_snapshot_id,
            170: aset170.ontology_snapshot_id,
        }

    def test_missing_version_raises_value_error(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.first.return_value = None

        with pytest.raises(ValueError, match="AnnotationSet not found"):
            _resolve_annotation_set_ids(session, "goa", [999])


class TestCheckRerankerNameCollisions:
    def test_passes_when_no_existing_rows(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.first.return_value = None
        # No raise = pass.
        _check_reranker_name_collisions(session, ["foo-NK", "foo-LK", "foo-PK"])

    def test_raises_with_quoted_name_on_collision(self) -> None:
        session = MagicMock()
        session.query.return_value.filter.return_value.first.return_value = MagicMock()

        with pytest.raises(ValueError, match="RerankerModel 'foo-NK' already exists"):
            _check_reranker_name_collisions(session, ["foo-NK"])


class TestLoadIAWeights:
    def test_returns_none_for_empty_path(self, tmp_path) -> None:
        assert _load_ia_weights(None) is None
        assert _load_ia_weights("") is None

    def test_parses_tab_separated_lines(self, tmp_path) -> None:
        ia_file = tmp_path / "ia.tsv"
        ia_file.write_text("GO:0008150\t1.0\nGO:0003674\t2.5\n")
        weights = _load_ia_weights(str(ia_file))
        assert weights == {"GO:0008150": 1.0, "GO:0003674": 2.5}

    def test_skips_blank_and_short_lines(self, tmp_path) -> None:
        ia_file = tmp_path / "ia.tsv"
        ia_file.write_text("GO:0008150\t1.0\n\nbad-row-no-tab\nGO:0003674\t2.5\n")
        weights = _load_ia_weights(str(ia_file))
        # Only the two well-formed rows survive.
        assert weights == {"GO:0008150": 1.0, "GO:0003674": 2.5}


class TestLoadGoMaps:
    def test_returns_id_maps_plus_pivot_set(self) -> None:
        session = MagicMock()
        # First execute() returns the union rows; second returns the pivot rows.
        union_result = MagicMock()
        union_result.fetchall.return_value = [
            (1, "GO:0001", "P"),
            (2, "GO:0002", "F"),
            (3, "GO:0003", None),  # filtered out of aspect_map
        ]
        pivot_result = MagicMock()
        pivot_result.fetchall.return_value = [("GO:0001",), ("GO:0002",)]
        session.execute.side_effect = [union_result, pivot_result]

        snap = uuid.uuid4()
        native = {uuid.uuid4()}
        go_id_map, aspect_map, pivot_go_ids = _load_go_maps(session, snap, native)

        assert go_id_map == {1: "GO:0001", 2: "GO:0002", 3: "GO:0003"}
        # ``aspect=None`` row is skipped from the aspect map.
        assert aspect_map == {1: "P", 2: "F"}
        assert pivot_go_ids == {"GO:0001", "GO:0002"}
