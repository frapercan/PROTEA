"""Tests for protea.core.evaluation — pure-Python components + mocked DB tests."""
import uuid
from unittest.mock import MagicMock, patch

from protea.core.evaluation import (
    EvaluationData,
    _build_negative_keys,
    _get_descendants,
    _load_children_map,
    _load_experimental_annotations_by_ns,
    _load_go_maps,
    compute_evaluation_data,
)

# ---------------------------------------------------------------------------
# EvaluationData — dataclass properties
# ---------------------------------------------------------------------------

class TestEvaluationDataProperties:
    def _make(self, nk=None, lk=None, pk=None, known=None, pk_known=None):
        return EvaluationData(
            nk=nk or {},
            lk=lk or {},
            pk=pk or {},
            known=known or {},
            pk_known=pk_known or {},
        )

    def test_nk_proteins_count(self):
        ed = self._make(nk={"P1": {"GO:0001"}, "P2": {"GO:0002"}})
        assert ed.nk_proteins == 2

    def test_lk_proteins_count(self):
        ed = self._make(lk={"P3": {"GO:0003"}})
        assert ed.lk_proteins == 1

    def test_pk_proteins_count(self):
        ed = self._make(pk={"P4": {"GO:0004"}, "P5": {"GO:0005"}})
        assert ed.pk_proteins == 2

    def test_nk_annotations_count(self):
        ed = self._make(nk={"P1": {"GO:0001", "GO:0002"}, "P2": {"GO:0003"}})
        assert ed.nk_annotations == 3

    def test_lk_annotations_count(self):
        ed = self._make(lk={"P1": {"GO:0001"}, "P2": {"GO:0002", "GO:0003"}})
        assert ed.lk_annotations == 3

    def test_pk_annotations_count(self):
        ed = self._make(pk={"P1": {"GO:0001", "GO:0002"}})
        assert ed.pk_annotations == 2

    def test_known_terms_count(self):
        ed = self._make(known={"P1": {"GO:0001"}, "P2": {"GO:0002", "GO:0003"}})
        assert ed.known_terms_count == 3

    def test_delta_proteins_union(self):
        ed = self._make(
            nk={"P1": {"GO:0001"}},
            lk={"P2": {"GO:0002"}},
            pk={"P3": {"GO:0003"}},
        )
        assert ed.delta_proteins == 3

    def test_delta_proteins_overlapping(self):
        # Same protein can appear in lk and pk for different namespaces
        ed = self._make(
            nk={},
            lk={"P1": {"GO:0001"}},
            pk={"P1": {"GO:0002"}},
        )
        assert ed.delta_proteins == 1

    def test_empty_data(self):
        ed = self._make()
        assert ed.nk_proteins == 0
        assert ed.lk_proteins == 0
        assert ed.pk_proteins == 0
        assert ed.nk_annotations == 0
        assert ed.lk_annotations == 0
        assert ed.pk_annotations == 0
        assert ed.known_terms_count == 0
        assert ed.delta_proteins == 0

    def test_stats_dict_keys(self):
        ed = self._make()
        s = ed.stats()
        expected = {
            "delta_proteins", "nk_proteins", "lk_proteins", "pk_proteins",
            "nk_annotations", "lk_annotations", "pk_annotations", "known_terms_count",
        }
        assert set(s.keys()) == expected

    def test_stats_dict_values(self):
        ed = self._make(
            nk={"P1": {"GO:0001", "GO:0002"}},
            lk={"P2": {"GO:0003"}},
            pk={"P3": {"GO:0004"}},
            known={"P1": {"GO:0010"}},
        )
        s = ed.stats()
        assert s["nk_proteins"] == 1
        assert s["lk_proteins"] == 1
        assert s["pk_proteins"] == 1
        assert s["nk_annotations"] == 2
        assert s["lk_annotations"] == 1
        assert s["pk_annotations"] == 1
        assert s["known_terms_count"] == 1
        assert s["delta_proteins"] == 3


# ---------------------------------------------------------------------------
# _get_descendants — BFS over GO DAG
# ---------------------------------------------------------------------------

class TestGetDescendants:
    def test_no_children(self):
        result = _get_descendants(1, {})
        assert result == set()

    def test_direct_children(self):
        children_map = {1: {2, 3}}
        result = _get_descendants(1, children_map)
        assert result == {2, 3}

    def test_transitive_descendants(self):
        children_map = {1: {2}, 2: {3}, 3: {4}}
        result = _get_descendants(1, children_map)
        assert result == {2, 3, 4}

    def test_diamond_dag_no_duplicate(self):
        # 1 → 2, 3; 2 → 4; 3 → 4
        children_map = {1: {2, 3}, 2: {4}, 3: {4}}
        result = _get_descendants(1, children_map)
        assert result == {2, 3, 4}

    def test_cycle_safe(self):
        # Shouldn't happen in GO but must not infinite-loop
        children_map = {1: {2}, 2: {3}, 3: {1}}
        result = _get_descendants(1, children_map)
        assert 2 in result and 3 in result

    def test_start_term_not_in_result(self):
        children_map = {1: {2}}
        result = _get_descendants(1, children_map)
        assert 1 not in result

    def test_leaf_node(self):
        children_map = {1: {2}, 2: set()}
        result = _get_descendants(1, children_map)
        assert result == {2}


# ---------------------------------------------------------------------------
# _load_children_map — lines 124-137
# ---------------------------------------------------------------------------

class TestLoadChildrenMap:
    def test_loads_and_groups_by_parent(self):
        snap_id = uuid.uuid4()
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = [
            (10, 20),
            (10, 30),
            (20, 40),
        ]
        result = _load_children_map(mock_session, snap_id)
        assert result == {10: {20, 30}, 20: {40}}

    def test_empty_result(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = []
        result = _load_children_map(mock_session, uuid.uuid4())
        assert result == {}

    def test_passes_snapshot_id(self):
        snap_id = uuid.uuid4()
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = []
        _load_children_map(mock_session, snap_id)
        call_args = mock_session.execute.call_args
        assert call_args[0][1]["snap_id"] == snap_id

    def test_single_relationship(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = [(1, 2)]
        result = _load_children_map(mock_session, uuid.uuid4())
        assert result == {1: {2}}


# ---------------------------------------------------------------------------
# _load_go_maps — lines 161-169
# ---------------------------------------------------------------------------

class TestLoadGoMaps:
    def test_basic_maps(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = [
            (1, "GO:0001", "F"),
            (2, "GO:0002", "P"),
            (3, "GO:0003", "C"),
        ]
        id_map, aspect_map = _load_go_maps(mock_session, uuid.uuid4())
        assert id_map == {1: "GO:0001", 2: "GO:0002", 3: "GO:0003"}
        assert aspect_map == {1: "F", 2: "P", 3: "C"}

    def test_null_aspect_excluded_from_aspect_map(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = [
            (1, "GO:0001", "F"),
            (2, "GO:0002", None),
        ]
        id_map, aspect_map = _load_go_maps(mock_session, uuid.uuid4())
        assert id_map == {1: "GO:0001", 2: "GO:0002"}
        assert 2 not in aspect_map
        assert aspect_map == {1: "F"}

    def test_empty(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = []
        id_map, aspect_map = _load_go_maps(mock_session, uuid.uuid4())
        assert id_map == {}
        assert aspect_map == {}


# ---------------------------------------------------------------------------
# _build_negative_keys — lines 182-204
# ---------------------------------------------------------------------------

class TestBuildNegativeKeys:
    def test_no_not_annotations(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = []
        result = _build_negative_keys(mock_session, [uuid.uuid4()], {})
        assert result == set()

    def test_single_not_no_descendants(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = [("P1", 100)]
        result = _build_negative_keys(mock_session, [uuid.uuid4()], {})
        assert result == {("P1", 100)}

    def test_not_with_descendants(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = [("P1", 100)]
        children_map = {100: {200, 300}, 200: {400}}
        result = _build_negative_keys(mock_session, [uuid.uuid4()], children_map)
        assert result == {("P1", 100), ("P1", 200), ("P1", 300), ("P1", 400)}

    def test_multiple_proteins(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = [
            ("P1", 10),
            ("P2", 20),
        ]
        children_map = {10: {11}}
        result = _build_negative_keys(mock_session, [uuid.uuid4()], children_map)
        assert ("P1", 10) in result
        assert ("P1", 11) in result
        assert ("P2", 20) in result

    def test_duplicate_rows_deduplicated(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = [
            ("P1", 10),
            ("P1", 10),
        ]
        result = _build_negative_keys(mock_session, [uuid.uuid4()], {})
        assert result == {("P1", 10)}

    def test_passes_set_ids(self):
        ids = [uuid.uuid4(), uuid.uuid4()]
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = []
        _build_negative_keys(mock_session, ids, {})
        call_args = mock_session.execute.call_args
        assert call_args[0][1]["set_ids"] == ids


# ---------------------------------------------------------------------------
# _load_experimental_annotations_by_ns — lines 219-238
# ---------------------------------------------------------------------------

class TestLoadExperimentalAnnotationsByNs:
    def _go_id_map(self):
        return {100: "GO:0001", 200: "GO:0002", 300: "GO:0003", 400: "GO:0004"}

    def _aspect_map(self):
        return {100: "F", 200: "P", 300: "C", 400: "F"}

    def test_groups_by_protein_and_namespace(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = [
            ("P1", 100),
            ("P1", 200),
            ("P2", 300),
        ]
        result = _load_experimental_annotations_by_ns(
            mock_session, uuid.uuid4(), set(), self._go_id_map(), self._aspect_map()
        )
        assert result["P1"]["F"] == {"GO:0001"}
        assert result["P1"]["P"] == {"GO:0002"}
        assert result["P2"]["C"] == {"GO:0003"}

    def test_negative_keys_excluded(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = [
            ("P1", 100),
            ("P1", 200),
        ]
        negative_keys = {("P1", 100)}
        result = _load_experimental_annotations_by_ns(
            mock_session, uuid.uuid4(), negative_keys, self._go_id_map(), self._aspect_map()
        )
        assert "F" not in result.get("P1", {})
        assert result["P1"]["P"] == {"GO:0002"}

    def test_missing_go_id_skipped(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = [("P1", 999)]
        result = _load_experimental_annotations_by_ns(
            mock_session, uuid.uuid4(), set(), self._go_id_map(), self._aspect_map()
        )
        assert result == {}

    def test_missing_aspect_skipped(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = [("P1", 100)]
        result = _load_experimental_annotations_by_ns(
            mock_session, uuid.uuid4(), set(), {100: "GO:0001"}, {}
        )
        assert result == {}

    def test_empty_rows(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = []
        result = _load_experimental_annotations_by_ns(
            mock_session, uuid.uuid4(), set(), {}, {}
        )
        assert result == {}

    def test_multiple_terms_same_namespace(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = [
            ("P1", 100),
            ("P1", 400),  # also F namespace
        ]
        result = _load_experimental_annotations_by_ns(
            mock_session, uuid.uuid4(), set(), self._go_id_map(), self._aspect_map()
        )
        assert result["P1"]["F"] == {"GO:0001", "GO:0004"}


# ---------------------------------------------------------------------------
# compute_evaluation_data — lines 265-322
# ---------------------------------------------------------------------------

class TestComputeEvaluationData:
    def _ids(self):
        return uuid.uuid4(), uuid.uuid4(), uuid.uuid4()

    @patch("protea.core.evaluation._load_experimental_annotations_by_ns")
    @patch("protea.core.evaluation._build_negative_keys")
    @patch("protea.core.evaluation._load_children_map")
    @patch("protea.core.evaluation._load_go_maps")
    def test_nk_protein(self, mock_go_maps, mock_children, mock_neg, mock_annots):
        """Protein with no old annotations -> NK."""
        old_id, new_id, snap_id = self._ids()
        mock_go_maps.return_value = ({}, {})
        mock_children.return_value = {}
        mock_neg.return_value = set()
        mock_annots.side_effect = [
            {},  # old
            {"P1": {"F": {"GO:0001", "GO:0002"}}},  # new
        ]
        result = compute_evaluation_data(MagicMock(), old_id, new_id, snap_id)
        assert result.nk == {"P1": {"GO:0001", "GO:0002"}}
        assert result.lk == {}
        assert result.pk == {}

    @patch("protea.core.evaluation._load_experimental_annotations_by_ns")
    @patch("protea.core.evaluation._build_negative_keys")
    @patch("protea.core.evaluation._load_children_map")
    @patch("protea.core.evaluation._load_go_maps")
    def test_lk_protein(self, mock_go_maps, mock_children, mock_neg, mock_annots):
        """Protein had F at t0, gains P (no old P) -> LK in P."""
        old_id, new_id, snap_id = self._ids()
        mock_go_maps.return_value = ({}, {})
        mock_children.return_value = {}
        mock_neg.return_value = set()
        mock_annots.side_effect = [
            {"P1": {"F": {"GO:0001"}}},
            {"P1": {"F": {"GO:0001"}, "P": {"GO:0002"}}},
        ]
        result = compute_evaluation_data(MagicMock(), old_id, new_id, snap_id)
        assert result.nk == {}
        assert result.lk == {"P1": {"GO:0002"}}
        assert result.pk == {}

    @patch("protea.core.evaluation._load_experimental_annotations_by_ns")
    @patch("protea.core.evaluation._build_negative_keys")
    @patch("protea.core.evaluation._load_children_map")
    @patch("protea.core.evaluation._load_go_maps")
    def test_pk_protein(self, mock_go_maps, mock_children, mock_neg, mock_annots):
        """Protein had F at t0, gains new F -> PK in F."""
        old_id, new_id, snap_id = self._ids()
        mock_go_maps.return_value = ({}, {})
        mock_children.return_value = {}
        mock_neg.return_value = set()
        mock_annots.side_effect = [
            {"P1": {"F": {"GO:0001"}}},
            {"P1": {"F": {"GO:0001", "GO:0002"}}},
        ]
        result = compute_evaluation_data(MagicMock(), old_id, new_id, snap_id)
        assert result.nk == {}
        assert result.lk == {}
        assert result.pk == {"P1": {"GO:0002"}}
        assert result.pk_known == {"P1": {"GO:0001"}}

    @patch("protea.core.evaluation._load_experimental_annotations_by_ns")
    @patch("protea.core.evaluation._build_negative_keys")
    @patch("protea.core.evaluation._load_children_map")
    @patch("protea.core.evaluation._load_go_maps")
    def test_mixed_lk_and_pk(self, mock_go_maps, mock_children, mock_neg, mock_annots):
        """Same protein: PK in F, LK in C."""
        old_id, new_id, snap_id = self._ids()
        mock_go_maps.return_value = ({}, {})
        mock_children.return_value = {}
        mock_neg.return_value = set()
        mock_annots.side_effect = [
            {"P1": {"F": {"GO:0001"}}},
            {"P1": {"F": {"GO:0001", "GO:0002"}, "C": {"GO:0003"}}},
        ]
        result = compute_evaluation_data(MagicMock(), old_id, new_id, snap_id)
        assert result.pk == {"P1": {"GO:0002"}}
        assert result.lk == {"P1": {"GO:0003"}}
        assert result.pk_known == {"P1": {"GO:0001"}}

    @patch("protea.core.evaluation._load_experimental_annotations_by_ns")
    @patch("protea.core.evaluation._build_negative_keys")
    @patch("protea.core.evaluation._load_children_map")
    @patch("protea.core.evaluation._load_go_maps")
    def test_no_new_annotations(self, mock_go_maps, mock_children, mock_neg, mock_annots):
        """Protein only in old -> skipped (no new_all)."""
        old_id, new_id, snap_id = self._ids()
        mock_go_maps.return_value = ({}, {})
        mock_children.return_value = {}
        mock_neg.return_value = set()
        mock_annots.side_effect = [
            {"P1": {"F": {"GO:0001"}}},
            {},
        ]
        result = compute_evaluation_data(MagicMock(), old_id, new_id, snap_id)
        assert result.nk == {}
        assert result.lk == {}
        assert result.pk == {}
        assert result.delta_proteins == 0

    @patch("protea.core.evaluation._load_experimental_annotations_by_ns")
    @patch("protea.core.evaluation._build_negative_keys")
    @patch("protea.core.evaluation._load_children_map")
    @patch("protea.core.evaluation._load_go_maps")
    def test_no_delta_same_terms(self, mock_go_maps, mock_children, mock_neg, mock_annots):
        """Old and new identical -> no delta."""
        old_id, new_id, snap_id = self._ids()
        mock_go_maps.return_value = ({}, {})
        mock_children.return_value = {}
        mock_neg.return_value = set()
        mock_annots.side_effect = [
            {"P1": {"F": {"GO:0001"}}},
            {"P1": {"F": {"GO:0001"}}},
        ]
        result = compute_evaluation_data(MagicMock(), old_id, new_id, snap_id)
        assert result.nk == {}
        assert result.lk == {}
        assert result.pk == {}

    @patch("protea.core.evaluation._load_experimental_annotations_by_ns")
    @patch("protea.core.evaluation._build_negative_keys")
    @patch("protea.core.evaluation._load_children_map")
    @patch("protea.core.evaluation._load_go_maps")
    def test_known_includes_all_old(self, mock_go_maps, mock_children, mock_neg, mock_annots):
        """known dict contains all old experimental annotations flattened."""
        old_id, new_id, snap_id = self._ids()
        mock_go_maps.return_value = ({}, {})
        mock_children.return_value = {}
        mock_neg.return_value = set()
        mock_annots.side_effect = [
            {"P1": {"F": {"GO:0001"}, "P": {"GO:0002"}}, "P2": {"C": {"GO:0003"}}},
            {"P1": {"F": {"GO:0001"}, "P": {"GO:0002", "GO:0099"}}},
        ]
        result = compute_evaluation_data(MagicMock(), old_id, new_id, snap_id)
        assert result.known == {"P1": {"GO:0001", "GO:0002"}, "P2": {"GO:0003"}}

    @patch("protea.core.evaluation._load_experimental_annotations_by_ns")
    @patch("protea.core.evaluation._build_negative_keys")
    @patch("protea.core.evaluation._load_children_map")
    @patch("protea.core.evaluation._load_go_maps")
    def test_multiple_proteins(self, mock_go_maps, mock_children, mock_neg, mock_annots):
        """Multiple proteins with different categories."""
        old_id, new_id, snap_id = self._ids()
        mock_go_maps.return_value = ({}, {})
        mock_children.return_value = {}
        mock_neg.return_value = set()
        mock_annots.side_effect = [
            {"P_old": {"F": {"GO:0001"}}},
            {"P_old": {"F": {"GO:0001", "GO:0002"}}, "P_nk": {"P": {"GO:0010"}}},
        ]
        result = compute_evaluation_data(MagicMock(), old_id, new_id, snap_id)
        assert result.nk == {"P_nk": {"GO:0010"}}
        assert result.pk == {"P_old": {"GO:0002"}}
        assert result.pk_known == {"P_old": {"GO:0001"}}

    @patch("protea.core.evaluation._load_experimental_annotations_by_ns")
    @patch("protea.core.evaluation._build_negative_keys")
    @patch("protea.core.evaluation._load_children_map")
    @patch("protea.core.evaluation._load_go_maps")
    def test_protein_with_empty_new_namespaces(self, mock_go_maps, mock_children, mock_neg, mock_annots):
        """Protein key in new but no namespace data -> new_all empty -> skip."""
        old_id, new_id, snap_id = self._ids()
        mock_go_maps.return_value = ({}, {})
        mock_children.return_value = {}
        mock_neg.return_value = set()
        mock_annots.side_effect = [
            {},
            {"P1": {}},
        ]
        result = compute_evaluation_data(MagicMock(), old_id, new_id, snap_id)
        assert result.nk == {}
        assert result.lk == {}
        assert result.pk == {}

    @patch("protea.core.evaluation._load_experimental_annotations_by_ns")
    @patch("protea.core.evaluation._build_negative_keys")
    @patch("protea.core.evaluation._load_children_map")
    @patch("protea.core.evaluation._load_go_maps")
    def test_all_three_namespaces_pk(self, mock_go_maps, mock_children, mock_neg, mock_annots):
        """All three namespaces (F, P, C) gain new terms -> PK in all."""
        old_id, new_id, snap_id = self._ids()
        mock_go_maps.return_value = ({}, {})
        mock_children.return_value = {}
        mock_neg.return_value = set()
        mock_annots.side_effect = [
            {"P1": {"F": {"GO:F1"}, "P": {"GO:P1"}, "C": {"GO:C1"}}},
            {"P1": {"F": {"GO:F1", "GO:F2"}, "P": {"GO:P1", "GO:P2"}, "C": {"GO:C1", "GO:C2"}}},
        ]
        result = compute_evaluation_data(MagicMock(), old_id, new_id, snap_id)
        assert result.pk == {"P1": {"GO:F2", "GO:P2", "GO:C2"}}
        assert result.pk_known == {"P1": {"GO:F1", "GO:P1", "GO:C1"}}

    @patch("protea.core.evaluation._load_experimental_annotations_by_ns")
    @patch("protea.core.evaluation._build_negative_keys")
    @patch("protea.core.evaluation._load_children_map")
    @patch("protea.core.evaluation._load_go_maps")
    def test_both_empty(self, mock_go_maps, mock_children, mock_neg, mock_annots):
        """Both old and new empty -> empty result."""
        old_id, new_id, snap_id = self._ids()
        mock_go_maps.return_value = ({}, {})
        mock_children.return_value = {}
        mock_neg.return_value = set()
        mock_annots.side_effect = [{}, {}]
        result = compute_evaluation_data(MagicMock(), old_id, new_id, snap_id)
        assert result.delta_proteins == 0
        assert result.known == {}
