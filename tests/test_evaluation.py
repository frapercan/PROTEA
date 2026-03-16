"""Tests for protea.core.evaluation — pure-Python components."""
import pytest

from protea.core.evaluation import EvaluationData, _get_descendants


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
