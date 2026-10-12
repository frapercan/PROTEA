"""The donor bank's ORDER is part of the result, not housekeeping.

91 per cent of queries have neighbours at distance ~0 -- themselves, or twins
carrying the same sequence -- so candidate terms tie at the k-th slot and the
search admits whichever donor comes first in the bank. Two places decided that
order by accident:

* the unified pool query had no ``ORDER BY``, so the order was whatever the
  plan emitted. Measured on the 219 bank, 142.422 rows, identical rows: a hash
  join and a merge join gave different orders, and the merge join's order was
  exactly accession order. That alone moved ``f_micro_w`` by ~1e-3 per cell.
* the per-aspect index array was built by iterating a ``set`` of accession
  STRINGS. CPython randomises string hashing per process unless
  PYTHONHASHSEED is pinned, and it is not pinned here, so that order changed
  on every worker restart that rebuilt the cache.

The disk cache hid both: an identical re-run reproduced to the pair, while a
run after the cache expired would not have. These tests pin the order instead.
"""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import numpy as np
from protea_contracts.payloads import DonorPolicy
from sqlalchemy.orm import Session

from protea.core.operations.predict_go_terms import _batch_op_reference as refmod
from protea.core.operations.predict_go_terms._batch_op_reference import (
    _PoolRequest,
    _ReferenceMixin,
)

CFG = uuid.UUID("11111111-1111-1111-1111-111111111111")
ANN = uuid.UUID("22222222-2222-2222-2222-222222222222")


def _sql(request: _PoolRequest) -> str:
    query = _ReferenceMixin()._reference_pool_query(Session(), request)
    return str(query.statement.compile(compile_kwargs={"literal_binds": True}))


def _assert_orders_the_outer_select(sql: str) -> None:
    """The ORDER BY must be the outer query's, and there must be exactly one.

    Asserting ``"protein.accession" in sql.split("order by")[1]`` is NOT enough:
    ``split`` takes everything after the FIRST occurrence, and an ORDER BY placed
    inside the annotated subquery is followed by
    ``) as anon_1 on protein.accession = anon_1.protein_accession``, which
    contains that substring. So three tests passed with the order pinned on the
    wrong select, where it orders nothing the pool sees.
    """
    low = sql.lower()
    assert low.count("order by") == 1, "exactly one ORDER BY, on the outer select"
    tail = low.rsplit("order by", 1)[1]
    assert "protein.accession" in tail
    assert "collate" in tail, 'codepoint order, so COLLATE "C" must be in it'
    assert "chunk_index_s" in tail, "accession alone is not a total order"
    # Nothing may follow it: a trailing join or filter would mean the ORDER BY
    # sits on an inner select.
    assert ")" not in tail.split("chunk_index_s")[-1]


class TestTheUnifiedPoolIsOrdered:
    def test_it_orders_by_accession(self) -> None:
        _assert_orders_the_outer_select(_sql(_PoolRequest(CFG, ANN)))

    def test_a_restricted_pool_is_ordered_too(self) -> None:
        """The order must not depend on which policy narrowed the pool."""
        request = _PoolRequest(CFG, ANN, DonorPolicy(evidence_codes=["IDA", "EXP"]))
        _assert_orders_the_outer_select(_sql(request))

    def test_a_reviewed_only_pool_is_ordered_too(self) -> None:
        request = _PoolRequest(CFG, ANN, DonorPolicy(reviewed_only=True))
        sql = _sql(request)
        assert "reviewed" in sql.lower()
        _assert_orders_the_outer_select(sql)

    def test_it_is_not_pinned_on_the_annotated_subquery(self) -> None:
        """The ORDER BY must outlive the join, not sit inside the subquery."""
        low = _sql(_PoolRequest(CFG, ANN)).lower()
        antes, _, despues = low.rpartition("order by")
        assert "join" in antes, "the join comes before the ORDER BY"
        assert "join" not in despues, "nothing joins after it"


class TestThePerAspectIndexIsOrdered:
    """Built from the accessions in accession order, whatever order they arrive in.

    The collector is stubbed to hand them over in REVERSE accession order, so
    this is deterministic: iterating the input unchanged would write the
    reverse, and only sorting writes accession order. No reliance on whatever
    order a real ``set`` happens to produce.
    """

    ACCS = ["A0A001", "B0B002", "C0C003", "P12345", "Q9Y6K1"]

    def _run(self, tmp_path):
        from protea.core.operations.predict_go_terms._batch_op import (
            PredictGOTermsBatchOperation,
        )

        # The unified pool is in accession order; acc_to_idx is deliberately
        # NOT, so a test that passes cannot be passing by coincidence.
        unified = list(self.ACCS)
        acc_to_idx = {acc: len(unified) - 1 - i for i, acc in enumerate(unified)}
        reverse = list(reversed(self.ACCS))

        written: dict[str, np.ndarray] = {}

        def fake_save(path, arr):
            written[str(path)] = np.asarray(arr)

        with (
            patch.object(
                PredictGOTermsBatchOperation,
                "_collect_aspect_annotations",
                staticmethod(lambda *a, **k: ({"P": reverse}, {"P": {}})),
            ),
            patch.object(refmod, "_aspect_index_path", lambda *a: tmp_path / "idx.npy"),
            patch.object(
                refmod, "_aspect_index_pool_path", lambda *a: tmp_path / "idx.pool"
            ),
            patch.object(refmod, "pool_fingerprint", lambda accs: "fp"),
            patch.object(refmod, "_build_anno_csr", lambda accs, m: {"accs": list(accs)}),
            patch.object(refmod, "_save_anno_csr_to_disk", lambda *a, **k: None),
            patch.object(np, "save", fake_save),
        ):
            refmod._ReferenceMixin._query_and_persist_aspect_caches(
                Session(), ["P"], unified, acc_to_idx, _PoolRequest(CFG, ANN)
            )
        return written, acc_to_idx

    def test_the_indices_follow_accession_order(self, tmp_path) -> None:
        written, acc_to_idx = self._run(tmp_path)
        assert len(written) == 1
        indices = next(iter(written.values()))
        expected = [acc_to_idx[acc] for acc in sorted(self.ACCS)]
        assert list(indices) == expected

    def test_it_does_not_keep_the_order_it_was_handed(self, tmp_path) -> None:
        """The regression, and it fails deterministically without the sort.

        The collector is stubbed to hand the accessions over in REVERSE order,
        which is exactly what this asserts the output is NOT, so reverting
        ``sorted(accessions)`` makes it fail every time rather than once in a
        while. That is the point of stubbing the order instead of trusting a
        real ``set`` to come out shuffled.
        """
        written, acc_to_idx = self._run(tmp_path)
        indices = next(iter(written.values()))
        handed = [acc_to_idx[acc] for acc in reversed(self.ACCS)]
        assert list(indices) != handed

    def test_the_indices_stay_int32(self, tmp_path) -> None:
        written, _ = self._run(tmp_path)
        assert next(iter(written.values())).dtype == np.int32


class TestAspectIndicesDirectly:
    """The helper on its own, including what it does with an unknown accession."""

    def test_it_sorts_whatever_iterable_it_is_given(self) -> None:
        accs = ["Q9Y6K1", "A0A001", "P12345"]
        acc_to_idx = {"A0A001": 7, "P12345": 3, "Q9Y6K1": 0}
        assert list(refmod._aspect_indices(accs, acc_to_idx)) == [7, 3, 0]
        assert list(refmod._aspect_indices(reversed(accs), acc_to_idx)) == [7, 3, 0]
        assert list(refmod._aspect_indices(set(accs), acc_to_idx)) == [7, 3, 0]

    def test_an_accession_outside_the_pool_is_dropped_not_mapped(self) -> None:
        """An aspect can name a protein the unified pool does not carry."""
        out = refmod._aspect_indices(["B0B002", "A0A001"], {"A0A001": 1})
        assert list(out) == [1]

    def test_an_empty_aspect_gives_an_empty_int32_array(self) -> None:
        out = refmod._aspect_indices([], {"A0A001": 1})
        assert len(out) == 0
        assert out.dtype == np.int32


class TestThePerDonorAnnotationListIsOrdered:
    """The same defect one level down, and it reaches the score.

    These lists are flattened into the on-disk CSR positionally, and a candidate
    takes its ``evidence_code`` from the first matching row -- and the code
    multiplies the score. So an unpinned list order is an unpinned score.
    """

    def test_it_sorts_by_term_then_code_then_qualifier(self) -> None:
        mapa = {
            "P": {
                "A0A001": [
                    {"go_term_id": 9, "evidence_code": "IDA", "qualifier": None},
                    {"go_term_id": 2, "evidence_code": "IPI", "qualifier": "enables"},
                    {"go_term_id": 2, "evidence_code": "EXP", "qualifier": "enables"},
                ]
            }
        }
        refmod._pin_annotation_order(mapa)
        assert [a["go_term_id"] for a in mapa["P"]["A0A001"]] == [2, 2, 9]
        assert [a["evidence_code"] for a in mapa["P"]["A0A001"]] == ["EXP", "IPI", "IDA"]

    def test_a_null_qualifier_does_not_raise(self) -> None:
        """``qualifier`` is nullable and ``evidence_code`` can be absent too."""
        mapa = {
            "F": {
                "Q1": [
                    {"go_term_id": 1, "evidence_code": None, "qualifier": None},
                    {"go_term_id": 1, "evidence_code": "IDA", "qualifier": None},
                ]
            }
        }
        refmod._pin_annotation_order(mapa)
        assert [a["evidence_code"] for a in mapa["F"]["Q1"]] == [None, "IDA"]

    def test_it_is_stable_whatever_order_it_is_handed(self) -> None:
        base = [
            {"go_term_id": i, "evidence_code": c, "qualifier": None}
            for i, c in ((5, "IDA"), (1, "EXP"), (3, "IPI"), (1, "IMP"))
        ]
        a = {"P": {"X": list(base)}}
        b = {"P": {"X": list(reversed(base))}}
        refmod._pin_annotation_order(a)
        refmod._pin_annotation_order(b)
        assert a["P"]["X"] == b["P"]["X"]

    def test_an_empty_map_is_fine(self) -> None:
        mapa: dict = {"P": {}, "F": {"Q1": []}}
        refmod._pin_annotation_order(mapa)
        assert mapa == {"P": {}, "F": {"Q1": []}}


class TestTheBankOrderCriteria:
    def test_it_is_accession_then_chunk(self) -> None:
        criteria = [str(c) for c in refmod._bank_order()]
        assert len(criteria) == 2
        assert "accession" in criteria[0]
        assert "chunk_index_s" in criteria[1]

    def test_the_accession_key_is_codepoint_not_the_db_collation(self) -> None:
        """glibc en_US.utf8 puts A0A18 before A0A1-9; Python's sorted does not."""
        assert 'COLLATE "C"' in str(refmod._bank_order()[0])


class TestTheCollectorActuallyPinsTheOrder:
    """The wiring, not just the helper.

    Every test above exercises ``_pin_annotation_order`` directly, so deleting
    its CALL from ``_collect_aspect_annotations`` left all of them passing. That
    is the same gap that let the real defects sit: a helper that works, never
    reached.
    """

    ROWS = [
        ("A0A001", "P", 9, None, "IDA"),
        ("A0A001", "P", 2, "enables", "IPI"),
        ("A0A001", "P", 2, "enables", "EXP"),
        ("B0B002", "P", 4, None, "IMP"),
    ]

    def _collect(self):
        from protea.core.operations.predict_go_terms._batch_op import (
            PredictGOTermsBatchOperation,
        )

        class _Rows(list):
            def yield_per(self, _n):
                return self

        query = _Rows(self.ROWS)
        query.join = lambda *a, **k: query
        query.filter = lambda *a, **k: query
        session = MagicMock()
        session.query.return_value = query
        with patch.object(refmod, "_restrict_annotations", lambda q, p: q):
            return PredictGOTermsBatchOperation._collect_aspect_annotations(
                session, ["P"], ANN, None
            )

    def test_the_lists_come_back_sorted(self) -> None:
        """Rows arrive unsorted; the collector must not hand them over that way."""
        _, go_map = self._collect()
        assert [a["go_term_id"] for a in go_map["P"]["A0A001"]] == [2, 2, 9]
        assert [a["evidence_code"] for a in go_map["P"]["A0A001"]] == [
            "EXP",
            "IPI",
            "IDA",
        ]

    def test_it_still_collects_the_accessions(self) -> None:
        accset, go_map = self._collect()
        assert accset["P"] == {"A0A001", "B0B002"}
        assert set(go_map["P"]) == {"A0A001", "B0B002"}
