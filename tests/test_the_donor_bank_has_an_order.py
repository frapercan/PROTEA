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
from unittest.mock import patch

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


class TestTheUnifiedPoolIsOrdered:
    def test_it_orders_by_accession(self) -> None:
        sql = _sql(_PoolRequest(CFG, ANN)).lower()
        assert "order by" in sql
        assert "protein.accession" in sql.split("order by")[1]

    def test_a_restricted_pool_is_ordered_too(self) -> None:
        """The order must not depend on which policy narrowed the pool."""
        request = _PoolRequest(CFG, ANN, DonorPolicy(evidence_codes=["IDA", "EXP"]))
        sql = _sql(request).lower()
        assert "order by" in sql
        assert "protein.accession" in sql.split("order by")[1]

    def test_a_reviewed_only_pool_is_ordered_too(self) -> None:
        request = _PoolRequest(CFG, ANN, DonorPolicy(reviewed_only=True))
        sql = _sql(request).lower()
        assert "reviewed" in sql
        assert "order by" in sql
        assert "protein.accession" in sql.split("order by")[1]

    def test_the_order_is_last_so_it_orders_the_whole_pool(self) -> None:
        """Not inside the annotated subquery, which would order nothing useful."""
        sql = _sql(_PoolRequest(CFG, ANN)).lower()
        assert sql.count("order by") == 1
        assert sql.rstrip().endswith("protein.accession")


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
        """The regression: this passed trivially while the set order leaked through."""
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
