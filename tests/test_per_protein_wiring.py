"""The per-protein table has to actually get written by a real run.

The transformation layer has its own tests. This one covers the wiring, which
has a different failure mode: everything succeeds, the aggregate is correct, and
the extra table is silently absent or empty. Three ways that happens.

The namespace keys disagree. ``parse_results`` returns the short CAFA code and
the sink reports cafaeval's long namespace, so a mismatch yields an empty tau
map, no rows, and no file, with nothing anywhere saying why.

A failure in the extra work takes down the run. The aggregate is the product; a
supplementary table that cannot be written must not discard a sound evaluation.

An absent file looks the same as one nobody asked for. Both empty and failed
emit, so the two are distinguishable after the fact.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from protea.core.operations._run_cafa_eval_driver import _persist_per_protein
from protea.core.operations._run_cafa_helpers import _NS_LABELS


class _Ctx:
    def __init__(self, root: Path, th_step: float = 0.01):
        self.artifacts_root = root
        self.th_step = th_step


def _events(emit: MagicMock) -> list[str]:
    return [c.args[0] for c in emit.call_args_list]


class TestTheNamespaceKeysLineUp:
    def test_the_short_codes_parse_results_returns_all_invert(self) -> None:
        """If this drifts, the tau map is empty and no table is ever written."""
        short_codes = set(_NS_LABELS.values())
        assert short_codes == {"BPO", "MFO", "CCO"}
        assert len(set(_NS_LABELS.keys())) == len(short_codes)

    def test_an_empty_sink_says_so_rather_than_passing_quietly(self, tmp_path) -> None:
        emit = MagicMock()
        sink = MagicMock(records=[])
        _persist_per_protein(
            _Ctx(tmp_path), "nk", sink, {"BPO": {"tau": 0.3}}, emit
        )
        assert "run_cafa_evaluation.per_protein_empty" in _events(emit)
        assert not list(tmp_path.glob("**/per_protein.parquet"))


class TestTheExtraTableNeverTakesDownTheRun:
    def test_a_failure_is_emitted_and_swallowed(self, tmp_path) -> None:
        """The aggregate is the product; this table is supplementary."""
        emit = MagicMock()
        broken = MagicMock()
        type(broken).records = property(lambda self: (_ for _ in ()).throw(RuntimeError("boom")))
        _persist_per_protein(_Ctx(tmp_path), "nk", broken, {"BPO": {"tau": 0.3}}, emit)
        assert "run_cafa_evaluation.per_protein_failed" in _events(emit)

    def test_a_result_without_any_tau_does_not_raise(self, tmp_path) -> None:
        emit = MagicMock()
        _persist_per_protein(_Ctx(tmp_path), "nk", MagicMock(records=[]), {}, emit)
        assert "run_cafa_evaluation.per_protein_empty" in _events(emit)


class TestAWrittenTableIsAnnounced:
    def test_it_writes_the_parquet_and_says_where(self, tmp_path) -> None:
        import numpy as np

        emit = MagicMock()
        sink = MagicMock(records=[{
            "tp_at_tau": np.array([[1.0]]),
            "pred_at_tau": np.array([[2.0]]),
            "n_gt": np.array([2.0]),
            "ids": {"P00001": 0},
            "row_index": np.array([0]),
            "ns": "biological_process",
            "variant": "weighted",
        }])
        _persist_per_protein(_Ctx(tmp_path, th_step=0.5), "nk", sink,
                             {"BPO": {"tau": 0.5}}, emit)
        assert "run_cafa_evaluation.per_protein_written" in _events(emit)
        assert (tmp_path / "nk" / "per_protein.parquet").exists()
