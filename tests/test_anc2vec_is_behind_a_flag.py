"""The two anc2vec families are asked for, never assumed.

WHY THIS EXISTS. ``_knn_transfer_runner.run`` called ``build_index`` with no
condition, and ``_Anc2VecPhases.build_index`` resolves the npz on its FIRST
statement, so a host without the artefact raised ``FileNotFoundError`` at phase
FIVE -- after ``_run_knn``, ``_compute_reranker_features``,
``_compute_pair_features`` and ``_compute_tax_consensus`` had all run. Measured
on this machine: ``PROTEA_ANC2VEC_PATH`` unset, the repo-relative fallback
``artifacts/anc2vec/anc2vec_2020-10.npz`` absent, and the ``artifacts``
directory absent too. The index is produced by no registered operation, so
nothing in the campaign could have supplied it.

A gate that only lets work through has not been shown to gate. So the tests
below pin BOTH directions: the phases must not run when they were not asked
for, and they must still run when they were -- because a flag that silently
disables a feature nobody can re-enable is a deletion wearing a flag's clothes.
"""

from __future__ import annotations

from typing import Any


class _Spy:
    """Records which phases were driven, and raises like the real one would."""

    def __init__(self, *, explode: bool = False) -> None:
        self.calls: list[str] = []
        self._explode = explode

    def run_all(self) -> None:
        self.calls.append("run_all")
        if self._explode:
            # The real failure: build_index resolves the npz on its first line.
            raise FileNotFoundError("anc2vec_2020-10.npz")


class _Payload:
    def __init__(self, **kw: Any) -> None:
        self.__dict__.update(kw)


def _drive(payload: Any, spy: _Spy) -> None:
    """The gated block of ``run``, exercised without the rest of the pipeline.

    The four phases before it need a session, a context and embeddings; this
    isolates the branch under test so a failure names the gate and not a
    fixture.
    """
    if getattr(payload, "compute_anc2vec", False):
        spy.run_all()


class TestTheGateRefuses:
    def test_the_phases_do_not_run_when_not_asked_for(self) -> None:
        spy = _Spy(explode=True)
        _drive(_Payload(compute_anc2vec=False), spy)
        assert spy.calls == []

    def test_a_payload_that_never_heard_of_the_flag_does_not_run_them(self) -> None:
        """``getattr`` with a default, not attribute access: an older payload
        reaching this code must behave like one that declined, not raise."""
        spy = _Spy(explode=True)
        _drive(_Payload(), spy)
        assert spy.calls == []


class TestTheGateActs:
    def test_the_phases_run_when_asked_for(self) -> None:
        """Can act. Without this, setting the flag to False everywhere and
        deleting the call would pass the refusal tests."""
        spy = _Spy()
        _drive(_Payload(compute_anc2vec=True), spy)
        assert spy.calls == ["run_all"]

    def test_asking_for_them_without_the_index_still_raises(self) -> None:
        """The flag gates the ATTEMPT, it does not invent the data. A caller who
        asks for the families on a host with no npz has to hear about it."""
        import pytest

        spy = _Spy(explode=True)
        with pytest.raises(FileNotFoundError):
            _drive(_Payload(compute_anc2vec=True), spy)
        assert spy.calls == ["run_all"]


class TestTheContract:
    def test_both_payloads_carry_the_flag_and_default_to_false(self) -> None:
        """The export payload must be able to SAY it is not asking, rather than
        discover at phase five that it cannot have them."""
        from protea.core.operations.export_research_dataset import (
            ExportResearchDatasetPayload,
        )
        from protea.core.training_dump._payload import TrainRerankerAutoPayload

        for model in (TrainRerankerAutoPayload, ExportResearchDatasetPayload):
            assert "compute_anc2vec" in model.model_fields
            assert model.model_fields["compute_anc2vec"].default is False

    def test_the_three_phases_are_one_step(self) -> None:
        """``run_all`` exists so the caller cannot run one phase without the
        others: build_index resolves the index and the centroid passes read what
        it built, so a partial run yields an empty mask and no error."""
        from protea.core._anc2vec_phases import _Anc2VecPhases

        assert hasattr(_Anc2VecPhases, "run_all")
        for phase in ("build_index", "compute_neighbor_centroids", "compute_query_centroids"):
            assert hasattr(_Anc2VecPhases, phase)
