"""KNN runs on CPU unless someone writes down that it should not.

protea-method resolves ``PROTEA_KNN_DEVICE="auto"`` to CUDA whenever a card is
visible, and the variable was set nowhere in this repository: not in compose,
not in the systemd units, not in ``farm.env``. So the absence of configuration
selected the GPU, which is the only path where an out-of-memory retry can
return fewer rows than queries, and the only path where matmul precision
reorders neighbours near ties.

These tests pin the inversion. They are cheap and they exist because the
default they guard is invisible: nothing else in a run says which device was
used, so a regression here would be silent again.
"""

from __future__ import annotations

import logging
import os

import pytest

from protea.core.knn_search import _default_device_to_cpu


@pytest.fixture(autouse=True)
def _clean_device(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each test decides the variable's state; none inherits the shell's."""
    monkeypatch.delenv("PROTEA_KNN_DEVICE", raising=False)


class TestTheDefaultIsCpuAndNotAuto:
    def test_an_unset_variable_becomes_cpu(self) -> None:
        _default_device_to_cpu()
        assert os.environ["PROTEA_KNN_DEVICE"] == "cpu"

    def test_it_never_resolves_to_auto(self) -> None:
        """``auto`` is the library default and means CUDA where a card exists."""
        _default_device_to_cpu()
        assert os.environ["PROTEA_KNN_DEVICE"] != "auto"

    def test_calling_twice_is_stable(self) -> None:
        _default_device_to_cpu()
        _default_device_to_cpu()
        assert os.environ["PROTEA_KNN_DEVICE"] == "cpu"


class TestAnExplicitChoiceIsKeptAndAnnounced:
    def test_an_explicit_device_is_not_overridden(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A deliberate GPU measurement must remain possible."""
        monkeypatch.setenv("PROTEA_KNN_DEVICE", "cuda")
        _default_device_to_cpu()
        assert os.environ["PROTEA_KNN_DEVICE"] == "cuda"

    def test_a_non_cpu_device_is_logged(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Deliberate is fine; silent is not."""
        monkeypatch.setenv("PROTEA_KNN_DEVICE", "cuda")
        with caplog.at_level(logging.WARNING, logger="protea.core.knn_search"):
            _default_device_to_cpu()
        assert "not cpu" in caplog.text

    def test_an_explicit_cpu_says_nothing(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        monkeypatch.setenv("PROTEA_KNN_DEVICE", "cpu")
        with caplog.at_level(logging.WARNING, logger="protea.core.knn_search"):
            _default_device_to_cpu()
        assert caplog.text == ""

    def test_the_case_of_the_value_does_not_decide_it(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """``CPU`` is CPU. Warning on it would train the reader to ignore it."""
        monkeypatch.setenv("PROTEA_KNN_DEVICE", "CPU")
        with caplog.at_level(logging.WARNING, logger="protea.core.knn_search"):
            _default_device_to_cpu()
        assert caplog.text == ""


class TestTheSearchPathAppliesIt:
    def test_search_knn_pins_the_device_before_delegating(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The default is worth nothing if the entry point skips it."""
        import protea.core.knn_search as shim

        seen: dict[str, str | None] = {}

        def _spy(*_a: object, **_k: object) -> list[list[tuple[str, float]]]:
            seen["device"] = os.environ.get("PROTEA_KNN_DEVICE")
            return []

        monkeypatch.setattr(shim, "_lib_search_knn", _spy)
        shim.search_knn()
        assert seen["device"] == "cpu"
