"""Regression tests for FIX-REAPER-CTOR.

scripts/worker.py previously passed timeout_seconds/stall_seconds/
max_lease_requeues as loose kwargs to StaleJobReaper(), but
StaleJobReaper.__init__ only accepts (session_factory, amqp_url, config).
Those tuning values belong to StaleJobReaperConfig. The fix extracts
_build_reaper_config() in worker.py and passes config= instead.

These tests exercise the exact construction path without any database
or AMQP connection.
"""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import MagicMock

from protea.config.tuning import WorkerTuning
from protea.workers.stale_job_reaper import StaleJobReaper, StaleJobReaperConfig
from scripts.worker import _build_reaper_config


class TestBuildReaperConfig:
    def test_no_type_error_from_worker_tuning(self) -> None:
        """_build_reaper_config must not raise TypeError."""
        wt = WorkerTuning()
        cfg = _build_reaper_config(wt)
        assert isinstance(cfg, StaleJobReaperConfig)

    def test_timeout_seconds_mapped(self) -> None:
        wt = WorkerTuning(reaper_main_timeout_seconds=7200)
        cfg = _build_reaper_config(wt)
        assert cfg.timeout_seconds == 7200

    def test_stall_seconds_mapped(self) -> None:
        wt = WorkerTuning(reaper_stall_seconds=900)
        cfg = _build_reaper_config(wt)
        assert cfg.stall_seconds == 900

    def test_max_lease_requeues_mapped(self) -> None:
        wt = WorkerTuning(max_lease_requeues=5)
        cfg = _build_reaper_config(wt)
        assert cfg.max_lease_requeues == 5

    def test_to_timedeltas_reflects_tuning_values(self) -> None:
        wt = WorkerTuning(reaper_main_timeout_seconds=3600, reaper_stall_seconds=600)
        cfg = _build_reaper_config(wt)
        timeout_td, stall_td, _queued_td, _grace_td = cfg.to_timedeltas()
        assert timeout_td == timedelta(seconds=3600)
        assert stall_td == timedelta(seconds=600)

    def test_event_grace_seconds_mapped(self) -> None:
        wt = WorkerTuning(reaper_event_grace_seconds=3300)
        cfg = _build_reaper_config(wt)
        assert cfg.event_grace_seconds == 3300

    def test_to_timedeltas_includes_event_grace(self) -> None:
        cfg = StaleJobReaperConfig(event_grace_seconds=1234)
        _timeout, _stall, _queued, grace_td = cfg.to_timedeltas()
        assert grace_td == timedelta(seconds=1234)

    def test_default_tuning_produces_valid_config(self) -> None:
        wt = WorkerTuning()
        cfg = _build_reaper_config(wt)
        assert cfg.timeout_seconds == wt.reaper_main_timeout_seconds
        assert cfg.stall_seconds == wt.reaper_stall_seconds
        assert cfg.max_lease_requeues == wt.max_lease_requeues


class TestStaleJobReaperConstruction:
    def test_reaper_accepts_config_kwarg(self) -> None:
        """StaleJobReaper must accept config= without TypeError."""
        factory = MagicMock()
        cfg = StaleJobReaperConfig(timeout_seconds=3600, stall_seconds=900)
        reaper = StaleJobReaper(factory, amqp_url=None, config=cfg)
        assert reaper is not None

    def test_reaper_construction_via_build_helper(self) -> None:
        """End-to-end: build config from WorkerTuning, pass to StaleJobReaper."""
        factory = MagicMock()
        wt = WorkerTuning(
            reaper_main_timeout_seconds=14400,
            reaper_stall_seconds=1200,
            max_lease_requeues=2,
        )
        cfg = _build_reaper_config(wt)
        reaper = StaleJobReaper(factory, amqp_url="amqp://localhost", config=cfg)
        assert reaper is not None

    def test_reaper_tuning_values_land_in_timedeltas(self) -> None:
        """Timeout and stall from WorkerTuning must reach the reaper's internal timedeltas."""
        factory = MagicMock()
        wt = WorkerTuning(reaper_main_timeout_seconds=5400, reaper_stall_seconds=300)
        cfg = _build_reaper_config(wt)
        reaper = StaleJobReaper(factory, config=cfg)
        assert reaper._timeout == timedelta(seconds=5400)
        assert reaper._stall == timedelta(seconds=300)
