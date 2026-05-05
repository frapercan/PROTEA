"""Tests for protea.config.tuning (T-CONF.2 skeleton)."""

from __future__ import annotations

from pathlib import Path

import pytest

from protea.config.tuning import (
    OperationTuning,
    QueueTuning,
    TuningSettings,
    WorkerTuning,
    _apply_env_overrides,
    _coerce,
    _load_yaml_tuning,
    get_tuning,
)


class TestQueueTuningDefaults:
    def test_publisher_defaults(self) -> None:
        q = QueueTuning()
        assert q.publisher_max_attempts == 12
        assert q.publisher_base_delay == 1.0

    def test_oom_defaults(self) -> None:
        q = QueueTuning()
        assert q.oom_max_retries == 5
        assert q.oom_base_delay == 5
        assert q.oom_max_delay == 300

    def test_validates_non_negative(self) -> None:
        with pytest.raises(Exception):
            QueueTuning(publisher_max_attempts=0)

    def test_validates_oom_max_delay_positive(self) -> None:
        with pytest.raises(Exception):
            QueueTuning(oom_max_delay=0)


class TestCoerce:
    def test_int(self) -> None:
        assert _coerce("42") == 42

    def test_float(self) -> None:
        assert _coerce("1.5") == pytest.approx(1.5)

    def test_bool_true(self) -> None:
        assert _coerce("true") is True
        assert _coerce("TRUE") is True

    def test_bool_false(self) -> None:
        assert _coerce("false") is False

    def test_string_passthrough(self) -> None:
        assert _coerce("not-a-number") == "not-a-number"


class TestApplyEnvOverrides:
    def test_no_env_no_change(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Strip any test-fixture overrides.
        for key in list(__import__("os").environ):
            if key.startswith("PROTEA_TUNING__"):
                monkeypatch.delenv(key, raising=False)
        merged: dict = {}
        out = _apply_env_overrides(merged)
        assert out == {}

    def test_single_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PROTEA_TUNING__QUEUE__PUBLISHER_MAX_ATTEMPTS", "20")
        merged: dict = {}
        out = _apply_env_overrides(merged)
        assert out == {"queue": {"publisher_max_attempts": 20}}

    def test_merges_with_yaml(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PROTEA_TUNING__QUEUE__OOM_MAX_RETRIES", "8")
        merged = {"queue": {"publisher_max_attempts": 30}}
        out = _apply_env_overrides(merged)
        assert out["queue"]["publisher_max_attempts"] == 30
        assert out["queue"]["oom_max_retries"] == 8


class TestGetTuning:
    def setup_method(self) -> None:
        get_tuning.cache_clear()

    def teardown_method(self) -> None:
        get_tuning.cache_clear()

    def test_returns_defaults_when_no_yaml_or_env(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        # Strip any inherited env overrides.
        for key in list(__import__("os").environ):
            if key.startswith("PROTEA_TUNING__"):
                monkeypatch.delenv(key, raising=False)
        # Pretend the project root has no system.yaml.
        monkeypatch.setattr(
            "protea.config.tuning._resolve_project_root", lambda: tmp_path
        )
        get_tuning.cache_clear()
        s = get_tuning()
        assert s.queue.publisher_max_attempts == 12
        assert s.queue.oom_max_retries == 5

    def test_env_override_applies(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        monkeypatch.setattr(
            "protea.config.tuning._resolve_project_root", lambda: tmp_path
        )
        monkeypatch.setenv("PROTEA_TUNING__QUEUE__PUBLISHER_MAX_ATTEMPTS", "25")
        get_tuning.cache_clear()
        s = get_tuning()
        assert s.queue.publisher_max_attempts == 25

    def test_yaml_override_applies(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        cfg_dir = tmp_path / "protea" / "config"
        cfg_dir.mkdir(parents=True)
        (cfg_dir / "system.yaml").write_text(
            "tuning:\n  queue:\n    oom_max_retries: 9\n",
            encoding="utf-8",
        )
        for key in list(__import__("os").environ):
            if key.startswith("PROTEA_TUNING__"):
                monkeypatch.delenv(key, raising=False)
        monkeypatch.setattr(
            "protea.config.tuning._resolve_project_root", lambda: tmp_path
        )
        get_tuning.cache_clear()
        s = get_tuning()
        assert s.queue.oom_max_retries == 9
        # Untouched fields keep defaults.
        assert s.queue.publisher_max_attempts == 12

    def test_env_overrides_yaml(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        cfg_dir = tmp_path / "protea" / "config"
        cfg_dir.mkdir(parents=True)
        (cfg_dir / "system.yaml").write_text(
            "tuning:\n  queue:\n    publisher_max_attempts: 7\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(
            "protea.config.tuning._resolve_project_root", lambda: tmp_path
        )
        monkeypatch.setenv("PROTEA_TUNING__QUEUE__PUBLISHER_MAX_ATTEMPTS", "33")
        get_tuning.cache_clear()
        s = get_tuning()
        assert s.queue.publisher_max_attempts == 33

    def test_load_yaml_handles_missing_section(self, tmp_path: Path) -> None:
        cfg_dir = tmp_path / "protea" / "config"
        cfg_dir.mkdir(parents=True)
        (cfg_dir / "system.yaml").write_text(
            "database:\n  url: postgresql://x\n",
            encoding="utf-8",
        )
        out = _load_yaml_tuning(tmp_path)
        assert out == {}


class TestTuningSettingsModel:
    def test_compose(self) -> None:
        s = TuningSettings(queue=QueueTuning(publisher_max_attempts=15))
        assert s.queue.publisher_max_attempts == 15

    def test_default_compose(self) -> None:
        s = TuningSettings()
        assert s.queue.publisher_max_attempts == 12

    def test_default_worker_compose(self) -> None:
        s = TuningSettings()
        assert s.worker.db_pool_size == 20
        assert s.worker.model_cache_max == 1
        assert s.worker.api_cache_default_ttl_seconds == 300.0


class TestWorkerTuningDefaults:
    def test_pool_defaults(self) -> None:
        w = WorkerTuning()
        assert w.db_pool_size == 20
        assert w.db_pool_max_overflow == 40
        assert w.db_pool_recycle_seconds == 3600

    def test_cache_defaults(self) -> None:
        w = WorkerTuning()
        assert w.model_cache_max == 1
        assert w.ref_cache_max == 1
        assert w.api_cache_default_ttl_seconds == pytest.approx(300.0)

    def test_reaper_defaults(self) -> None:
        w = WorkerTuning()
        assert w.reaper_main_timeout_seconds == 86400
        assert w.reaper_default_timeout_seconds == 3600
        assert w.reaper_stall_seconds == 1800

    def test_validates_pool_size(self) -> None:
        with pytest.raises(Exception):
            WorkerTuning(db_pool_size=0)

    def test_validates_reaper_main_floor(self) -> None:
        with pytest.raises(Exception):
            WorkerTuning(reaper_main_timeout_seconds=10)

    def test_validates_cache_max(self) -> None:
        with pytest.raises(Exception):
            WorkerTuning(model_cache_max=0)


class TestWorkerEnvOverrides:
    def setup_method(self) -> None:
        get_tuning.cache_clear()

    def teardown_method(self) -> None:
        get_tuning.cache_clear()

    def test_env_override_pool_size(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.setattr(
            "protea.config.tuning._resolve_project_root", lambda: tmp_path
        )
        monkeypatch.setenv("PROTEA_TUNING__WORKER__DB_POOL_SIZE", "50")
        get_tuning.cache_clear()
        s = get_tuning()
        assert s.worker.db_pool_size == 50


class TestOperationTuningDefaults:
    def test_chunk_defaults(self) -> None:
        o = OperationTuning()
        assert o.annotation_chunk_size == 10_000
        assert o.stream_chunk_size == 2_000
        assert o.store_chunk_size == 10_000
        assert o.numpy_query_chunk == 500

    def test_validates_floor(self) -> None:
        with pytest.raises(Exception):
            OperationTuning(annotation_chunk_size=10)
        with pytest.raises(Exception):
            OperationTuning(stream_chunk_size=10)
        with pytest.raises(Exception):
            OperationTuning(store_chunk_size=100)
        with pytest.raises(Exception):
            OperationTuning(numpy_query_chunk=0)


class TestOperationEnvOverrides:
    def setup_method(self) -> None:
        get_tuning.cache_clear()

    def teardown_method(self) -> None:
        get_tuning.cache_clear()

    def test_env_override_chunk_size(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.setattr(
            "protea.config.tuning._resolve_project_root", lambda: tmp_path
        )
        monkeypatch.setenv("PROTEA_TUNING__OPERATION__ANNOTATION_CHUNK_SIZE", "50000")
        get_tuning.cache_clear()
        s = get_tuning()
        assert s.operation.annotation_chunk_size == 50_000
