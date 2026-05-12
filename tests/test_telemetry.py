"""Tests for the OTel SDK boot (T5.1a).

These tests cover the env-driven config resolution and the
no-op-when-disabled / soft-fail-when-deps-missing behaviour. They do
NOT spin up a real OTel SDK or collector — that belongs in the
integration tier once T5.1b lands.
"""

from __future__ import annotations

import logging
import sys
from typing import Any
from unittest.mock import MagicMock

import pytest

from protea.infrastructure.telemetry import (
    SdkBundle,
    TelemetryConfig,
    _as_bool,
    _as_float,
    _build_provider,
    configure_telemetry,
    resolve_telemetry_config,
)


class TestAsBool:
    @pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on", "  true  "])
    def test_truthy_values(self, value: str) -> None:
        assert _as_bool(value) is True

    @pytest.mark.parametrize("value", ["0", "false", "no", "off", "", "  ", "maybe"])
    def test_falsey_values(self, value: str) -> None:
        assert _as_bool(value) is False

    def test_none_is_false(self) -> None:
        assert _as_bool(None) is False


class TestAsFloat:
    def test_parses_valid_float(self) -> None:
        assert _as_float("0.25", default=1.0) == 0.25

    def test_falls_back_on_invalid(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING):
            value = _as_float("not-a-number", default=0.5)
        assert value == 0.5
        assert "invalid PROTEA_OTEL_SAMPLE_RATIO" in caplog.text

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_empty_returns_default(self, value: str | None) -> None:
        assert _as_float(value, default=0.75) == 0.75


class TestResolveTelemetryConfig:
    def test_defaults_when_env_empty(self) -> None:
        cfg = resolve_telemetry_config(env={})
        assert cfg == TelemetryConfig(
            enabled=False,
            endpoint=None,
            service_name="protea-api",
            sample_ratio=1.0,
        )

    def test_env_overrides(self) -> None:
        cfg = resolve_telemetry_config(
            env={
                "PROTEA_OTEL_ENABLED": "true",
                "PROTEA_OTEL_ENDPOINT": "http://collector:4318",
                "PROTEA_OTEL_SERVICE_NAME": "protea-worker-embeddings",
                "PROTEA_OTEL_SAMPLE_RATIO": "0.1",
            }
        )
        assert cfg.enabled is True
        assert cfg.endpoint == "http://collector:4318"
        assert cfg.service_name == "protea-worker-embeddings"
        assert cfg.sample_ratio == 0.1

    def test_default_service_name_override(self) -> None:
        cfg = resolve_telemetry_config(
            env={"PROTEA_OTEL_ENABLED": "1"},
            default_service_name="protea-worker-jobs",
        )
        assert cfg.service_name == "protea-worker-jobs"

    def test_explicit_env_beats_default_service_name(self) -> None:
        cfg = resolve_telemetry_config(
            env={
                "PROTEA_OTEL_ENABLED": "1",
                "PROTEA_OTEL_SERVICE_NAME": "explicit",
            },
            default_service_name="protea-worker-jobs",
        )
        assert cfg.service_name == "explicit"

    def test_empty_endpoint_becomes_none(self) -> None:
        cfg = resolve_telemetry_config(env={"PROTEA_OTEL_ENDPOINT": ""})
        assert cfg.endpoint is None


class TestConfigureTelemetryDisabled:
    def test_returns_config_without_booting(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("PROTEA_OTEL_ENABLED", raising=False)
        cfg = configure_telemetry()
        assert cfg.enabled is False

    def test_disabled_does_not_touch_fastapi(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("PROTEA_OTEL_ENABLED", raising=False)
        fake_app = MagicMock()
        cfg = configure_telemetry(fake_app)
        assert cfg.enabled is False
        # Disabled boot must not call any FastAPI methods on the app.
        fake_app.assert_not_called()


class TestConfigureTelemetrySdkMissing:
    def test_logs_warning_and_returns_config(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        # Force the SDK import to fail by injecting a sentinel into
        # sys.modules; the import statement inside configure_telemetry
        # will then raise ImportError.
        monkeypatch.setitem(sys.modules, "opentelemetry", None)
        with caplog.at_level(logging.WARNING):
            cfg = configure_telemetry(
                config=TelemetryConfig(
                    enabled=True,
                    endpoint=None,
                    service_name="protea-test",
                    sample_ratio=1.0,
                )
            )
        assert cfg.enabled is True
        assert "opentelemetry SDK not installed" in caplog.text


class TestBuildProvider:
    """Drive ``_build_provider`` with stand-ins to confirm the wiring."""

    def test_assembles_resource_sampler_and_exporter(self) -> None:
        captured: dict[str, Any] = {}

        class FakeResource:
            @staticmethod
            def create(attrs: dict[str, Any]) -> str:
                captured["resource_attrs"] = attrs
                return "resource"

        class FakeTraceIdRatioBased:
            def __init__(self, ratio: float) -> None:
                captured["sample_ratio"] = ratio

        class FakeParentBased:
            def __init__(self, sampler: Any) -> None:
                captured["sampler_inner"] = sampler

        class FakeBatchSpanProcessor:
            def __init__(self, exporter: Any) -> None:
                captured["processor_exporter"] = exporter

        class FakeOTLPSpanExporter:
            def __init__(self, **kwargs: Any) -> None:
                captured["exporter_kwargs"] = kwargs

        class FakeTracerProvider:
            def __init__(self, *, resource: Any, sampler: Any) -> None:
                captured["provider_resource"] = resource
                captured["provider_sampler"] = sampler
                self.processors: list[Any] = []

            def add_span_processor(self, processor: Any) -> None:
                self.processors.append(processor)

        provider = _build_provider(
            TelemetryConfig(
                enabled=True,
                endpoint="http://collector:4318",
                service_name="protea-api",
                sample_ratio=0.25,
            ),
            SdkBundle(
                TracerProvider=FakeTracerProvider,
                Resource=FakeResource,
                BatchSpanProcessor=FakeBatchSpanProcessor,
                OTLPSpanExporter=FakeOTLPSpanExporter,
                ParentBased=FakeParentBased,
                TraceIdRatioBased=FakeTraceIdRatioBased,
            ),
        )

        assert captured["resource_attrs"] == {"service.name": "protea-api"}
        assert captured["sample_ratio"] == 0.25
        assert isinstance(captured["sampler_inner"], FakeTraceIdRatioBased)
        assert captured["exporter_kwargs"] == {
            "endpoint": "http://collector:4318/v1/traces"
        }
        assert isinstance(provider, FakeTracerProvider)
        assert len(provider.processors) == 1

    def test_no_endpoint_uses_exporter_default(self) -> None:
        captured: dict[str, Any] = {}

        class FakeResource:
            @staticmethod
            def create(attrs: dict[str, Any]) -> str:
                return "resource"

        class _NoOp:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                captured.setdefault("calls", []).append((args, kwargs))

        class FakeOTLPSpanExporter:
            def __init__(self, **kwargs: Any) -> None:
                captured["exporter_kwargs"] = kwargs

        class FakeTracerProvider:
            def __init__(self, *, resource: Any, sampler: Any) -> None:
                pass

            def add_span_processor(self, processor: Any) -> None:
                pass

        _build_provider(
            TelemetryConfig(
                enabled=True,
                endpoint=None,
                service_name="protea-api",
                sample_ratio=1.0,
            ),
            SdkBundle(
                TracerProvider=FakeTracerProvider,
                Resource=FakeResource,
                BatchSpanProcessor=_NoOp,
                OTLPSpanExporter=FakeOTLPSpanExporter,
                ParentBased=_NoOp,
                TraceIdRatioBased=_NoOp,
            ),
        )

        # Empty kwargs let the OTLP exporter fall back to its own
        # default endpoint (http://localhost:4318).
        assert captured["exporter_kwargs"] == {}

    def test_endpoint_trailing_slash_normalised(self) -> None:
        captured: dict[str, Any] = {}

        class FakeOTLPSpanExporter:
            def __init__(self, **kwargs: Any) -> None:
                captured["exporter_kwargs"] = kwargs

        class _NoOp:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                pass

        class FakeResource:
            @staticmethod
            def create(attrs: dict[str, Any]) -> str:
                return "resource"

        class FakeTracerProvider:
            def __init__(self, *, resource: Any, sampler: Any) -> None:
                pass

            def add_span_processor(self, processor: Any) -> None:
                pass

        _build_provider(
            TelemetryConfig(
                enabled=True,
                endpoint="http://collector:4318/",
                service_name="protea-api",
                sample_ratio=1.0,
            ),
            SdkBundle(
                TracerProvider=FakeTracerProvider,
                Resource=FakeResource,
                BatchSpanProcessor=_NoOp,
                OTLPSpanExporter=FakeOTLPSpanExporter,
                ParentBased=_NoOp,
                TraceIdRatioBased=_NoOp,
            ),
        )

        assert (
            captured["exporter_kwargs"]["endpoint"]
            == "http://collector:4318/v1/traces"
        )
