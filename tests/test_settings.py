from pathlib import Path

import pytest

from protea.infrastructure.settings import _DEFAULT_ALLOWED_ORIGINS, load_settings


def test_load_settings_reads_yaml(tmp_path: Path):
    (tmp_path / "protea" / "config").mkdir(parents=True, exist_ok=True)
    (tmp_path / "protea" / "config" / "system.yaml").write_text(
        "database:\n  url: postgresql+psycopg://u:p@localhost:5432/x\nqueue:\n  amqp_url: amqp://a:b@localhost:5672/\n",
        encoding="utf-8",
    )

    s = load_settings(tmp_path)
    assert s.db_url.endswith("/x")
    assert s.amqp_url.startswith("amqp://a:b@")


# ---------------------------------------------------------------------------
# T5.5 — CORS allowlist
# ---------------------------------------------------------------------------


class TestAllowedOriginsResolution:
    def test_default_when_unset(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("PROTEA_ALLOWED_ORIGINS", raising=False)
        s = load_settings(tmp_path)
        assert s.allowed_origins == _DEFAULT_ALLOWED_ORIGINS

    def test_env_overrides_default(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(
            "PROTEA_ALLOWED_ORIGINS",
            "https://app.example.com,https://staging.example.com",
        )
        s = load_settings(tmp_path)
        assert s.allowed_origins == (
            "https://app.example.com",
            "https://staging.example.com",
        )

    def test_env_with_whitespace_is_stripped(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(
            "PROTEA_ALLOWED_ORIGINS",
            "  https://a.example  , ,https://b.example ",
        )
        s = load_settings(tmp_path)
        assert s.allowed_origins == ("https://a.example", "https://b.example")

    def test_empty_env_returns_empty_tuple(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PROTEA_ALLOWED_ORIGINS", "")
        s = load_settings(tmp_path)
        # Empty string disables CORS — middleware skips registration.
        assert s.allowed_origins == ()

    def test_yaml_fallback(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("PROTEA_ALLOWED_ORIGINS", raising=False)
        (tmp_path / "protea" / "config").mkdir(parents=True, exist_ok=True)
        (tmp_path / "protea" / "config" / "system.yaml").write_text(
            "cors:\n  allowed_origins:\n    - https://yaml.example.com\n",
            encoding="utf-8",
        )
        s = load_settings(tmp_path)
        assert s.allowed_origins == ("https://yaml.example.com",)

    def test_env_beats_yaml(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "protea" / "config").mkdir(parents=True, exist_ok=True)
        (tmp_path / "protea" / "config" / "system.yaml").write_text(
            "cors:\n  allowed_origins:\n    - https://yaml.example.com\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("PROTEA_ALLOWED_ORIGINS", "https://env.example.com")
        s = load_settings(tmp_path)
        assert s.allowed_origins == ("https://env.example.com",)
