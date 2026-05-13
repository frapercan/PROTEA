from pathlib import Path

import pytest

from protea.infrastructure.settings import (
    _DEFAULT_ALLOWED_ORIGINS,
    _LEGACY_ENV_ALIASES,
    load_settings,
)


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


# ---------------------------------------------------------------------------
# T-OPS.6 - pydantic-settings migration: source hierarchy + deprecations
# ---------------------------------------------------------------------------


class TestSourcePriority:
    """env > .env > yaml > defaults precedence (T-OPS.6)."""

    def _write_yaml(self, root: Path, body: str) -> None:
        (root / "protea" / "config").mkdir(parents=True, exist_ok=True)
        (root / "protea" / "config" / "system.yaml").write_text(body, encoding="utf-8")

    def test_defaults_when_no_source(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Clear any process-wide PROTEA_* env so the defaults shine through.
        monkeypatch.delenv("PROTEA_DB_URL", raising=False)
        monkeypatch.delenv("PROTEA_AMQP_URL", raising=False)
        monkeypatch.delenv("PROTEA_ADMIN_TOKEN", raising=False)
        s = load_settings(tmp_path)
        assert s.db_url.startswith("postgresql+psycopg://")
        assert s.amqp_url.startswith("amqp://")
        assert s.admin_token == ""
        assert s.storage_backend == "local"

    def test_dotenv_at_project_root_is_picked_up(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("PROTEA_ADMIN_TOKEN", raising=False)
        (tmp_path / ".env").write_text(
            'PROTEA_ADMIN_TOKEN="from-dotenv"\n', encoding="utf-8"
        )
        s = load_settings(tmp_path)
        assert s.admin_token == "from-dotenv"

    def test_env_beats_dotenv(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / ".env").write_text(
            'PROTEA_ADMIN_TOKEN="from-dotenv"\n', encoding="utf-8"
        )
        monkeypatch.setenv("PROTEA_ADMIN_TOKEN", "from-env")
        s = load_settings(tmp_path)
        assert s.admin_token == "from-env"

    def test_yaml_beats_default(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("PROTEA_DB_URL", raising=False)
        self._write_yaml(tmp_path, "database:\n  url: postgresql+psycopg://u:p@h/yamldb\n")
        s = load_settings(tmp_path)
        assert s.db_url.endswith("/yamldb")

    def test_env_beats_yaml_db_url(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._write_yaml(tmp_path, "database:\n  url: postgresql+psycopg://u:p@h/yamldb\n")
        monkeypatch.setenv("PROTEA_DB_URL", "postgresql+psycopg://u:p@h/envdb")
        s = load_settings(tmp_path)
        assert s.db_url.endswith("/envdb")

    def test_storage_backend_lowercased(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PROTEA_STORAGE_BACKEND", "MINIO")
        s = load_settings(tmp_path)
        assert s.storage_backend == "minio"

    def test_storage_root_relative_resolves_against_project_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PROTEA_STORAGE_ROOT", "storage/artifacts")
        s = load_settings(tmp_path)
        assert s.storage_root == tmp_path / "storage" / "artifacts"

    def test_minio_secure_truthy_env(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PROTEA_MINIO_SECURE", "true")
        s = load_settings(tmp_path)
        assert s.minio_secure is True

    def test_anc2vec_path_default_is_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("PROTEA_ANC2VEC_PATH", raising=False)
        s = load_settings(tmp_path)
        assert s.anc2vec_path is None

    def test_anc2vec_path_env_overrides_default(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PROTEA_ANC2VEC_PATH", "/srv/protea/anc2vec.npz")
        s = load_settings(tmp_path)
        assert s.anc2vec_path == "/srv/protea/anc2vec.npz"

    def test_anc2vec_path_yaml_fallback(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("PROTEA_ANC2VEC_PATH", raising=False)
        self._write_yaml(tmp_path, "anc2vec:\n  path: /yaml/anc2vec.npz\n")
        s = load_settings(tmp_path)
        assert s.anc2vec_path == "/yaml/anc2vec.npz"


class TestLegacyEnvAliases:
    """T-OPS.6 deprecation: unprefixed env aliases (DATABASE_URL, AMQP_URL)
    are promoted to their PROTEA_* canonical names with a warning.
    """

    def test_legacy_database_url_promoted(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("PROTEA_DB_URL", raising=False)
        monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://l:e@h/legacy")
        with pytest.warns(DeprecationWarning, match="DATABASE_URL"):
            s = load_settings(tmp_path)
        assert s.db_url.endswith("/legacy")

    def test_legacy_amqp_url_promoted(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("PROTEA_AMQP_URL", raising=False)
        monkeypatch.setenv("AMQP_URL", "amqp://legacy:legacy@host/")
        with pytest.warns(DeprecationWarning, match="AMQP_URL"):
            s = load_settings(tmp_path)
        assert s.amqp_url.startswith("amqp://legacy:legacy@")

    def test_modern_env_wins_over_legacy(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PROTEA_DB_URL", "postgresql+psycopg://u:p@h/modern")
        monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://l:e@h/legacy")
        s = load_settings(tmp_path)
        # No deprecation warning fires when the modern name is already set.
        assert s.db_url.endswith("/modern")

    def test_alias_map_keyed_on_protea_prefix(self) -> None:
        """Sanity check: every legacy alias targets a PROTEA_* canonical name."""
        for modern in _LEGACY_ENV_ALIASES:
            assert modern.startswith("PROTEA_")


class TestEnvPrefixDeprecation:
    def test_non_default_prefix_warns(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        with pytest.warns(DeprecationWarning, match="env_prefix"):
            load_settings(tmp_path, env_prefix="CUSTOM_")
