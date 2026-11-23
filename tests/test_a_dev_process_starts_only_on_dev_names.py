"""A process that declares itself dev starts only on dev's database and vhost.

Layer two of the environment isolation (protea.infrastructure.environment_check).
Layer one, a separate stack on its own ports with its own credentials, fails
open on one wrong line in an env file: a dev worker given production's URL
connects to production's broker and runs the first message it takes against
production's database.

Both directions are pinned. With ``PROTEA_ENVIRONMENT=dev`` the API and every
worker refuse anything but the dev names, with ``EX_CONFIG`` and one line;
without it, startup does not change. Everything here parses URLs and opens
nothing.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from protea.infrastructure import environment_check as check
from protea.infrastructure.settings import Settings, load_settings

_PASSWORD = "not-a-real-password"
_PROD_HOST = "10.0.0.2"
_DEV_DB = f"postgresql+psycopg://protea:{_PASSWORD}@127.0.0.1:26432/protea_dev"
_DEV_AMQP = f"amqp://protea:{_PASSWORD}@127.0.0.1:26672/protea-dev"
_PROD_DB = f"postgresql+psycopg://protea:{_PASSWORD}@{_PROD_HOST}:5432/BioData"
_PROD_AMQP = f"amqp://protea:{_PASSWORD}@{_PROD_HOST}:5672/"


def _settings(
    environment: str | None,
    db_url: str = _DEV_DB,
    amqp_url: str = _DEV_AMQP,
    **names: str,
) -> Settings:
    return Settings(
        db_url=db_url,
        amqp_url=amqp_url,
        artifacts_dir=Path("unused"),
        environment=environment,
        **names,
    )


class TestTheNamesAreReadAsTheConnectionsReadThem:
    def test_the_database_is_the_path(self) -> None:
        assert check.database_name(_DEV_DB) == "protea_dev"

    def test_a_dbname_parameter_overrides_the_path(self) -> None:
        """psycopg receives the query after the path, so the parameter wins."""
        assert check.database_name(f"{_DEV_DB}?dbname=BioData") == "BioData"

    @pytest.mark.parametrize(
        "url",
        [
            "postgresql+psycopg://protea:pw@127.0.0.1:26432/",
            "postgresql+psycopg://protea:pw@127.0.0.1:26432",
            "postgresql+psycopg://protea:pw@127.0.0.1/x?dbname=a&dbname=b",
        ],
    )
    def test_a_url_that_names_no_single_database_reads_none(self, url: str) -> None:
        assert check.database_name(url) is None

    @pytest.mark.parametrize(
        ("url", "vhost"),
        [
            (_DEV_AMQP, "protea-dev"),
            ("amqp://g:g@127.0.0.1:26672/protea%2Ddev", "protea-dev"),
            ("amqp://g:g@127.0.0.1:26672/", "/"),
            ("amqp://g:g@127.0.0.1:26672", "/"),
            ("amqp://g:g@127.0.0.1:26672/%2F", "/"),
        ],
    )
    def test_the_vhost_is_the_one_pika_connects_to(self, url: str, vhost: str) -> None:
        assert check.amqp_vhost(url) == vhost


class TestWithoutDevNothingChanges:
    @pytest.mark.parametrize("environment", [None, "", "test", "production", "prod", "development"])
    def test_production_names_start(self, environment: str | None) -> None:
        assert check.environment_mismatch(_settings(environment, _PROD_DB, _PROD_AMQP)) is None

    def test_even_unreadable_urls_start(self) -> None:
        """Without dev nothing is parsed, so nothing new can fail."""
        assert check.environment_mismatch(_settings(None, "not a url", "also not")) is None

    @pytest.mark.parametrize(
        "double",
        [MagicMock(), SimpleNamespace(db_url=_PROD_DB, amqp_url=_PROD_AMQP)],
        ids=["magic-mock", "no-environment-field"],
    )
    def test_a_settings_double_without_the_field_starts(self, double: object) -> None:
        assert check.environment_mismatch(double) is None  # type: ignore[arg-type]

    def test_starting_is_silent(self, capsys: pytest.CaptureFixture[str]) -> None:
        check.exit_on_environment_mismatch(_settings(None, _PROD_DB, _PROD_AMQP))
        assert capsys.readouterr() == ("", "")


class TestDevRefusesAnythingButDevNames:
    def test_the_dev_names_start(self) -> None:
        assert check.environment_mismatch(_settings("dev")) is None

    def test_the_production_database_refuses(self) -> None:
        reason = check.environment_mismatch(_settings("dev", _PROD_DB, _DEV_AMQP))
        assert reason is not None
        assert "'protea_dev'" in reason and "'BioData'" in reason

    def test_the_default_vhost_refuses(self) -> None:
        reason = check.environment_mismatch(_settings("dev", _DEV_DB, _PROD_AMQP))
        assert reason is not None
        assert "'protea-dev'" in reason and "vhost '/'" in reason

    def test_a_dev_path_overridden_by_dbname_refuses(self) -> None:
        reason = check.environment_mismatch(_settings("dev", f"{_DEV_DB}?dbname=BioData"))
        assert reason is not None and "'BioData'" in reason

    def test_a_url_that_names_no_database_refuses(self) -> None:
        url = f"postgresql+psycopg://protea:{_PASSWORD}@127.0.0.1:26432/"
        reason = check.environment_mismatch(_settings("dev", url))
        assert reason is not None and "database None" in reason

    @pytest.mark.parametrize(
        ("db_url", "amqp_url", "which"),
        [
            ("not a url", _DEV_AMQP, "database URL"),
            (_DEV_DB, f"amqp://protea:{_PASSWORD}@127.0.0.1/protea-dev?bogus=1", "AMQP URL"),
        ],
    )
    def test_an_unreadable_url_refuses(self, db_url: str, amqp_url: str, which: str) -> None:
        reason = check.environment_mismatch(_settings("dev", db_url, amqp_url))
        assert reason is not None and f"{which} cannot be parsed" in reason

    def test_declared_names_replace_the_defaults(self) -> None:
        lab_db = f"postgresql+psycopg://protea:{_PASSWORD}@127.0.0.1:26432/lab_dev"
        lab_amqp = f"amqp://protea:{_PASSWORD}@127.0.0.1:26672/lab"
        declared = {"dev_database_name": "lab_dev", "dev_amqp_vhost": "lab"}
        assert check.environment_mismatch(_settings("dev", lab_db, lab_amqp, **declared)) is None
        assert check.environment_mismatch(_settings("dev", lab_db, lab_amqp)) is not None
        assert check.environment_mismatch(_settings("dev", **declared)) is not None

    @pytest.mark.parametrize("environment", ["dev", "Dev", " DEV "])
    def test_dev_is_read_as_the_rate_limiter_reads_it(
        self, environment: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two readers of one variable must agree about whether a process is dev."""
        from protea.api.rate_limit import _is_test_env

        monkeypatch.setenv("PROTEA_ENVIRONMENT", environment)
        assert _is_test_env()
        assert check.declares_dev(_settings(environment))
        assert check.environment_mismatch(_settings(environment, _PROD_DB, _PROD_AMQP))


class TestTheRefusal:
    def test_it_exits_78_with_one_line_on_stderr(self, capsys: pytest.CaptureFixture[str]) -> None:
        with pytest.raises(SystemExit) as exited:
            check.exit_on_environment_mismatch(_settings("dev", _PROD_DB, _PROD_AMQP))
        assert exited.value.code == check.EX_CONFIG == 78
        out, err = capsys.readouterr()
        assert out == ""
        assert err.endswith("\n") and err.count("\n") == 1
        assert err.startswith("refusing to start: PROTEA_ENVIRONMENT=dev")

    @pytest.mark.parametrize(
        ("db_url", "amqp_url"),
        [
            (_PROD_DB, _PROD_AMQP),
            # Both fail to parse, with the password and the host inside.
            (f"postgresql+psycopg://protea:{_PASSWORD}@{_PROD_HOST}:port/BioData", _DEV_AMQP),
            (_DEV_DB, f"{_PROD_AMQP}?bogus=1"),
        ],
        ids=["mismatch", "unreadable-database-url", "unreadable-amqp-url"],
    )
    def test_it_never_quotes_a_url(self, db_url: str, amqp_url: str) -> None:
        reason = check.environment_mismatch(_settings("dev", db_url, amqp_url))
        assert reason is not None
        assert _PASSWORD not in reason and _PROD_HOST not in reason


class TestTheSettingsDeclareTheNames:
    _VARIABLES = ("PROTEA_ENVIRONMENT", "PROTEA_DEV_DATABASE_NAME", "PROTEA_DEV_AMQP_VHOST")

    def test_the_defaults(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        for name in self._VARIABLES:
            monkeypatch.delenv(name, raising=False)
        s = load_settings(tmp_path)
        assert (s.environment, s.dev_database_name, s.dev_amqp_vhost) == (
            None,
            "protea_dev",
            "protea-dev",
        )

    def test_the_environment_overrides_them(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        for name, value in zip(self._VARIABLES, ("dev", "other_dev", "other-dev"), strict=True):
            monkeypatch.setenv(name, value)
        s = load_settings(tmp_path)
        assert (s.environment, s.dev_database_name, s.dev_amqp_vhost) == (
            "dev",
            "other_dev",
            "other-dev",
        )

    def test_a_dev_written_only_in_the_env_file_turns_the_check_on(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The settings read the project's .env, which the rate limiter does not."""
        from protea.api.rate_limit import _is_test_env

        for name in self._VARIABLES:
            monkeypatch.delenv(name, raising=False)
        (tmp_path / ".env").write_text("PROTEA_ENVIRONMENT=dev\n", encoding="utf-8")
        assert check.declares_dev(load_settings(tmp_path))
        assert not _is_test_env()


class _Opened(Exception):
    """Raised by the stand-in session factory: startup got past the check."""


def _open(*_args: object, **_kwargs: object) -> None:
    raise _Opened


@pytest.fixture
def worker_script(monkeypatch: pytest.MonkeyPatch):
    """``scripts/worker.py`` with its global logging and telemetry setup inert."""
    import scripts.worker as worker_script

    monkeypatch.setattr("protea.infrastructure.logging.configure_logging", lambda **_kw: None)
    monkeypatch.setattr("protea.infrastructure.telemetry.configure_telemetry", lambda **_kw: None)
    pika_logger = logging.getLogger("pika")
    level = pika_logger.level
    yield worker_script
    pika_logger.setLevel(level)


def _start_worker(
    worker_script, monkeypatch: pytest.MonkeyPatch, queue: str, settings: Settings
) -> None:
    monkeypatch.setattr(sys, "argv", ["worker.py", "--queue", queue])
    monkeypatch.setattr(worker_script, "load_settings", lambda _root: settings)
    monkeypatch.setattr(worker_script, "build_session_factory", _open)
    worker_script.main()


class TestTheWorkerRefusesBeforeItOpensAnything:
    @pytest.mark.parametrize("queue", ["protea.jobs", "protea.embeddings.batch", "reaper"])
    def test_dev_on_production_names_exits_78(
        self, worker_script, monkeypatch: pytest.MonkeyPatch, queue: str
    ) -> None:
        settings = _settings("dev", _PROD_DB, _PROD_AMQP)
        with pytest.raises(SystemExit) as exited:
            _start_worker(worker_script, monkeypatch, queue, settings)
        assert exited.value.code == 78

    @pytest.mark.parametrize("queue", ["protea.jobs", "reaper"])
    def test_without_dev_it_goes_on_to_open_the_database(
        self, worker_script, monkeypatch: pytest.MonkeyPatch, queue: str
    ) -> None:
        settings = _settings(None, _PROD_DB, _PROD_AMQP)
        with pytest.raises(_Opened):
            _start_worker(worker_script, monkeypatch, queue, settings)

    def test_dev_on_dev_names_goes_on_to_open_the_database(
        self, worker_script, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        with pytest.raises(_Opened):
            _start_worker(worker_script, monkeypatch, "protea.jobs", _settings("dev"))


class TestTheApiRefusesBeforeItBuildsAnything:
    def test_dev_on_production_names_exits_78(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from protea.api.app import create_app

        settings = _settings("dev", _PROD_DB, _PROD_AMQP)
        monkeypatch.setattr("protea.api.app.load_settings", lambda _root: settings)
        monkeypatch.setattr("protea.api.app.build_session_factory", _open)
        with pytest.raises(SystemExit) as exited:
            create_app(Path("/fake/root"))
        assert exited.value.code == 78

    @pytest.mark.parametrize(
        "settings",
        [_settings(None, _PROD_DB, _PROD_AMQP), _settings("dev")],
        ids=["no-environment", "dev-on-dev-names"],
    )
    def test_the_app_builds(self, settings: Settings, monkeypatch: pytest.MonkeyPatch) -> None:
        from protea.api.app import create_app

        factory = MagicMock()
        monkeypatch.setattr("protea.api.app.load_settings", lambda _root: settings)
        monkeypatch.setattr("protea.api.app.build_session_factory", lambda _url: factory)
        app = create_app(Path("/fake/root"))
        assert app.state.session_factory is factory
