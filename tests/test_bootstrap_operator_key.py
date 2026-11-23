"""The first operator API key on a database that has none.

``POST /auth/api-keys`` is admin-gated and mints keys without a role, so an
empty database could only get its first operator credential from a
hand-written INSERT. These tests pin the bootstrap that replaces it: it builds
the key with the router's own helpers, the role gate reads it as an operator,
it refuses once an operator-capable key exists, and the command line prints
the key once on stdout and takes nothing secret on argv.

The session is a mock: the logic under test is which rows are added and which
are refused, not what Postgres does with them.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from protea.api.auth import hash_key, prefix_of  # noqa: E402
from protea.api.auth.bootstrap import (  # noqa: E402
    OperatorKeyExistsError,
    mint_first_operator_key,
)
from protea.api.roles import ROLE_OPERATOR, role_of  # noqa: E402
from protea.infrastructure.orm.models.api_key import ApiKey  # noqa: E402
from protea.infrastructure.orm.models.auth_audit import AuthAudit  # noqa: E402
from scripts import bootstrap_operator_key as cli  # noqa: E402

_DB_PASSWORD = "s3cr3t-db-password"
_DB_URL = f"postgresql+psycopg://protea_dev:{_DB_PASSWORD}@127.0.0.1:26432/protea_dev"


def _session(existing_roles=(), dialect: str = "sqlite") -> MagicMock:
    """A session whose unrevoked keys carry ``existing_roles``."""
    session = MagicMock()
    session.scalars.return_value.all.return_value = list(existing_roles)
    session.get_bind.return_value.dialect.name = dialect
    return session


def _added(session: MagicMock, cls: type) -> list:
    return [c.args[0] for c in session.add.call_args_list if isinstance(c.args[0], cls)]


class TestMintFirstOperatorKey:
    def test_the_key_is_stored_hashed_and_the_gate_reads_it_as_operator(self):
        session = _session()
        minted = mint_first_operator_key(session, name="  dev-operator ", source="test")

        (row,) = _added(session, ApiKey)
        assert row.key_hash == hash_key(minted.raw_key)
        assert row.prefix == prefix_of(minted.raw_key) == minted.prefix
        assert row.name == minted.name == "dev-operator"
        assert row.role == minted.role == ROLE_OPERATOR
        assert row.id == minted.key_id
        # The same function the role gate calls on a request.
        assert role_of(row) == ROLE_OPERATOR
        assert minted.raw_key not in (row.key_hash, row.prefix, row.name)

    def test_the_mint_is_audited_without_the_key(self):
        session = _session()
        minted = mint_first_operator_key(session, name="dev-operator", source="test-source")

        (audit,) = _added(session, AuthAudit)
        assert audit.event_type == "api_key_mint"
        assert audit.details == {
            "key_id": str(minted.key_id),
            "prefix": minted.prefix,
            "name": "dev-operator",
            "role": ROLE_OPERATOR,
            "source": "test-source",
        }
        assert minted.raw_key not in repr(audit.details)

    @pytest.mark.parametrize("existing", ["operator", "admin", " Operator ", "ADMIN"])
    def test_an_operator_capable_key_means_this_is_not_the_first(self, existing):
        session = _session(existing_roles=["viewer", existing])
        with pytest.raises(OperatorKeyExistsError):
            mint_first_operator_key(session, name="dev-operator", source="test")
        session.add.assert_not_called()

    def test_viewer_and_role_less_keys_do_not_count(self):
        # A NULL role and an unknown string both read as viewer at the gate.
        session = _session(existing_roles=[None, "viewer", "superuser"])
        mint_first_operator_key(session, name="dev-operator", source="test")
        assert len(_added(session, ApiKey)) == 1

    def test_only_unrevoked_keys_are_counted(self):
        session = _session()
        mint_first_operator_key(session, name="dev-operator", source="test")
        statement = str(session.scalars.call_args.args[0])
        assert "api_key.revoked_at IS NULL" in statement

    @pytest.mark.parametrize("name", ["", "   ", "x" * 256])
    def test_an_unusable_name_is_refused_before_anything_is_written(self, name):
        session = _session()
        with pytest.raises(ValueError):
            mint_first_operator_key(session, name=name, source="test")
        session.add.assert_not_called()
        session.execute.assert_not_called()

    def test_postgres_locks_the_table_so_two_bootstraps_cannot_both_mint(self):
        session = _session(dialect="postgresql")
        mint_first_operator_key(session, name="dev-operator", source="test")
        lock = str(session.execute.call_args_list[0].args[0])
        assert lock == "LOCK TABLE api_key IN SHARE ROW EXCLUSIVE MODE"

    def test_other_dialects_take_no_lock(self):
        session = _session(dialect="sqlite")
        mint_first_operator_key(session, name="dev-operator", source="test")
        session.execute.assert_not_called()


class TestCommandLine:
    @pytest.fixture()
    def session(self, monkeypatch: pytest.MonkeyPatch) -> MagicMock:
        """A mocked session behind the CLI's session factory, and the dev URL."""
        session = _session()
        monkeypatch.setattr(cli, "build_session_factory", lambda _url: lambda: session)
        monkeypatch.setenv("PROTEA_DB_URL", _DB_URL)
        return session

    def test_no_option_can_carry_a_secret(self):
        options = {o for action in cli.build_parser()._actions for o in action.option_strings}
        assert options == {"-h", "--help", "--name"}

    def test_the_key_is_printed_once_on_stdout_and_nowhere_else(self, session, capsys):
        assert cli.main(["--name", "dev-operator"]) == 0
        out, err = capsys.readouterr()

        raw = out.removesuffix("\n")
        assert out == raw + "\n" and "\n" not in raw
        (row,) = _added(session, ApiKey)
        assert hash_key(raw) == row.key_hash
        session.commit.assert_called_once()
        assert raw not in err
        assert _DB_PASSWORD not in err
        assert "127.0.0.1:26432/protea_dev" in err
        assert row.prefix in err

    def test_nothing_is_printed_when_the_commit_fails(self, session, capsys):
        session.commit.side_effect = RuntimeError("commit failed")
        with pytest.raises(RuntimeError):
            cli.main(["--name", "dev-operator"])
        out, _err = capsys.readouterr()
        assert out == ""

    def test_an_existing_operator_key_is_a_refusal_with_nothing_printed(self, session, capsys):
        session.scalars.return_value.all.return_value = ["operator"]
        assert cli.main(["--name", "dev-operator"]) == 1
        out, err = capsys.readouterr()
        assert out == ""
        assert "REFUSING" in err
        session.commit.assert_not_called()
        session.rollback.assert_called_once()

    def test_without_a_url_in_the_environment_it_refuses_to_guess(self, monkeypatch, capsys):
        monkeypatch.delenv("PROTEA_DB_URL", raising=False)
        factory = MagicMock()
        monkeypatch.setattr(cli, "build_session_factory", factory)
        assert cli.main(["--name", "dev-operator"]) == 78
        out, _err = capsys.readouterr()
        assert out == ""
        factory.assert_not_called()

    def test_an_unparseable_url_is_refused_without_echoing_it(self, monkeypatch, capsys):
        monkeypatch.setenv("PROTEA_DB_URL", f"postgresql+psycopg://u:{_DB_PASSWORD}@h:port/db")
        factory = MagicMock()
        monkeypatch.setattr(cli, "build_session_factory", factory)
        assert cli.main(["--name", "dev-operator"]) == 78
        out, err = capsys.readouterr()
        assert out == ""
        assert _DB_PASSWORD not in err
        factory.assert_not_called()
