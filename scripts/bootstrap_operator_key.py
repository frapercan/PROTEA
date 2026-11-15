"""Create the first operator API key on a PROTEA database that has none.

Usage (the database URL comes from the environment, never from argv)::

    ( set -a; . ~/.secrets/protea-dev.env; set +a
      umask 077
      python scripts/bootstrap_operator_key.py --name dev-operator \\
          > ~/.secrets/protea-dev/operator-key )

The new key goes to stdout, once, and nothing else does. The target (with
its password masked), the key's id and prefix, and any refusal go to stderr,
none of them secret. The key is printed only after the transaction commits,
so a printed key always exists.

Exit status: 0 minted; 1 refused (an unrevoked operator or admin key already
exists, or the name is unusable); 78 (EX_CONFIG) PROTEA_DB_URL is missing or
unparseable.

THE URL IS READ FROM THE ENVIRONMENT AND NOWHERE ELSE. ``load_settings``
would also read ``.env`` and ``protea/config/system.yaml`` from whatever
checkout this runs in, and a tool that mints credentials must not take its
target from the tree it happens to be started in. There is deliberately no
option for the URL or for any secret: a command line is readable by every
local user in ``/proc``.

The logic lives in :mod:`protea.api.auth.bootstrap`; see it for what counts
as "first" and why.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

import protea.infrastructure.orm.models  # noqa: F401  (registers the tables the flush touches)
from protea.api.auth.bootstrap import OperatorKeyExistsError, mint_first_operator_key
from protea.infrastructure.session import build_session_factory, session_scope

_PROG = "bootstrap_operator_key"
_SOURCE = "scripts/bootstrap_operator_key.py"


def build_parser() -> argparse.ArgumentParser:
    """The command line. It carries a label and nothing secret, on purpose."""
    parser = argparse.ArgumentParser(
        prog=_PROG,
        description=(
            "Create the first operator API key on a database that has none. "
            "Reads PROTEA_DB_URL from the environment; prints the key once on stdout."
        ),
    )
    parser.add_argument(
        "--name",
        required=True,
        help="Label shown in key listings (not a secret), e.g. dev-operator.",
    )
    return parser


def _say(message: str) -> None:
    print(f"{_PROG}: {message}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    db_url = os.environ.get("PROTEA_DB_URL", "").strip()
    if not db_url:
        _say("PROTEA_DB_URL is not set in the environment; refusing to guess a target")
        return 78
    try:
        # Earlier SQLAlchemy releases quote the whole URL, password included,
        # in this error, so the message is replaced rather than shown.
        target = make_url(db_url).render_as_string(hide_password=True)
    except (ArgumentError, ValueError):
        _say("PROTEA_DB_URL is not a database URL")
        return 78
    _say(f"target {target}")

    factory = build_session_factory(db_url)
    try:
        with session_scope(factory) as session:
            minted = mint_first_operator_key(session, name=args.name, source=_SOURCE)
    except (OperatorKeyExistsError, ValueError) as exc:
        _say(f"REFUSING, {exc}")
        return 1

    sys.stdout.write(minted.raw_key + "\n")
    sys.stdout.flush()
    _say(f"minted {minted.role} key {minted.prefix}... id={minted.key_id} name={minted.name!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
