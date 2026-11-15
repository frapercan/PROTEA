"""A process that declares itself dev starts only on dev's database and vhost.

Layer two of the environment isolation. Layer one is a separate stack: dev's
own Postgres, RabbitMQ and bucket, on their own ports, with their own
credentials. That layer fails open on a single wrong line: an env file that
kept a production URL, or a unit pointed at the wrong file, connects a dev
worker to production's broker, and the first message it takes runs against
production's database. This is the second layer, in code. A process started
with ``PROTEA_ENVIRONMENT=dev`` reads the database name and the AMQP vhost out
of the URLs it was given and refuses to start unless both are the names dev
declares: ``PROTEA_DEV_DATABASE_NAME`` (default ``protea_dev``) and
``PROTEA_DEV_AMQP_VHOST`` (default ``protea-dev``).

The refusal exits with ``EX_CONFIG`` (78) and one line on stderr, so a dev
unit with ``RestartPreventExitStatus=78`` stops instead of restarting into the
same refusal every few seconds.

Any other value of ``PROTEA_ENVIRONMENT``, or none, starts exactly as before.
Production sets none, and ``test``, which the suite sets, only relaxes the
rate limiter.

THE NAMES ARE READ THE WAY THE CONNECTIONS READ THEM, which is the point of
reading them at all. The vhost comes from ``pika.URLParameters``, the parser
the consumers and the publisher connect with. The database comes from
SQLAlchemy's URL with its ``dbname`` query parameter applied on top, because
SQLAlchemy hands the query to psycopg after the path, so a ``dbname`` there
overrides the database the path names. A check that parsed differently from
the connection would pass a URL that connects somewhere else.

Nothing here opens a connection, and the reason never quotes a URL: URLs
carry passwords, and the line lands in the journal.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pika
from sqlalchemy.engine import make_url

if TYPE_CHECKING:
    from protea.infrastructure.settings import Settings

#: sysexits ``EX_CONFIG``: a configuration error. A dev unit lists it in
#: ``RestartPreventExitStatus=``, so the refusal stops the unit.
EX_CONFIG = 78

#: The ``PROTEA_ENVIRONMENT`` value that turns the check on. Compared after
#: ``strip().lower()``, the reading ``protea.api.rate_limit`` gives the same
#: variable, so the two never disagree about whether a process is dev.
DEV_ENVIRONMENT = "dev"

_REFUSAL = "refusing to start:"


def database_name(db_url: str) -> str | None:
    """The database a connection opened from ``db_url`` would land in.

    ``None`` when the URL names none, which libpq would resolve from the
    environment or the user name, out of this check's sight.
    """
    url = make_url(db_url)
    override = url.query.get("dbname")
    if override is None:
        return url.database or None
    # A repeated parameter arrives as a tuple, which names no single database.
    return override if isinstance(override, str) else None


def amqp_vhost(amqp_url: str) -> str:
    """The vhost a connection opened from ``amqp_url`` would land in.

    A URL with no path means the default vhost ``/``, as pika reads it.
    """
    return str(pika.URLParameters(amqp_url).virtual_host)


def declares_dev(settings: Settings) -> bool:
    """Whether the process was started with ``PROTEA_ENVIRONMENT=dev``.

    Anything that is not a string reads as undeclared, so a settings double
    built without the field starts as it always did.
    """
    declared = getattr(settings, "environment", None)
    return isinstance(declared, str) and declared.strip().lower() == DEV_ENVIRONMENT


def environment_mismatch(settings: Settings) -> str | None:
    """Why this process must not start, in one line, or ``None`` if it may.

    ``None`` whenever the process does not declare itself dev. The line names
    what was expected and what was found, never the URLs.
    """
    if not declares_dev(settings):
        return None
    expected_database = settings.dev_database_name
    expected_vhost = settings.dev_amqp_vhost
    # Whatever stops a name from being read refuses: an unreadable URL can
    # never be shown to be dev's.
    try:
        found_database = database_name(settings.db_url)
    except Exception:
        return f"{_REFUSAL} PROTEA_ENVIRONMENT=dev and the database URL cannot be parsed"
    try:
        found_vhost = amqp_vhost(settings.amqp_url)
    except Exception:
        return f"{_REFUSAL} PROTEA_ENVIRONMENT=dev and the AMQP URL cannot be parsed"
    if found_database == expected_database and found_vhost == expected_vhost:
        return None
    return (
        f"{_REFUSAL} PROTEA_ENVIRONMENT=dev requires database {expected_database!r}"
        f" (PROTEA_DEV_DATABASE_NAME) and AMQP vhost {expected_vhost!r}"
        f" (PROTEA_DEV_AMQP_VHOST), but was given database {found_database!r}"
        f" and vhost {found_vhost!r}"
    )


def exit_on_environment_mismatch(settings: Settings) -> None:
    """Exit with ``EX_CONFIG`` and the one-line reason if the process is out of place.

    Called right after the settings load, by ``scripts/worker.py`` (every
    queue, the reaper included) and by ``create_app``, so a refusal happens
    before a session factory, a consumer or a route exists. It raises
    ``SystemExit`` rather than calling ``os._exit``: uvicorn lets a
    ``SystemExit`` from the app factory through, so the API exits 78 like a
    worker does.
    """
    reason = environment_mismatch(settings)
    if reason is None:
        return
    print(reason, file=sys.stderr, flush=True)
    raise SystemExit(EX_CONFIG)
