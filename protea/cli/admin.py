"""``protea-cli admin`` — break-glass user-management subcommands (FARM-AUTH.2).

This module wires Click subcommands for situations where an admin must
be created or inspected without starting the full FastAPI server. The
canonical bootstrap path is the lifespan hook in
:mod:`protea.api.auth.bootstrap`; the CLI is the break-glass
alternative for a fresh deployment where the API is not yet reachable.

Entry point
-----------

Registered in ``pyproject.toml``::

    [tool.poetry.scripts]
    protea-cli = "protea.cli.admin:cli"

Usage
-----

Bootstrap an admin from the environment (mirrors the lifespan hook)::

    PROTEA_BOOTSTRAP_ADMIN_EMAIL=admin@example.com \\
    PROTEA_BOOTSTRAP_ADMIN_PASSWORD=s3cr3t \\
    protea-cli admin bootstrap

Add a user interactively::

    protea-cli admin add-user --email user@example.com --role researcher

"""

from __future__ import annotations

import sys
from pathlib import Path

import click

from protea.api.auth.bootstrap import bootstrap_admin
from protea.infrastructure.session import build_session_factory, session_scope
from protea.infrastructure.settings import load_settings


def _load_factory():
    """Load settings and return a session factory bound to the configured DB."""
    project_root = Path(__file__).resolve().parents[2]
    settings = load_settings(project_root)
    return build_session_factory(settings.db_url)


@click.group()
def admin() -> None:
    """Admin user-management subcommands."""


@admin.command("bootstrap")
@click.option(
    "--email",
    envvar="PROTEA_BOOTSTRAP_ADMIN_EMAIL",
    required=True,
    help="Email for the bootstrap admin account.",
)
@click.option(
    "--password",
    envvar="PROTEA_BOOTSTRAP_ADMIN_PASSWORD",
    default=None,
    help=(
        "Plaintext password to set. When omitted a secure random password "
        "is generated and printed to stderr."
    ),
)
def bootstrap_cmd(email: str, password: str | None) -> None:
    """Ensure an admin with EMAIL exists; create one if absent (idempotent)."""
    factory = _load_factory()
    with session_scope(factory) as session:
        _user, created = bootstrap_admin(session, email, password=password)
    if created:
        click.echo(f"Admin created: {email}", err=False)
        sys.exit(0)
    else:
        click.echo(f"Admin already exists: {email}", err=False)
        sys.exit(0)


@admin.command("add-user")
@click.option("--email", required=True, help="Email address for the new user.")
@click.option(
    "--role",
    default="researcher",
    show_default=True,
    type=click.Choice(["guest", "researcher", "operator", "admin"], case_sensitive=False),
    help="Role to assign to the new user.",
)
@click.option(
    "--password",
    default=None,
    help=(
        "Plaintext password. When omitted the CLI prompts interactively "
        "(use --password-prompt to force the prompt explicitly)."
    ),
)
@click.option(
    "--password-prompt",
    "prompt_password",
    is_flag=True,
    default=False,
    help="Always prompt for the password even if --password was supplied.",
)
def add_user_cmd(
    email: str,
    role: str,
    password: str | None,
    prompt_password: bool,
) -> None:
    """Add a new user account.

    Exits 0 on success; exits 1 if the email already exists.
    """
    from protea.api.auth.passwords import hash_password
    from protea.infrastructure.orm.models.user import User, UserRole, UserStatus

    if prompt_password or password is None:
        password = click.prompt("Password", hide_input=True, confirmation_prompt=True)

    role_enum = UserRole(role.lower())
    factory = _load_factory()
    try:
        with session_scope(factory) as session:
            from sqlalchemy import select

            existing = session.scalar(select(User).where(User.email == email))
            if existing is not None:
                click.echo(f"Error: email already exists: {email}", err=True)
                sys.exit(1)
            username_candidate = email.split("@")[0]
            user = User(
                email=email,
                username=username_candidate,
                password_hash=hash_password(password),
                role=role_enum,
                status=UserStatus.ACTIVE,
            )
            session.add(user)
    except SystemExit:
        raise
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)

    click.echo(f"User created: {email} (role={role})")
    sys.exit(0)


@click.group()
def cli() -> None:
    """PROTEA command-line interface."""


cli.add_command(admin)

__all__ = ["cli", "admin"]
