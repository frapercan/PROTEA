"""The migration chain has one head.

CI builds its schema with ``create_all`` and never runs Alembic, so two
migrations written on the same parent would both pass it and leave the chain
with two heads, which ``alembic upgrade head`` then refuses on the day of the
deploy. Migration pull requests are written in parallel and merged one at a
time; each is rebased on ``develop`` and its ``down_revision`` set to the head it
finds before it merges, and this test fails the one whose parent went stale.
No database is needed: the scripts are read from ``alembic/versions``.
"""

from __future__ import annotations

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

_REPO = Path(__file__).resolve().parents[1]


def _scripts() -> ScriptDirectory:
    config = Config(str(_REPO / "alembic.ini"))
    config.set_main_option("script_location", str(_REPO / "alembic"))
    return ScriptDirectory.from_config(config)


def test_the_chain_has_exactly_one_head() -> None:
    heads = _scripts().get_heads()
    assert len(heads) == 1, f"the migrations have {len(heads)} heads: {sorted(heads)}"


def test_the_chain_has_exactly_one_base() -> None:
    bases = _scripts().get_bases()
    assert len(bases) == 1, f"the migrations have {len(bases)} bases: {sorted(bases)}"


def test_every_migration_file_is_a_revision_of_the_chain() -> None:
    revisions = {script.path for script in _scripts().walk_revisions()}
    files = {str(path) for path in (_REPO / "alembic" / "versions").glob("*.py")}
    assert files == revisions
