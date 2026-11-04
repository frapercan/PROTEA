"""Add the four tables of an evaluated exploration (ADR-D55)

An exploration ties a falsifiable hypothesis to one evaluation window, rules
frozen with a fingerprint before any number is read, the arms that answer it,
a report generated from rows and a review in two steps. Four tables and no new
column on an existing one:

* ``exploration``: the hypothesis, its window, its rules and its review;
* ``exploration_link``: one row per object it uses or produces, as an exclusive
  arc of foreign keys with ``RESTRICT``;
* ``exploration_event``: an append-only log, enforced by a trigger;
* ``external_reference``: figures published by others, reusable as context.

The vocabularies are written out literally rather than imported, so this
revision keeps creating what it created when it was written.
``tests/test_exploration_tables.py`` checks they agree with the models today.

Additive, so downgrade is a clean drop.

Revision ID: b2f6c3d8e154
Revises: a7d3f1c9e842
Create Date: 2026-11-04 10:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b2f6c3d8e154"
down_revision: str | Sequence[str] | None = "a7d3f1c9e842"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The exclusive arc's targets, in the order the CHECK sums them.
_TARGETS: tuple[tuple[str, str], ...] = (
    ("job_id", "job"),
    ("prediction_set_id", "prediction_set"),
    ("evaluation_result_id", "evaluation_result"),
    ("model_weights_id", "model_weights"),
    ("embedding_config_id", "embedding_config"),
    ("query_set_id", "query_set"),
    ("annotation_set_id", "annotation_set"),
    ("information_accretion_set_id", "information_accretion_set"),
    ("external_reference_id", "external_reference"),
    ("target_exploration_id", "exploration"),
)

_ONE_TARGET = (
    " + ".join(
        f"(CASE WHEN {column} IS NOT NULL THEN 1 ELSE 0 END)"
        for column in (*(c for c, _ in _TARGETS), "store_key")
    )
    + " = 1"
)

#: Every CHECK this revision creates, by table and name.
CHECKS: dict[str, dict[str, str]] = {
    "external_reference": {
        "ck_external_reference_kind": (
            "kind IN ('paper', 'preprint', 'challenge', 'leaderboard', 'software', 'dataset')"
        ),
        "ck_external_reference_store_key_with_its_sha": (
            "(store_key IS NULL) = (store_sha256 IS NULL)"
        ),
    },
    "exploration": {
        "ck_exploration_kind": "kind IN ('one_axis', 'full_method', 'instrument')",
        "ck_exploration_state": "state IN ('draft', 'running', 'reported', 'decided')",
        "ck_exploration_archived_reason": (
            "archived_reason IS NULL OR archived_reason IN "
            "('abandoned', 'closed', 'superseded', 'withdrawn')"
        ),
        "ck_exploration_grade": (
            "grade IS NULL OR grade IN ('thesis', 'development', 'check', 'exploratory')"
        ),
        "ck_exploration_verdict": (
            "verdict IS NULL OR verdict IN ('for', 'against', 'equal', 'underpowered', 'blocked')"
        ),
        "ck_exploration_decision": "decision IS NULL OR decision IN ('accepted', 'rejected')",
        "ck_exploration_rules_frozen_together": (
            "(rules_sha256 IS NULL) = (rules_frozen_at IS NULL) "
            "AND (rules_frozen_at IS NULL) = (rules_frozen_by IS NULL)"
        ),
        "ck_exploration_only_a_draft_has_open_rules": (
            "(state = 'draft') = (rules_sha256 IS NULL)"
        ),
        "ck_exploration_only_decided_carries_a_decision": (
            "(state = 'decided') = (decision IS NOT NULL)"
        ),
        "ck_exploration_archived_together": "(archived_reason IS NULL) = (archived_at IS NULL)",
        "ck_exploration_launcher_is_not_the_proposer": "launcher IS NULL OR launcher <> proposer",
    },
    "exploration_link": {
        "ck_exploration_link_role": (
            "role IN ('input', 'arm', 'result', 'strata', 'comparison', 'job', 'temporary', "
            "'reference', 'supports', 'supersedes')"
        ),
        "ck_exploration_link_one_target": _ONE_TARGET,
        "ck_exploration_link_store_key_with_its_sha": (
            "(store_key IS NULL) = (store_sha256 IS NULL)"
        ),
        "ck_exploration_link_temporary_expires": "role <> 'temporary' OR expires_at IS NOT NULL",
        "ck_exploration_link_not_to_itself": (
            "target_exploration_id IS NULL OR target_exploration_id <> exploration_id"
        ),
    },
    "exploration_event": {
        "ck_exploration_event_actor": "actor_type IN ('user', 'api_key', 'server')",
        "ck_exploration_event_type": (
            "event IN ('created', 'launched', 'amendment', 'step_failed', 'report', "
            "'check_answered', 'objection', 'reply', 'reviewed', 'decision', 'deployed', "
            "'archived', 'marked_for_deletion', 'deleted')"
        ),
    },
}

APPEND_ONLY_FUNCTION = """
CREATE OR REPLACE FUNCTION exploration_event_is_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'exploration_event is append only: % refused', TG_OP;
END;
$$
"""

APPEND_ONLY_TRIGGER = """
CREATE TRIGGER exploration_event_append_only
    BEFORE UPDATE OR DELETE ON exploration_event
    FOR EACH ROW EXECUTE FUNCTION exploration_event_is_append_only()
"""


def _checks(table: str) -> list[sa.CheckConstraint]:
    return [sa.CheckConstraint(text, name=name) for name, text in CHECKS[table].items()]


def _uuid(name: str, *, nullable: bool = True) -> sa.Column:
    return sa.Column(name, postgresql.UUID(as_uuid=True), nullable=nullable)


def _now(name: str) -> sa.Column:
    return sa.Column(
        name, sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
    )


def _jsonb(name: str) -> sa.Column:
    return sa.Column(name, postgresql.JSONB(astext_type=sa.Text()), nullable=False)


def _restrict(column: str, table: str) -> sa.ForeignKeyConstraint:
    return sa.ForeignKeyConstraint([column], [f"{table}.id"], ondelete="RESTRICT")


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "external_reference",
        _uuid("id", nullable=False),
        sa.Column("title", sa.String(length=300), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("url", sa.String(length=1024), nullable=True),
        sa.Column("citation", sa.Text(), nullable=True),
        _now("captured_at"),
        sa.Column("captured_by", sa.String(length=200), nullable=False),
        sa.Column("store_key", sa.String(length=512), nullable=True),
        sa.Column("store_sha256", sa.String(length=64), nullable=True),
        _jsonb("profile"),
        sa.PrimaryKeyConstraint("id"),
        *_checks("external_reference"),
    )
    op.create_table(
        "exploration",
        _uuid("id", nullable=False),
        sa.Column("number", sa.Integer(), nullable=False),
        sa.Column("title", sa.String(length=200), nullable=False),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("hypothesis", sa.Text(), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("state", sa.String(length=16), server_default="draft", nullable=False),
        sa.Column("archived_reason", sa.String(length=16), nullable=True),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        _uuid("evaluation_set_id", nullable=False),
        _jsonb("rules"),
        sa.Column("rules_sha256", sa.String(length=64), nullable=True),
        sa.Column("rules_frozen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rules_frozen_by", sa.String(length=200), nullable=True),
        sa.Column("post_hoc", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("grade", sa.String(length=16), nullable=True),
        sa.Column("verdict", sa.String(length=16), nullable=True),
        sa.Column("proposer", sa.String(length=200), nullable=False),
        sa.Column("launcher", sa.String(length=200), nullable=True),
        sa.Column("reviewer", sa.String(length=200), nullable=True),
        sa.Column("decision", sa.String(length=8), nullable=True),
        sa.Column("decision_text", sa.Text(), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("decided_by", sa.String(length=200), nullable=True),
        sa.Column("report_sha256", sa.String(length=64), nullable=True),
        sa.Column("checklist_version", sa.String(length=16), nullable=True),
        _now("created_at"),
        _restrict("evaluation_set_id", "evaluation_set"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("number", name="uq_exploration_number"),
        *_checks("exploration"),
    )
    op.create_index("ix_exploration_evaluation_set_id", "exploration", ["evaluation_set_id"])
    op.create_table(
        "exploration_link",
        _uuid("id", nullable=False),
        _uuid("exploration_id", nullable=False),
        sa.Column("role", sa.String(length=16), nullable=False),
        sa.Column("arm", sa.String(length=64), nullable=True),
        sa.Column("owned", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        _now("created_at"),
        *(_uuid(column) for column, _ in _TARGETS),
        sa.Column("store_key", sa.String(length=512), nullable=True),
        sa.Column("store_sha256", sa.String(length=64), nullable=True),
        _restrict("exploration_id", "exploration"),
        *(_restrict(column, table) for column, table in _TARGETS),
        sa.PrimaryKeyConstraint("id"),
        *_checks("exploration_link"),
    )
    op.create_index("ix_exploration_link_exploration_id", "exploration_link", ["exploration_id"])
    op.create_table(
        "exploration_event",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        _uuid("exploration_id", nullable=False),
        _now("at"),
        sa.Column("actor", sa.String(length=200), nullable=False),
        sa.Column("actor_type", sa.String(length=8), nullable=False),
        sa.Column("event", sa.String(length=24), nullable=False),
        _uuid("link_id"),
        _jsonb("body"),
        _restrict("exploration_id", "exploration"),
        sa.ForeignKeyConstraint(["link_id"], ["exploration_link.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        *_checks("exploration_event"),
    )
    op.create_index("ix_exploration_event_exploration_id", "exploration_event", ["exploration_id"])
    op.execute(APPEND_ONLY_FUNCTION)
    op.execute(APPEND_ONLY_TRIGGER)


def downgrade() -> None:
    """Downgrade schema."""
    op.execute("DROP TRIGGER IF EXISTS exploration_event_append_only ON exploration_event")
    op.execute("DROP FUNCTION IF EXISTS exploration_event_is_append_only()")
    op.drop_index("ix_exploration_event_exploration_id", table_name="exploration_event")
    op.drop_table("exploration_event")
    op.drop_index("ix_exploration_link_exploration_id", table_name="exploration_link")
    op.drop_table("exploration_link")
    op.drop_index("ix_exploration_evaluation_set_id", table_name="exploration")
    op.drop_table("exploration")
    op.drop_table("external_reference")
