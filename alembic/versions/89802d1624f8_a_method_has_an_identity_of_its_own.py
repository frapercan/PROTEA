"""A method has an identity of its own: the four method tables (ADR-D60, decision 3)

Until this revision a method could enter the record only as an embedding
configuration, because ``prediction_set.embedding_config_id`` was NOT NULL. This
revision gives the method its own rows and lets a prediction set name one:

* ``method``: one row per method spec, its id the uuid5 of the spec's canonical
  JSON and ``spec_sha256`` the sha256 of the same bytes
  (``protea.core.contracts.method``);
* ``method_training_window``: the windows a method's fitted pins were trained
  on, by evaluation set id, with the role ``fit`` or ``holdout`` and at most one
  holdout per method;
* ``method_pin``: the bytes a method depends on, by role and sha256, with their
  references;
* ``prediction_explanation``: why a method predicted a term for a protein;
* ``prediction_set.method_id``, a nullable foreign key, while
  ``prediction_set.embedding_config_id`` becomes nullable and a CHECK requires
  one of the two, so a set always names what produced it.

Every existing prediction set has a configuration and no method, so it passes
the CHECK as it stands and reads exactly as before.

The vocabularies are written out literally rather than imported, so this
revision keeps creating what it created when it was written.
``tests/test_method_tables.py`` checks they agree with the models today.

THE DOWNGRADE REFUSES ONE CASE. A prediction set with no configuration cannot
go back under a NOT NULL column, so the downgrade names how many there are and
stops instead of failing inside ``ALTER TABLE``. Otherwise it drops what the
upgrade made, method rows included, as an additive revision does.

Revision ID: 89802d1624f8
Revises: b7c3f91d2a04
Create Date: 2026-11-15 22:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "89802d1624f8"
down_revision: str | Sequence[str] | None = "b7c3f91d2a04"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: Every CHECK this revision creates, by table and name.
CHECKS: dict[str, dict[str, str]] = {
    "method": {
        "ck_method_family": (
            "family IN ('embedding-reranker', 'world-model', 'explainable-units')"
        ),
        "ck_method_runtime": "runtime IN ('native', 'container')",
        "ck_method_instance_release": "instance_release > 0",
        "ck_method_spec_sha256": "length(spec_sha256) = 64",
        "ck_method_short_name": "length(short_name) > 0",
        "ck_method_description": "length(description) > 0",
    },
    "method_training_window": {
        "ck_method_training_window_role": "role IN ('fit', 'holdout')",
    },
    "method_pin": {
        "ck_method_pin_sha256": "length(sha256) = 64",
    },
    "prediction_set": {
        "ck_prediction_set_has_a_producer": (
            "embedding_config_id IS NOT NULL OR method_id IS NOT NULL"
        ),
        "ck_prediction_set_a_configuration_has_a_limit": (
            "embedding_config_id IS NULL OR limit_per_entry IS NOT NULL"
        ),
    },
}

#: The partial unique index that keeps a method to one holdout window.
ONE_HOLDOUT = "uq_method_training_window_one_holdout"
ONE_HOLDOUT_WHERE = "role = 'holdout'"


def _checks(table: str) -> list[sa.CheckConstraint]:
    return [sa.CheckConstraint(text, name=name) for name, text in CHECKS[table].items()]


def _uuid(name: str, *, nullable: bool = False) -> sa.Column:
    return sa.Column(name, postgresql.UUID(as_uuid=True), nullable=nullable)


def _jsonb(name: str) -> sa.Column:
    return sa.Column(name, postgresql.JSONB(astext_type=sa.Text()), nullable=False)


def _create_method_tables() -> None:
    op.create_table(
        "method",
        _uuid("id"),
        sa.Column("family", sa.String(length=32), nullable=False),
        sa.Column("code", sa.String(length=64), nullable=False),
        sa.Column("short_name", sa.String(length=64), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("instance_release", sa.Integer(), nullable=False),
        _uuid("ontology_snapshot_id"),
        _jsonb("recipe"),
        sa.Column("runtime", sa.String(length=16), nullable=False),
        sa.Column("explains", sa.Boolean(), nullable=False),
        sa.Column("spec_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["ontology_snapshot_id"], ["ontology_snapshot.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("spec_sha256", name="uq_method_spec_sha256"),
        sa.UniqueConstraint("short_name", name="uq_method_short_name"),
        *_checks("method"),
    )
    op.create_index("ix_method_code", "method", ["code"])
    op.create_index("ix_method_ontology_snapshot_id", "method", ["ontology_snapshot_id"])
    op.create_table(
        "method_training_window",
        _uuid("method_id"),
        _uuid("evaluation_set_id"),
        sa.Column("role", sa.String(length=8), nullable=False),
        sa.ForeignKeyConstraint(["method_id"], ["method.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["evaluation_set_id"], ["evaluation_set.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("method_id", "evaluation_set_id"),
        *_checks("method_training_window"),
    )
    op.create_index(
        "ix_method_training_window_evaluation_set_id",
        "method_training_window",
        ["evaluation_set_id"],
    )
    op.create_index(
        ONE_HOLDOUT,
        "method_training_window",
        ["method_id"],
        unique=True,
        postgresql_where=sa.text(ONE_HOLDOUT_WHERE),
    )
    op.create_table(
        "method_pin",
        _uuid("method_id"),
        sa.Column("role", sa.String(length=64), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        _jsonb("refs"),
        sa.ForeignKeyConstraint(["method_id"], ["method.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("method_id", "role", "sha256"),
        *_checks("method_pin"),
    )
    op.create_index("ix_method_pin_sha256", "method_pin", ["sha256"])


def _create_explanations() -> None:
    op.create_table(
        "prediction_explanation",
        _uuid("prediction_set_id"),
        sa.Column("accession", sa.String(), nullable=False),
        sa.Column("go_term_id", sa.BigInteger(), nullable=False),
        _jsonb("payload"),
        sa.ForeignKeyConstraint(["prediction_set_id"], ["prediction_set.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["go_term_id"], ["go_term.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("prediction_set_id", "accession", "go_term_id"),
    )
    op.create_index(
        "ix_prediction_explanation_go_term_id", "prediction_explanation", ["go_term_id"]
    )


def upgrade() -> None:
    """Upgrade schema."""
    _create_method_tables()
    _create_explanations()
    op.add_column("prediction_set", _uuid("method_id", nullable=True))
    op.create_foreign_key(
        "fk_prediction_set_method_id",
        "prediction_set",
        "method",
        ["method_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index("ix_prediction_set_method_id", "prediction_set", ["method_id"])
    op.alter_column(
        "prediction_set",
        "embedding_config_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=True,
    )
    # A method's prediction set has no retrieval depth; a configuration's keeps one
    # (ck_prediction_set_a_configuration_has_a_limit).
    op.alter_column("prediction_set", "limit_per_entry", existing_type=sa.Integer(), nullable=True)
    for name, text in CHECKS["prediction_set"].items():
        op.create_check_constraint(name, "prediction_set", text)


def downgrade() -> None:
    """Downgrade schema."""
    without = (
        op.get_bind()
        .execute(sa.text("SELECT count(*) FROM prediction_set WHERE embedding_config_id IS NULL"))
        .scalar_one()
    )
    if without:
        raise RuntimeError(
            f"refusing to downgrade: {without} prediction set(s) have no embedding "
            "configuration, and embedding_config_id goes back to NOT NULL. Those sets were "
            "produced by a method with no configuration; delete them, or keep this revision."
        )
    for name in CHECKS["prediction_set"]:
        op.drop_constraint(name, "prediction_set", type_="check")
    op.alter_column("prediction_set", "limit_per_entry", existing_type=sa.Integer(), nullable=False)
    op.alter_column(
        "prediction_set",
        "embedding_config_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=False,
    )
    op.drop_index("ix_prediction_set_method_id", table_name="prediction_set")
    op.drop_constraint("fk_prediction_set_method_id", "prediction_set", type_="foreignkey")
    op.drop_column("prediction_set", "method_id")
    op.drop_index("ix_prediction_explanation_go_term_id", table_name="prediction_explanation")
    op.drop_table("prediction_explanation")
    op.drop_index("ix_method_pin_sha256", table_name="method_pin")
    op.drop_table("method_pin")
    op.drop_index(ONE_HOLDOUT, table_name="method_training_window")
    op.drop_index(
        "ix_method_training_window_evaluation_set_id", table_name="method_training_window"
    )
    op.drop_table("method_training_window")
    op.drop_index("ix_method_ontology_snapshot_id", table_name="method")
    op.drop_index("ix_method_code", table_name="method")
    op.drop_table("method")
