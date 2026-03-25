"""add go_term_relationship table

Revision ID: f1a2b3c4d5e6
Revises: e5f6a7b8c9d0
Create Date: 2026-03-10 00:00:00.000000
"""
from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "f1a2b3c4d5e6"
down_revision = "e5f6a7b8c9d0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "go_term_relationship",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("child_go_term_id", sa.BigInteger(), nullable=False),
        sa.Column("parent_go_term_id", sa.BigInteger(), nullable=False),
        sa.Column("relation_type", sa.String(40), nullable=False),
        sa.Column("ontology_snapshot_id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(["child_go_term_id"], ["go_term.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["parent_go_term_id"], ["go_term.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["ontology_snapshot_id"], ["ontology_snapshot.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("child_go_term_id", "parent_go_term_id", "relation_type",
                            name="uq_go_term_relationship"),
    )
    op.create_index("ix_go_term_relationship_child", "go_term_relationship", ["child_go_term_id"])
    op.create_index("ix_go_term_relationship_parent", "go_term_relationship", ["parent_go_term_id"])
    op.create_index("ix_go_term_relationship_snapshot", "go_term_relationship", ["ontology_snapshot_id"])


def downgrade() -> None:
    op.drop_table("go_term_relationship")
