"""add_query_set

Revision ID: c3d4e5f6a7b8
Revises: 4f38043a5e41
Create Date: 2026-03-10 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "c3d4e5f6a7b8"
down_revision: Union[str, Sequence[str], None] = "4f38043a5e41"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "query_set",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "query_set_entry",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("query_set_id", sa.UUID(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("accession", sa.String(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["query_set_id"], ["query_set.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["sequence_id"], ["sequence.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "query_set_id", "accession", name="uq_query_set_entry_set_accession"
        ),
    )
    op.create_index(
        op.f("ix_query_set_entry_query_set_id"),
        "query_set_entry",
        ["query_set_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_query_set_entry_sequence_id"),
        "query_set_entry",
        ["sequence_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_query_set_entry_accession"),
        "query_set_entry",
        ["accession"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_query_set_entry_accession"), table_name="query_set_entry")
    op.drop_index(op.f("ix_query_set_entry_sequence_id"), table_name="query_set_entry")
    op.drop_index(op.f("ix_query_set_entry_query_set_id"), table_name="query_set_entry")
    op.drop_table("query_set_entry")
    op.drop_table("query_set")
