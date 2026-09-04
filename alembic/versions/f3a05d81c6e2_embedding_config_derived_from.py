"""embedding_config.derived_from_embedding_config_id

The three learned rung2 encoders are built on top of another config's vectors.
The only record of which one was the hash ``0868f1ff`` glued into their
``display_name`` — a string that no join can follow and that any rename
destroys.

Every other fact those names carried already has a home: the bank they were
fitted against is ``trained_on_annotation_set_id``, the pooling is ``pooling``,
the architecture is the name itself. The parent had none, so it lived in the
one place left. This gives it one.

Nullable on purpose: a pretrained config is not derived from anything, and a
zero-value sentinel would make "no parent" and "parent unknown" the same
answer.

Revision ID: f3a05d81c6e2
Revises: e1c7a94f2b30
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "f3a05d81c6e2"
down_revision = "e1c7a94f2b30"
branch_labels = None
depends_on = None

_COLUMN = "derived_from_embedding_config_id"


def upgrade() -> None:
    op.add_column(
        "embedding_config",
        sa.Column(_COLUMN, postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_embedding_config_derived_from",
        "embedding_config",
        "embedding_config",
        [_COLUMN],
        ["id"],
        ondelete="RESTRICT",
    )

    # Backfill from the hash the display name carries. Matching on the id
    # prefix rather than parsing the name: the prefix is what the name holds
    # and what the id is, so the join is between two things that are the same.
    op.execute(
        sa.text(
            f"UPDATE embedding_config c SET {_COLUMN} = parent.id "
            "FROM embedding_config parent "
            "WHERE c.display_name LIKE '%:' || left(parent.id::text, 8) "
            "AND parent.id <> c.id"
        )
    )


def downgrade() -> None:
    op.drop_constraint("fk_embedding_config_derived_from", "embedding_config", type_="foreignkey")
    op.drop_column("embedding_config", _COLUMN)
