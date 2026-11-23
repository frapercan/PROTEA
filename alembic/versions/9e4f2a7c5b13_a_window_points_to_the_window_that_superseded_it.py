"""A window points to the window that superseded it

The windows a frame leaves behind keep their rows and their results: those are
records of what was measured. What they lacked was a way to say they no longer
stand. ``supersede_evaluation_sets`` marks them: ``superseded_by_id`` points to
the successor, a window over the same two corpora, and ``superseded_at`` and
``supersession_note`` say when and why.

All three are NULL on every existing row, so no stored window moves. The check
holds them together (all three or none) and refuses a window that points at
itself; the foreign key restricts deleting a successor while anything points at
it.

Revision ID: 9e4f2a7c5b13
Revises: 7c2e9b4d1a6f
Create Date: 2026-11-20 15:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9e4f2a7c5b13"
down_revision: str | Sequence[str] | None = "7c2e9b4d1a6f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The model's check, by the same text; a test compares the two.
SUPERSESSION_CHECK = (
    "(superseded_by_id IS NULL) = (superseded_at IS NULL) "
    "AND (superseded_by_id IS NULL) = (supersession_note IS NULL) "
    "AND (superseded_by_id IS NULL OR superseded_by_id <> id)"
)


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "evaluation_set",
        sa.Column("superseded_by_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "evaluation_set",
        sa.Column("superseded_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column("evaluation_set", sa.Column("supersession_note", sa.Text(), nullable=True))
    op.create_foreign_key(
        "fk_evaluation_set_superseded_by_id",
        "evaluation_set",
        "evaluation_set",
        ["superseded_by_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index("ix_evaluation_set_superseded_by_id", "evaluation_set", ["superseded_by_id"])
    op.create_check_constraint(
        "ck_evaluation_set_supersession", "evaluation_set", SUPERSESSION_CHECK
    )


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    marked = bind.execute(
        sa.text("select count(*) from evaluation_set where superseded_by_id is not null")
    ).scalar_one()
    if marked:
        raise RuntimeError(
            f"{marked} evaluation_set rows are marked superseded; dropping the columns "
            "would lose which window replaced them. Clear the marks first."
        )
    op.drop_constraint("ck_evaluation_set_supersession", "evaluation_set", type_="check")
    op.drop_index("ix_evaluation_set_superseded_by_id", table_name="evaluation_set")
    op.drop_constraint("fk_evaluation_set_superseded_by_id", "evaluation_set", type_="foreignkey")
    op.drop_column("evaluation_set", "supersession_note")
    op.drop_column("evaluation_set", "superseded_at")
    op.drop_column("evaluation_set", "superseded_by_id")
