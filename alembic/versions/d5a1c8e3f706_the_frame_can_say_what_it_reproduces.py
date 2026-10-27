"""The frame can say what it reproduces

The campaign's headline result is that one window's truth reproduces an
external reference's, counter by counter. Until now the platform could not
state it: the comparison lived in ``tests/test_long_window_acceptance.py``,
which is the right place to keep a contract from rotting and the wrong place
to keep a finding. A test says the claim held on somebody's machine at some
commit; a reader of the platform saw nothing.

So the claim becomes a row with a job behind it and a digest beside it.

WHAT IDENTIFIES ONE CLAIM: the window, the reference, and the reference's
BYTES. The third is in the key because an acceptance contract is a file that
can be regenerated, so a reference whose counters changed is a different claim
about the same window; overwriting the first would lose the disagreement the
change may have introduced.

WHAT THE ROW HOLDS IS THE WHOLE VERDICT. ``compared`` is the denominator any
claim has to quote -- measured on the long window, 26 of the reference's 34
counters are readable on both sides, 7 are quantities this record does not
keep and 1 belongs to the run rather than the window. ``disagreements`` keeps
both values per counter. A boolean with no denominator is the kind of number
this campaign keeps deleting.

Catalog-only: a new table, no backfill. The first claim is written by
``verify_window_against_reference``.

Revision ID: d5a1c8e3f706
Revises: c3f8a2e07d14
Create Date: 2026-10-27 19:05:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d5a1c8e3f706"
down_revision: str | Sequence[str] | None = "c3f8a2e07d14"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "reference_reproduction",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "evaluation_set_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("evaluation_set.id"),
            nullable=False,
        ),
        sa.Column("reference_key", sa.String(length=128), nullable=False),
        sa.Column("reference_digest", sa.String(length=64), nullable=False),
        sa.Column("reference_commit", sa.String(length=64), nullable=True),
        sa.Column("compared", sa.Integer(), nullable=False),
        sa.Column("agreed", sa.Integer(), nullable=False),
        sa.Column("disagreements", postgresql.JSONB(), nullable=False),
        sa.Column("absent", postgresql.JSONB(), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("job.id"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_unique_constraint(
        "uq_reference_reproduction_claim",
        "reference_reproduction",
        ["evaluation_set_id", "reference_key", "reference_digest"],
    )
    op.create_index(
        "ix_reference_reproduction_set", "reference_reproduction", ["evaluation_set_id"]
    )


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    claims = bind.execute(sa.text("select count(*) from reference_reproduction")).scalar_one()
    if claims:
        raise RuntimeError(
            f"{claims} reproduction claims are recorded; dropping the table would delete "
            "the only place the platform states that a window reproduces a reference, and "
            "the comparison is not recoverable from the window alone. Export them first."
        )
    op.drop_index("ix_reference_reproduction_set", table_name="reference_reproduction")
    op.drop_table("reference_reproduction")
