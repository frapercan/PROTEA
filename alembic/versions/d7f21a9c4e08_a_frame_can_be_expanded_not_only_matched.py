"""a frame can be expanded, not only matched

Revision ID: d7f21a9c4e08
Revises: c4e88a1b6d30

``evaluation_result.frame_digest`` is a content address. It answers whether two
results are comparable and nothing else: given the digest alone, no one can say
which window, which ontology pivot or which accretion table it stands for.

That is the same defect the digest was introduced to fix, one level along. A
harness label named the harness and not the parameters; a bare digest names the
parameters collectively and none of them individually. Both let a reader confirm
a property they cannot inspect, and the second is worse in one way: it is
recoverable only by recomputing from rows that may no longer exist.

So the material is stored beside the address. The table is write-once by
construction: the digest is the primary key and is derived from the material, so
a second write with different material under the same key is a hash collision or
a producer bug, and either way it must fail rather than overwrite. The
provenance column carries what produced the first sighting, including the code
version, so a frame can be traced to the tree that defined it.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "d7f21a9c4e08"
down_revision = "c4e88a1b6d30"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "evaluation_frame",
        # The address IS the identity. Not a surrogate key beside it: a frame
        # with two rows would let two materials claim one address, which is the
        # one thing the digest promises cannot happen.
        sa.Column("digest", sa.String(length=32), primary_key=True),
        sa.Column("material", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("fields", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("provenance", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "first_sealed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_table("evaluation_frame")
