"""Per-config uniform embedding scale so high-magnitude PLM layers fit halfvec

The ``sequence_embedding.embedding`` column is a pgvector ``HALFVEC`` (fp16,
max magnitude ~65504). The champion base (Ankh-base last layer 48) fits fp16,
but mid-transformer layers do not: Ankh-base layer 10 reaches ``|max| ~4.9e5``,
which overflows fp16 and made ``compute_embeddings`` fail with
``psycopg.errors.DataException: infinite value not allowed in halfvec``.

This adds a per-config ``embedding_scale`` (double precision, NOT NULL, default
``1.0``). The store path divides each per-sequence embedding by the owning
config's ``embedding_scale`` before the halfvec INSERT. A uniform per-config
divisor is mathematically safe for every downstream consumer: cosine-KNN is
scale-invariant, and the downstream per-dim z-score standardisation absorbs a
uniform scale, so storing ``embedding / scale`` is equivalent to the raw
embedding.

Backward compatible: the ``1.0`` server default means every existing config
(including the byte-for-byte champion) is unchanged. An operator sets a larger
scale (e.g. 32) only on high-magnitude configs before computing their
embeddings.

Additive and instant: ``ADD COLUMN ... NOT NULL DEFAULT 1.0`` is a
metadata-only change on modern Postgres (constant default), no table rewrite.

Revision ID: b4e2a9c7d1f3
Revises: d7f3a9c1e5b2
Create Date: 2026-07-11 12:00:00.000000

"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b4e2a9c7d1f3"
down_revision: str | Sequence[str] | None = "d7f3a9c1e5b2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "embedding_config",
        sa.Column(
            "embedding_scale",
            sa.Float(),
            nullable=False,
            server_default="1.0",
        ),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("embedding_config", "embedding_scale")
