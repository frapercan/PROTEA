"""Every member names its own taxon

A member of a photo gets its own NCBI taxon as the cut's GOA rows name it
(``protea.core.cut_photo.taxa``): an input of the explainable-units family's
combiner (the organism of the query), dated with the cut. Nullable, so no
stored row moves: a member without rows at the cut, or whose rows name two own
taxa, has none. ``load_cut_photo`` writes it for every new photo, and
``declare_member_taxa`` fills it for the photos already loaded.

Catalog-only: a nullable column without a default.

Revision ID: c3a9e7d1f5b2
Revises: b8e3f1c6d2a4
Create Date: 2026-11-17 17:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c3a9e7d1f5b2"
down_revision: str | Sequence[str] | None = "b8e3f1c6d2a4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("cut_photo_member", sa.Column("taxon_id", sa.Integer(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("cut_photo_member", "taxon_id")
