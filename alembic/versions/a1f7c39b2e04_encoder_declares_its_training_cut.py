"""a fitted encoder declares the annotation release it was fitted on

Revision ID: a1f7c39b2e04
Revises: c4d8e2f1a903
Create Date: 2026-08-20

An encoder fitted on annotations has a temporal cut, and nothing in the
schema made it say so. The consequence is not that a particular artifact
is contaminated. It is that the question cannot be asked: an encoder that
does not declare which release it saw can be certified neither clean nor
contaminated for any frame, and an unfalsifiable artifact is worse than a
known dirty one, because a known contamination can be excluded.

The deployed sparse encoder is exactly that today. Its stored metadata
declares pooling, dictionary size, top-k, input width and backbone, and
nothing about when it was fitted.

NULL means NOT FITTED, which is the honest state for a pretrained backbone
used as it ships: it saw no annotations of ours and has no cut. It does
not mean unknown, and keeping those apart is the entire purpose of the
column.
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "a1f7c39b2e04"
down_revision = "c4d8e2f1a903"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "embedding_config",
        sa.Column("trained_on_annotation_set_id", sa.UUID(as_uuid=True), nullable=True),
    )
    op.create_index(
        "ix_embedding_config_trained_on",
        "embedding_config",
        ["trained_on_annotation_set_id"],
    )
    # RESTRICT rather than CASCADE or SET NULL. Deleting an annotation set
    # out from under a fitted encoder would either delete the encoder,
    # losing an artifact that is still valid, or silently blank its cut,
    # turning a certifiable encoder back into an unfalsifiable one. Both
    # are worse than refusing the delete.
    op.create_foreign_key(
        "fk_embedding_config_trained_on",
        "embedding_config",
        "annotation_set",
        ["trained_on_annotation_set_id"],
        ["id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    op.drop_constraint("fk_embedding_config_trained_on", "embedding_config", type_="foreignkey")
    op.drop_index("ix_embedding_config_trained_on", table_name="embedding_config")
    op.drop_column("embedding_config", "trained_on_annotation_set_id")
