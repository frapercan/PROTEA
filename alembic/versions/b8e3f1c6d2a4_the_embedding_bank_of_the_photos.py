"""The embedding bank of the photos

ADR-D62. The bank is computed away from the server, one configuration per card,
and loaded by ``load_embedding_bank``: this adds the table that declares each
bank and the column that lets every ``sequence_embedding`` row name the bank
that wrote it.

- ``embedding_bank``: one row per (configuration, manifest), with the photos it
  covers, the counts, the model's revision, the software, the card, the token
  budgets, the manifest by URI and sha256, the loading status, the job, both
  revisions and ADR-D58's descriptors.
- ``sequence_embedding.embedding_bank_id``: nullable, so no stored row moves
  (NULL is a row the queue computed). Not indexed, on purpose: a bank counts its
  own rows as it writes them, and an index over every embedding row would cost
  more than the one reader it would serve. ``RESTRICT``: a bank cannot vanish
  while rows name it.

Catalog-only on ``sequence_embedding``: a nullable column without a default and a
foreign key that every existing row satisfies by being NULL.

Revision ID: b8e3f1c6d2a4
Revises: d2f6a8c4e0b7
Create Date: 2026-11-17 14:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b8e3f1c6d2a4"
down_revision: str | Sequence[str] | None = "d2f6a8c4e0b7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The model's checks as they stand at this revision, frozen here.
SHORT_NAME_CHECK = "short_name ~ '^[a-z0-9][a-z0-9-]*$' AND length(short_name) <= 64"
DESCRIPTIONS_CHECK = "length(description) > 0"
DIGESTS_CHECK = "length(manifest_sha256) = 64 AND length(sequences_sha256) = 64"
COUNTS_CHECK = (
    "sequences > 0 AND residues > 0 AND dim > 0 AND shards > 0 "
    "AND shards_loaded >= 0 AND shards_loaded <= shards "
    "AND rows_inserted >= 0 AND rows_present >= 0 "
    "AND rows_inserted + rows_present <= sequences"
)
STATUS_CHECK = (
    "(status = 'loading' AND loaded_at IS NULL) "
    "OR (status = 'loaded' AND loaded_at IS NOT NULL AND shards_loaded = shards "
    "AND rows_inserted + rows_present = sequences)"
)


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "embedding_bank",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "embedding_config_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("embedding_config.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("photos", postgresql.JSONB(), nullable=False),
        sa.Column("sequences", sa.Integer(), nullable=False),
        sa.Column("residues", sa.BigInteger(), nullable=False),
        sa.Column("dim", sa.Integer(), nullable=False),
        sa.Column("shards", sa.Integer(), nullable=False),
        sa.Column("shards_loaded", sa.Integer(), nullable=False),
        sa.Column("rows_inserted", sa.BigInteger(), nullable=False),
        sa.Column("rows_present", sa.BigInteger(), nullable=False),
        sa.Column("sequences_sha256", sa.String(length=64), nullable=False),
        sa.Column("model_revision", sa.String(length=64), nullable=False),
        sa.Column("software", postgresql.JSONB(), nullable=False),
        sa.Column("card", sa.String(length=128), nullable=False),
        sa.Column("token_budgets", postgresql.JSONB(), nullable=False),
        sa.Column("manifest", postgresql.JSONB(), nullable=False),
        sa.Column("manifest_uri", sa.String(length=512), nullable=False),
        sa.Column("manifest_sha256", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("short_name", sa.String(length=64), nullable=False, unique=True),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("code_revision", sa.String(length=64), nullable=True),
        sa.Column("builder_revision", sa.String(length=64), nullable=True),
        sa.Column(
            "job_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("job.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.Column("loaded_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "embedding_config_id", "manifest_sha256", name="uq_embedding_bank_config_manifest"
        ),
        sa.CheckConstraint(SHORT_NAME_CHECK, name="ck_embedding_bank_short_name"),
        sa.CheckConstraint(DESCRIPTIONS_CHECK, name="ck_embedding_bank_descriptions"),
        sa.CheckConstraint(DIGESTS_CHECK, name="ck_embedding_bank_digests"),
        sa.CheckConstraint(COUNTS_CHECK, name="ck_embedding_bank_counts"),
        sa.CheckConstraint(STATUS_CHECK, name="ck_embedding_bank_status"),
    )
    op.create_index(
        "ix_embedding_bank_embedding_config_id", "embedding_bank", ["embedding_config_id"]
    )
    op.add_column(
        "sequence_embedding",
        sa.Column(
            "embedding_bank_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(
                "embedding_bank.id",
                ondelete="RESTRICT",
                name="fk_sequence_embedding_embedding_bank",
            ),
            nullable=True,
        ),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        "fk_sequence_embedding_embedding_bank", "sequence_embedding", type_="foreignkey"
    )
    op.drop_column("sequence_embedding", "embedding_bank_id")
    op.drop_index("ix_embedding_bank_embedding_config_id", table_name="embedding_bank")
    op.drop_table("embedding_bank")
