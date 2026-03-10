"""add esm3c chunking normalize_residues

Revision ID: a1b2c3d4e5f6
Revises: cdd8510858db
Create Date: 2026-03-08 12:00:00.000000

Changes
-------
embedding_config
    + normalize_residues  BOOLEAN NOT NULL DEFAULT false
    + use_chunking        BOOLEAN NOT NULL DEFAULT false
    + chunk_size          INTEGER NOT NULL DEFAULT 512
    + chunk_overlap       INTEGER NOT NULL DEFAULT 0

sequence_embedding
    + chunk_index_s  INTEGER NOT NULL DEFAULT 0
    + chunk_index_e  INTEGER (nullable)
    - uq_seq_embedding_seq_config  (sequence_id, embedding_config_id)
    + uq_seq_embedding_seq_config_chunk  (sequence_id, embedding_config_id, chunk_index_s)
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, Sequence[str], None] = "cdd8510858db"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── embedding_config: new fields ─────────────────────────────────────────
    op.add_column(
        "embedding_config",
        sa.Column(
            "normalize_residues",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column(
        "embedding_config",
        sa.Column(
            "use_chunking",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )
    op.add_column(
        "embedding_config",
        sa.Column(
            "chunk_size",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("512"),
        ),
    )
    op.add_column(
        "embedding_config",
        sa.Column(
            "chunk_overlap",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )

    # ── sequence_embedding: new columns ──────────────────────────────────────
    op.add_column(
        "sequence_embedding",
        sa.Column(
            "chunk_index_s",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.add_column(
        "sequence_embedding",
        sa.Column("chunk_index_e", sa.Integer(), nullable=True),
    )

    # ── replace unique constraint ─────────────────────────────────────────────
    op.drop_constraint(
        "uq_seq_embedding_seq_config",
        "sequence_embedding",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_seq_embedding_seq_config_chunk",
        "sequence_embedding",
        ["sequence_id", "embedding_config_id", "chunk_index_s"],
    )


def downgrade() -> None:
    # Restore old unique constraint (drop chunk columns first)
    op.drop_constraint(
        "uq_seq_embedding_seq_config_chunk",
        "sequence_embedding",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_seq_embedding_seq_config",
        "sequence_embedding",
        ["sequence_id", "embedding_config_id"],
    )

    op.drop_column("sequence_embedding", "chunk_index_e")
    op.drop_column("sequence_embedding", "chunk_index_s")

    op.drop_column("embedding_config", "chunk_overlap")
    op.drop_column("embedding_config", "chunk_size")
    op.drop_column("embedding_config", "use_chunking")
    op.drop_column("embedding_config", "normalize_residues")
