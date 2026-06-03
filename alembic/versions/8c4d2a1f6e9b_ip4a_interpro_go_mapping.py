"""ip4a_interpro_go_mapping

Revision ID: 8c4d2a1f6e9b
Revises: 7b023568309d
Create Date: 2026-05-12 00:00:00.000000

IP.4a of the executor plan (F-IP annotation ensemble). Adds the
``interpro_go_mapping`` table that persists the EBI InterPro2GO flat
file (one row per ``ipr_accession`` to ``go_id`` mapping). The natural
key is (``ipr_accession``, ``go_id``, ``source_version``) so multiple
InterPro releases can coexist; the surrogate UUID PK keeps joins cheap.

Strictly additive. Safe to apply on a live DB: a fresh table plus its
indexes is metadata-only on Postgres (no rewrite of existing tables,
no row scan).

SQL preview (for reviewer):
    CREATE TABLE interpro_go_mapping (
        id UUID NOT NULL,
        ipr_accession VARCHAR(32) NOT NULL,
        go_id VARCHAR(16) NOT NULL,
        evidence TEXT,
        source_version TEXT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE
            DEFAULT now() NOT NULL,
        PRIMARY KEY (id),
        CONSTRAINT uq_interpro_go_mapping_pair
            UNIQUE (ipr_accession, go_id, source_version)
    );
    CREATE INDEX idx_interpro_go_mapping_ipr
        ON interpro_go_mapping (ipr_accession);
    CREATE INDEX idx_interpro_go_mapping_go
        ON interpro_go_mapping (go_id);
    CREATE INDEX idx_interpro_go_mapping_version
        ON interpro_go_mapping (source_version);
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "8c4d2a1f6e9b"
down_revision: str | Sequence[str] | None = "7b023568309d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "interpro_go_mapping",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("ipr_accession", sa.String(length=32), nullable=False),
        sa.Column("go_id", sa.String(length=16), nullable=False),
        sa.Column("evidence", sa.Text(), nullable=True),
        sa.Column("source_version", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "ipr_accession",
            "go_id",
            "source_version",
            name="uq_interpro_go_mapping_pair",
        ),
    )
    op.create_index(
        "idx_interpro_go_mapping_ipr",
        "interpro_go_mapping",
        ["ipr_accession"],
    )
    op.create_index(
        "idx_interpro_go_mapping_go",
        "interpro_go_mapping",
        ["go_id"],
    )
    op.create_index(
        "idx_interpro_go_mapping_version",
        "interpro_go_mapping",
        ["source_version"],
    )


def downgrade() -> None:
    op.drop_index("idx_interpro_go_mapping_version", table_name="interpro_go_mapping")
    op.drop_index("idx_interpro_go_mapping_go", table_name="interpro_go_mapping")
    op.drop_index("idx_interpro_go_mapping_ipr", table_name="interpro_go_mapping")
    op.drop_table("interpro_go_mapping")
