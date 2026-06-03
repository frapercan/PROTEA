"""ip2_interpro_annotation

Revision ID: 7b023568309d
Revises: d7e4c2b9a1f0, 2a5e9b3f1c4d
Create Date: 2026-05-12 00:00:00.000000

IP.2 of the executor plan (F-IP). Adds the ``interpro_annotation``
table that persists one InterProScan domain hit per row. Mirrors the
IP.1a parser record (``protea_sources.interpro.parser.InterProAnnotation``)
plus a surrogate UUID primary key and the ``ipr_release`` tag so
multiple InterProScan runs at different release versions can coexist
for the same protein.

Acts as a merge migration: the two heads at branch time
(``d7e4c2b9a1f0`` t3.1a_goprediction_features_jsonb and
``2a5e9b3f1c4d`` add api_key table) are reunified under this revision
so subsequent migrations have a single head to chain from.

Strictly additive. Safe to apply on a live DB without a maintenance
window: a fresh table plus its indexes is metadata-only on Postgres
(no rewrite of existing tables, no row scan).

SQL preview (for reviewer):
    CREATE TABLE interpro_annotation (
        id UUID NOT NULL,
        protein_accession VARCHAR NOT NULL,
        source_db TEXT NOT NULL,
        accession TEXT NOT NULL,
        start INTEGER NOT NULL,
        "end" INTEGER NOT NULL,
        evidence TEXT,
        ipr_release TEXT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE
            DEFAULT now() NOT NULL,
        PRIMARY KEY (id),
        CONSTRAINT ck_interpro_annotation_start_positive CHECK ("start" >= 1),
        CONSTRAINT ck_interpro_annotation_end_ge_start CHECK ("end" >= "start"),
        FOREIGN KEY(protein_accession) REFERENCES protein (accession)
            ON DELETE CASCADE
    );
    CREATE INDEX idx_interpro_annotation_protein_id
        ON interpro_annotation (protein_accession);
    CREATE INDEX idx_interpro_annotation_accession
        ON interpro_annotation (accession);
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7b023568309d"
down_revision: str | Sequence[str] | None = ("d7e4c2b9a1f0", "2a5e9b3f1c4d")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "interpro_annotation",
        sa.Column("id", UUID(as_uuid=True), nullable=False),
        sa.Column("protein_accession", sa.String(), nullable=False),
        sa.Column("source_db", sa.Text(), nullable=False),
        sa.Column("accession", sa.Text(), nullable=False),
        sa.Column("start", sa.Integer(), nullable=False),
        sa.Column("end", sa.Integer(), nullable=False),
        sa.Column("evidence", sa.Text(), nullable=True),
        sa.Column("ipr_release", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["protein_accession"],
            ["protein.accession"],
            ondelete="CASCADE",
        ),
        sa.CheckConstraint('"start" >= 1', name="ck_interpro_annotation_start_positive"),
        sa.CheckConstraint('"end" >= "start"', name="ck_interpro_annotation_end_ge_start"),
    )
    op.create_index(
        "idx_interpro_annotation_protein_id",
        "interpro_annotation",
        ["protein_accession"],
    )
    op.create_index(
        "idx_interpro_annotation_accession",
        "interpro_annotation",
        ["accession"],
    )


def downgrade() -> None:
    op.drop_index("idx_interpro_annotation_accession", table_name="interpro_annotation")
    op.drop_index("idx_interpro_annotation_protein_id", table_name="interpro_annotation")
    op.drop_table("interpro_annotation")
