"""swiss-prot membership per archived release, with the sequence md5

Revision ID: a7c3e91b4d26
Revises: f2a8c41d9e37
Create Date: 2026-10-27 09:10:00.000000

Two tables so that a temporal window can ask "was this entry reviewed THEN",
which nothing in the schema could answer.

Why a table and not a derivation
--------------------------------

``protein.reviewed`` is one boolean meaning "reviewed as of 2026-09-15". Used
inside a window it repeats the defect it was created by -- describing the
present over data from the past -- and the error is not small in the direction
that matters: 4.3% of today's Swiss-Prot was not in Swiss-Prot in 2016, against
0.1% in the other direction.

The cheap route exists but not here.
``protea.core.operations._universe_sources.is_swissprot_entry`` reads
contemporaneous membership out of the GAF's DB Object Synonym column, which
carries the entry name of its own release. Measured 2026-10-06 over the cached
series: releases 164..178 carry an entry name on 100% of rows, 179 and 180 on
0%, and 231 on 5%. GOA replaced the entry name with the gene symbol and it is
in no other column. The campaign's evaluation window is GOA 227 to 230, both
inside that dead stretch, so there the GAF cannot be asked and UniProt's
archived release is the only source.

Why the sequence md5 is in the key table rather than alongside
--------------------------------------------------------------

LAFA's targets are the accessions in Swiss-Prot at both ends of the window AND
carrying the same sequence at both. The two conditions differ: of the 573,640
accessions in both 2025_03 and 2026_01, 180 changed sequence and 14 of those
are targets, which is the whole difference between 7,415 targets and LAFA's
7,401. A membership table without the md5 can express the first condition and
not the second, so it would reproduce the wrong population while looking
correct.

``(release_id, accession)`` is the primary key rather than a surrogate: it is
the natural key, it provides the uniqueness, and it is the index the
cross-release join scans. At about 574k rows per release the md5 heap fetch on
that join costs nothing.

``content_sha256`` is the digest of the canonical table the release resolves to
-- one ``"<accession>\\t<md5>\\n"`` line per entry, sorted by accession -- and
not of the file that was read, so a streamed download from UniProt and a
pre-extracted table are interchangeable and check each other.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "a7c3e91b4d26"
down_revision = "f2a8c41d9e37"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "swissprot_release",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("release", sa.String(length=16), nullable=False),
        sa.Column("published_at", sa.Date(), nullable=False),
        sa.Column("source_url", sa.String(length=512), nullable=False),
        sa.Column("accession_count", sa.Integer(), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("meta", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["job.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("release", name="uq_swissprot_release_release"),
    )
    op.create_index(
        "ix_swissprot_release_published_at", "swissprot_release", ["published_at"]
    )
    op.create_index("ix_swissprot_release_job_id", "swissprot_release", ["job_id"])

    op.create_table(
        "swissprot_membership",
        sa.Column("release_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("accession", sa.String(length=32), nullable=False),
        sa.Column("sequence_md5", sa.String(length=32), nullable=False),
        sa.ForeignKeyConstraint(
            ["release_id"], ["swissprot_release.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("release_id", "accession"),
    )


def downgrade() -> None:
    op.drop_table("swissprot_membership")
    op.drop_index("ix_swissprot_release_job_id", table_name="swissprot_release")
    op.drop_index("ix_swissprot_release_published_at", table_name="swissprot_release")
    op.drop_table("swissprot_release")
