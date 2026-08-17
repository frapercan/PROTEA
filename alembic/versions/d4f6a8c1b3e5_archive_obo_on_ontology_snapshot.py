"""Add obo_uri and obo_sha256 to ontology_snapshot

ADR-D47. ``load_ontology_snapshot`` parses the OBO in memory and persists only
``obo_url``, so ``run_cafa_evaluation`` refetches the file from the upstream
third party on every run. The term universe, the True Path Rule edges, the
propagation, the Information Accretion table and every metric downstream of them
rest on bytes the project does not hold.

Both columns are nullable: the snapshots loaded before this revision have no
archive, and ``run_cafa_evaluation`` keeps the upstream fallback (emitting a
warning) so nothing that worked stops working. The
``archive_ontology_snapshot`` operation backfills them, gating the fetched bytes
against the term set already in the database rather than trusting that the URL
still serves what it served at load time.

Revision ID: d4f6a8c1b3e5
Revises: c3d5b7e9a1f2
Create Date: 2026-07-30 00:00:00.000000

"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'd4f6a8c1b3e5'
down_revision: str | Sequence[str] | None = 'c3d5b7e9a1f2'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        'ontology_snapshot',
        sa.Column(
            'obo_uri',
            sa.String(length=512),
            nullable=True,
            comment=(
                "Artifact-store URI of the archived raw OBO (gzipped). NULL "
                "means the ontology behind this snapshot exists only as an "
                "upstream URL."
            ),
        ),
    )
    op.add_column(
        'ontology_snapshot',
        sa.Column(
            'obo_sha256',
            sa.String(length=64),
            nullable=True,
            comment="sha256 of the UNCOMPRESSED OBO bytes archived at obo_uri.",
        ),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('ontology_snapshot', 'obo_sha256')
    op.drop_column('ontology_snapshot', 'obo_uri')
