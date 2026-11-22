"""Current UniProt metadata names the release that served it, and carries its descriptive fields

``protein_uniprot_metadata`` holds an entry as UniProt published it, which is
today's knowledge of the protein. A row that does not say which release it
came from cannot be shown honestly, nor told apart from what was known at an
earlier cut. This adds the release and its date, null for the rows written
before (their release is unknown, not current), and the entry's descriptive
fields for display: entry name, reviewed, protein and gene names, organism and
length.

The downgrade drops the columns.

Revision ID: 5d2e8c1b4a73
Revises: 3b9d4e2a7f15
Create Date: 2026-11-22 14:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5d2e8c1b4a73"
down_revision: str | Sequence[str] | None = "3b9d4e2a7f15"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

COLUMNS: tuple[tuple[str, sa.types.TypeEngine], ...] = (
    ("uniprot_release", sa.String()),
    ("uniprot_release_date", sa.String()),
    ("entry_name", sa.Text()),
    ("reviewed", sa.Boolean()),
    ("protein_names", sa.Text()),
    ("gene_names", sa.Text()),
    ("organism_name", sa.Text()),
    ("organism_id", sa.Integer()),
    ("length", sa.Integer()),
)


def upgrade() -> None:
    for name, kind in COLUMNS:
        op.add_column("protein_uniprot_metadata", sa.Column(name, kind, nullable=True))


def downgrade() -> None:
    for name, _kind in reversed(COLUMNS):
        op.drop_column("protein_uniprot_metadata", name)
