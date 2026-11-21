"""The annotation sets loaded before the loader described them say what they are

``load_cut_photo`` writes each annotation set's description since #1213
(2026-11-17). The sets it loaded before that carry none: on dev, the six GOA
cuts 160, 177, 194, 205, 211 and 227, loaded on 2026-11-16. This writes them the
words the loader writes now (``load_cut_photo.photo_set_description``), built
from the row and its ontology snapshot, and touches nothing else: a row that
already says something, or that another loader made, keeps what it has.

The downgrade leaves the words where they are. They were missing, not wrong,
and a set the loader wrote after #1213 carries the same text, so nothing could
tell which ones this filled.

Revision ID: e0399c98f515
Revises: 2ee1c12a5e0c
Create Date: 2026-11-21 18:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e0399c98f515"
down_revision: str | Sequence[str] | None = "2ee1c12a5e0c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The loader's words, in SQL: the cut, the day it was published and the GO
#: graph it was loaded on. A test holds this to ``photo_set_description``.
DESCRIBE = """
UPDATE annotation_set AS a
SET description = 'GOA ' || a.source_version
    || ', published ' || to_char(a.source_published_at AT TIME ZONE 'UTC', 'YYYY-MM-DD')
    || ', as the photo of cut ' || a.source_version
    || ' loaded it (ADR-D59): the rows of the photo''s members on the GO graph '
    || o.obo_version || '.'
FROM ontology_snapshot AS o
WHERE o.id = a.ontology_snapshot_id
  AND (a.description IS NULL OR btrim(a.description) = '')
  AND a.meta->>'created_by' = 'load_cut_photo'
  AND a.source_published_at IS NOT NULL
"""


def upgrade() -> None:
    op.execute(sa.text(DESCRIBE))


def downgrade() -> None:
    """Nothing to undo: the words were missing, not wrong (see the module docstring)."""
