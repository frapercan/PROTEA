"""An ingested set names its producer and the bytes it published under keys of their own

An ingested set kept its producer's record (name, commit, shas, models) under
``meta["method"]``, the key a native method's set uses for its short name, and
the method seal read that key as a label. For an ingested set it is the method
itself. Since this revision ``ingest_method_predictions`` writes the record under
``meta["producer"]`` and the digest of the file under
``meta["predictions_sha256"]``, both identity in the seal, beside the revisions
and the population every set records.

This moves the record of the sets ingested before, and copies the digest they
kept in ``meta["source"]["sha256"]`` to its key. Their code and library
revisions were never recorded, so the seal still refuses to compare them until
they are ingested again; nothing here pretends otherwise.

The downgrade moves the record back and drops the digest's key.

Revision ID: 3b9d4e2a7f15
Revises: 7c5e1f0a9d42
Create Date: 2026-11-22 01:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "3b9d4e2a7f15"
down_revision: str | Sequence[str] | None = "7c5e1f0a9d42"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

NAME_THE_PRODUCER = """
UPDATE prediction_set
SET meta = (meta - 'method')
    || jsonb_build_object('producer', meta -> 'method')
    || CASE WHEN meta -> 'source' ? 'sha256'
            THEN jsonb_build_object('predictions_sha256', meta -> 'source' -> 'sha256')
            ELSE '{}'::jsonb END
WHERE meta ->> 'algorithm' = 'ingested_method' AND meta ? 'method'
"""

UNNAME_THE_PRODUCER = """
UPDATE prediction_set
SET meta = (meta - 'producer' - 'predictions_sha256')
    || jsonb_build_object('method', meta -> 'producer')
WHERE meta ->> 'algorithm' = 'ingested_method' AND meta ? 'producer'
"""


def upgrade() -> None:
    op.execute(sa.text(NAME_THE_PRODUCER))


def downgrade() -> None:
    op.execute(sa.text(UNNAME_THE_PRODUCER))
