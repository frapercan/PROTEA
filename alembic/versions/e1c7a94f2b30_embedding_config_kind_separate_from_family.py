"""embedding_config.kind, separate from family

``family`` was carrying two different kinds of statement at once. For the ten
pretrained configs it held a model lineage — ``ankh``, ``esm2``, ``esmc``,
``t5``, ``protst``. For the three learned ones it held an architecture —
``learned-code``, ``residue-sparse``. Reading a column that answers two
questions means never knowing which one a given row answered, and grouping by
it silently mixes lineages with architectures.

``kind`` takes the coarse split (pretrained vs learned) and leaves ``family``
to mean one thing: the lineage a model descends from.

The backfill also splits ``t5``. ProtT5 and ProstT5 shared it, which is a claim
about them being one lineage rather than a fact: different training objectives,
different corpora. If they are ever to be pooled, that pooling is a decision
someone makes in an analysis, not a default baked into a column.

Nothing here touches ``IDENTITY_FIELDS``, so no config id moves and no stored
embedding is orphaned.

Revision ID: e1c7a94f2b30
Revises: d4b8c2f10a37
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "e1c7a94f2b30"
down_revision = "d4b8c2f10a37"
branch_labels = None
depends_on = None

#: Old ``family`` value -> (new kind, new family). Keyed on what the column
#: actually holds today rather than on model names, so the backfill is a
#: statement about the data in front of it.
_SPLIT = {
    "ankh": ("pretrained", "ankh"),
    "esm2": ("pretrained", "esm2"),
    "esmc": ("pretrained", "esmc"),
    "protst": ("pretrained", "protst"),
    "learned-code": ("learned", "rung2"),
    "residue-sparse": ("learned", "rung2"),
}

#: ``t5`` split by model_name, because the column cannot tell the two apart.
_T5_BY_MODEL = {"Rostlab/ProstT5": "prostt5"}
_T5_DEFAULT = "prot_t5"


def upgrade() -> None:
    op.add_column("embedding_config", sa.Column("kind", sa.String(), nullable=True))

    for old, (kind, family) in _SPLIT.items():
        op.execute(
            sa.text(
                "UPDATE embedding_config SET kind = :k, family = :f WHERE family = :old"
            ).bindparams(k=kind, f=family, old=old)
        )

    op.execute(
        sa.text(
            "UPDATE embedding_config SET kind = 'pretrained', family = :prostt5 "
            "WHERE family = 't5' AND model_name = :model"
        ).bindparams(prostt5=_T5_BY_MODEL["Rostlab/ProstT5"], model="Rostlab/ProstT5")
    )
    op.execute(
        sa.text(
            "UPDATE embedding_config SET kind = 'pretrained', family = :other WHERE family = 't5'"
        ).bindparams(other=_T5_DEFAULT)
    )

    # A row whose family was never set cannot be classified by this migration,
    # and guessing would put a config in a bar it may not belong to.
    op.execute(sa.text("UPDATE embedding_config SET kind = 'unclassified' WHERE kind IS NULL"))
    op.alter_column("embedding_config", "kind", nullable=False)


def downgrade() -> None:
    """Restore the merged column.

    ``family`` is put back to the shape the old readers expect: the learned
    configs return to their architecture strings and the two t5 lineages
    re-merge. That re-merge is lossy on purpose — it is the defect this
    migration removed, and reproducing it is what downgrading means.
    """
    op.execute(
        sa.text(
            "UPDATE embedding_config SET family = 'learned-code' "
            "WHERE kind = 'learned' AND pooling <> 'residue-sparse-mean'"
        )
    )
    op.execute(
        sa.text(
            "UPDATE embedding_config SET family = 'residue-sparse' "
            "WHERE kind = 'learned' AND pooling = 'residue-sparse-mean'"
        )
    )
    op.execute(
        sa.text("UPDATE embedding_config SET family = 't5' WHERE family IN ('prot_t5', 'prostt5')")
    )
    op.drop_column("embedding_config", "kind")
