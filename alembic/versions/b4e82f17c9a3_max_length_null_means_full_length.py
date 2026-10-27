"""max_length NULL means full length, no truncation

Revision ID: b4e82f17c9a3
Revises: a7c3e91b4d26
Create Date: 2026-10-27 01:40:00.000000

``embedding_config.max_length`` could not say "do not truncate". It was
``integer NOT NULL`` defaulting to 1022, so every configuration asserted a
truncation point, and a configuration computed over whole sequences had no
honest way to describe itself.

Why not a sentinel integer
--------------------------

Because any integer is a claim. Writing the longest observed sequence --
35.213 in the delivered ankh_base full-length package, 35.991 in this store --
would make the configuration a function of the corpus: a longer protein
arriving tomorrow would silently redefine what that configuration meant, and
``max_length`` is an IDENTITY field (``protea/core/embedding_identity.py:44``),
so the id would then have been minted for a recipe nobody wrote. Zero is worse:
it reads as "truncate to nothing".

NULL says the one thing that is true and stays true: no truncation was applied.

What this does NOT move
-----------------------

No existing row is NULL, so no existing recipe changes and no existing id is
reissued. The identity namespace is untouched. The only configurations this
affects are ones created after it.

What had to change with it
--------------------------

Three call sites assumed an integer, and a nullable column without them would
be a crash or a silent truncation rather than a feature:

* ``_compute_embeddings_helpers.py`` costed a pass with
  ``min(n, config.max_length)``, which raises ``TypeError`` against None.
  With no truncation, residues processed equals residues available, which is
  what the chunking branch already says.
* the ESM and T5/Ankh tokenisers were called with ``truncation=True`` and
  ``max_length=config.max_length``. Hugging Face reads ``max_length=None``
  under ``truncation=True`` as "use the model's own ``model_max_length``",
  which for T5 is a sentinel in the quintillions and for others is 512. Either
  way the truncation point would come from the checkpoint instead of from the
  configuration, silently. Truncation is now requested only when the
  configuration names a length.
* the service-layer validator required a positive integer, so the API could
  not express the configuration the column now permits.
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "b4e82f17c9a3"
down_revision = "a7c3e91b4d26"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "embedding_config",
        "max_length",
        existing_type=sa.Integer(),
        nullable=True,
        existing_server_default=None,
    )


def downgrade() -> None:
    # A NULL cannot be represented once the column is NOT NULL again, and
    # guessing a number for it would invent a truncation point that was never
    # applied. Refuse instead of corrupting the recipe.
    bind = op.get_bind()
    nulls = bind.execute(
        sa.text("SELECT count(*) FROM embedding_config WHERE max_length IS NULL")
    ).scalar_one()
    if nulls:
        raise RuntimeError(
            f"{nulls} embedding_config row(s) have max_length NULL, meaning "
            "full length. Downgrading would have to invent a truncation point "
            "that was never applied. Delete or re-describe those configs first."
        )
    op.alter_column(
        "embedding_config",
        "max_length",
        existing_type=sa.Integer(),
        nullable=False,
        existing_server_default=None,
    )
