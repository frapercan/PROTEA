"""Retire the rung-1 seeds and seed the stage-1 standard embedding configs

ADR-D48. The eight configs seeded by ``e7a1c4f9b2d6``, ``a1c9e4b7d2f8`` and
``f2b8d1c6a94e`` are replaced by the eight PLMs of ADR-D35 on one standard
recipe. Nothing was ever computed against the retired rows: on 2026-10-05 the
five foreign keys that point at ``embedding_config`` (``sequence_embedding``,
``prediction_set``, ``embedding_config.derived_from_embedding_config_id``,
``reranker_model``, ``dataset``) held no row referencing any of them.

Why the rung-1 rows go
----------------------

* The roster was chosen to fit an 8 GB card (RTX 4060). ESM-2 3B and ESM-2 150M
  were excluded for that reason, and the compute node now has 12 GB.
* Two of the eight were never comparable members of an axis. ``esm2_8m`` was
  seeded as a mechanism cell, a pipeline smoke test. ``protst`` is a 512-d
  text-aligned projection whose backend honours only ``normalize``, so its
  recorded ``max_length=2048`` described nothing it did.
* ``max_length=2048`` ran ESM-2 and ESM-C past the roughly 1024-token context
  they were trained on.
* The roster contradicted ADR-D35, which names the eight PLMs of the campaign.

What is seeded
--------------

The ADR-D35 roster (esm2 150M/650M/3B, prot_t5, prostt5, ankh base/large,
esmc_600m) on one recipe: last layer, mean pooling, pooled L2, no residue
normalisation, ``max_length=1022``, no chunking. ``max_length`` counts tokens
including special tokens on the esm, t5 and ankh backends and residues on
esm3c, so the residues seen are 1020 (ESM-2, ProstT5), 1021 (ProtT5, Ankh) and
1022 (ESM-C). ``param_count`` is left NULL on purpose:
``count_backend_parameters`` measures the module each backend actually
executes, which for ProtT5 is not the published figure.

Why retire and seed in ONE migration
------------------------------------

An empty ``embedding_config`` table is not a neutral state. When it is empty,
``annotate._resolve_dispatch_resources`` mints a default ESM-2 650M config for
the next anonymous ``/annotate`` call. Deleting in one revision and seeding in
the next would open that window between them. Here the DELETE and the INSERT
share the migration's transaction, so no reader ever sees the table empty.

The guard
---------

A retired row is deleted only if nothing references it. A dependent row turns
the upgrade into a refusal, because a DELETE that cascades (``SET NULL``) or
fails (``RESTRICT``) is not a replacement any more but a reconciliation, which
needs a decision rather than a migration.

Downgrade
---------

Restores the eight rung-1 rows exactly as they stood in the live registry on
2026-10-05, ``param_count`` and ``created_at`` included, after the same guard
on the stage-1 rows.

Revision ID: 3cd5f76d282f
Revises: f3a05d81c6e2
Create Date: 2026-10-05 00:00:00.000000

"""
import json
import uuid
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = '3cd5f76d282f'
down_revision: str | Sequence[str] | None = 'f3a05d81c6e2'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# The identity namespace of protea.core.embedding_identity, restated rather than
# imported: a migration must keep producing the same rows after the application
# code moves on. tests/test_stage1_standard_configs.py pins that the two agree.
_NS = uuid.uuid5(uuid.NAMESPACE_URL, "https://protea/embedding-config")

_RECIPE_BASE = {
    "layer_indices": [0], "layer_agg": "mean", "pooling": "mean",
    "normalize_residues": False, "normalize": True, "embedding_scale": 1.0,
    "use_chunking": False, "chunk_size": 512, "chunk_overlap": 0,
}
STAGE1 = dict(_RECIPE_BASE, max_length=1022)
RUNG1 = dict(_RECIPE_BASE, max_length=2048)
_IDENTITY_COLS = ("model_name", "model_backend", *sorted(STAGE1))

_STAGE1_NOTE = (
    "ADR-D48 stage-1 standard recipe: last layer, mean pool, L2-normalised, "
    "max_length 1022 (tokens incl. specials; esm3c counts residues)"
)

# (pinned_id, key, model_name, model_backend, family)
NEW = (
    ("336a2bcb-d35e-50c5-ba7e-9ae35f1a1dcb", "esm2_150m",
     "facebook/esm2_t30_150M_UR50D", "esm", "esm2"),
    ("9c4ea46b-c8c9-5cdb-af36-14df75605dd2", "esm2_650m",
     "facebook/esm2_t33_650M_UR50D", "esm", "esm2"),
    ("6fef8557-ca97-55c4-b75a-e9fca7d8dd93", "esm2_3b",
     "facebook/esm2_t36_3B_UR50D", "esm", "esm2"),
    ("38e079cd-0613-5ea7-b6dc-f70f0c419731", "esmc_600m",
     "esmc_600m", "esm3c", "esmc"),
    ("7d409774-ec37-5606-a90c-95646ce813ac", "prot_t5",
     "Rostlab/prot_t5_xl_half_uniref50-enc", "t5", "prot_t5"),
    ("cd00a743-8215-5ce9-8ad3-2846d9f590db", "prostt5",
     "Rostlab/ProstT5", "t5", "prostt5"),
    ("e7ba37da-c18d-5261-b5b4-49a3093d9d4d", "ankh_base",
     "ElnaggarLab/ankh-base", "ankh", "ankh"),
    ("2b27f22a-ed4c-50a8-9068-ec18be8d4a46", "ankh_large",
     "ElnaggarLab/ankh-large", "ankh", "ankh"),
)

# The rung-1 rows exactly as the live registry held them on 2026-10-05.
# (id, key, model_name, model_backend, family, param_count, description)
_RUNG1_CREATED_AT = "2026-09-14 21:47:15.650152+00"
_GRID = "rung1 matched grid: mean pool, last layer, max_length 2048, L2-normalised"
_RECIPE = "rung1 matched recipe: mean pool, last layer, max_length 2048, L2"
OLD = (
    ("0868f1ff-907a-5e4a-9d73-c0f2ed3c2437", "ankh_base",
     "ElnaggarLab/ankh-base", "ankh", "ankh", 453170688, f"ankh_base | {_GRID}"),
    ("9f52de2b-ec6e-5e48-b440-e506b50d62bb", "ankh_large",
     "ElnaggarLab/ankh-large", "ankh", "ankh", 1151707648, f"ankh_large | {_GRID}"),
    ("f64daa67-d540-5b1f-80a9-4673e0c31ed9", "esmc_600m",
     "esmc_600m", "esm3c", "esmc", 575036992, f"esmc_600m | {_GRID}"),
    ("ab430e07-5586-5bdc-9b7e-cc2a3ca18781", "esm2_650m",
     "facebook/esm2_t33_650M_UR50D", "esm", "esm2", 652353941, f"esm2_650m | {_GRID}"),
    ("b7b0d26a-f083-5dc0-afd5-b704b29e14e9", "esm2_8m",
     "facebook/esm2_t6_8M_UR50D", "esm", "esm2", 7841868,
     "esm2_8m | rung0 mechanism cell: mean pool, last layer, max_length 2048, "
     "L2-normalised (same matched recipe as the rung-1 grid)"),
    ("4d5d29ee-5a8c-53d2-bdc2-080187971454", "protst",
     "mila-intel/ProtST-esm1b", "protst", "protst", None,
     "protst | NOT on the matched recipe: 512-d text-aligned whole-protein "
     "projection, honours only normalize"),
    ("d8d26a5e-ba2b-532f-ad81-93b67785e7be", "prostt5",
     "Rostlab/ProstT5", "t5", "prostt5", None, f"prostt5 | {_RECIPE}"),
    ("9987ca96-df70-598d-b610-a738d23dad13", "prot_t5",
     "Rostlab/prot_t5_xl_half_uniref50-enc", "t5", "prot_t5", None, f"prot_t5 | {_RECIPE}"),
)

# Every foreign key that points at embedding_config.id, as (table, column).
_DEPENDENTS = (
    ("sequence_embedding", "embedding_config_id"),
    ("prediction_set", "embedding_config_id"),
    ("reranker_model", "embedding_config_id"),
    ("dataset", "embedding_config_id"),
    ("embedding_config", "derived_from_embedding_config_id"),
)


def _recipe(base: dict, model_name: str, model_backend: str) -> dict:
    recipe = dict(base, model_name=model_name, model_backend=model_backend)
    if len(recipe) != len(_IDENTITY_COLS):
        raise RuntimeError(f"recipe has {len(recipe)} fields, expected {len(_IDENTITY_COLS)}")
    return recipe


def _derive_id(base: dict, model_name: str, model_backend: str) -> str:
    return str(uuid.uuid5(_NS, json.dumps(
        _recipe(base, model_name, model_backend), sort_keys=True, separators=(",", ":"))))


def _refuse_if_referenced(bind, config_id: str, key: str) -> None:
    for table, column in _DEPENDENTS:
        n = bind.execute(sa.text(
            f"SELECT count(*) FROM {table}"  # noqa: S608 - fixed literals
            f" WHERE {column} = CAST(:id AS uuid)"
        ), {"id": config_id}).scalar_one()
        if n:
            raise RuntimeError(
                f"refusing to retire {key} ({config_id}): {n} {table}.{column} "
                "rows reference it. This is a reconciliation, not a replacement."
            )


def _verify_identity(bind, config_id: str, key: str, expected: dict) -> None:
    row = bind.execute(sa.text(
        "SELECT model_name, model_backend, layer_indices, layer_agg, pooling,"
        " normalize_residues, normalize, embedding_scale, max_length,"
        " use_chunking, chunk_size, chunk_overlap"
        " FROM embedding_config WHERE id = CAST(:id AS uuid)"
    ), {"id": config_id}).mappings().one_or_none()
    if row is None:
        raise RuntimeError(f"{key} ({config_id}) missing")
    for col in _IDENTITY_COLS:
        got = list(row[col]) if col == "layer_indices" else row[col]
        if got != expected[col]:
            raise RuntimeError(
                f"{key} ({config_id}) diverges on {col}: db={got!r} "
                f"expected={expected[col]!r}. This id denotes a different configuration."
            )


def _insert(bind, *, config_id, key, model_name, backend, family, base,
            description, param_count=None, created_at=None) -> None:
    bind.execute(sa.text("""
        INSERT INTO embedding_config (
            id, model_name, model_backend, layer_indices, layer_agg, pooling,
            normalize_residues, normalize, embedding_scale, max_length,
            use_chunking, chunk_size, chunk_overlap,
            description, display_name, family, kind, param_count, created_at
        ) VALUES (
            CAST(:id AS uuid), :model_name, :model_backend,
            CAST(:layer_indices AS jsonb), :layer_agg, :pooling,
            :normalize_residues, :normalize, :embedding_scale, :max_length,
            :use_chunking, :chunk_size, :chunk_overlap,
            :description, :display_name, :family, 'pretrained', :param_count,
            COALESCE(CAST(:created_at AS timestamptz), now())
        ) ON CONFLICT (id) DO NOTHING
    """), {
        "id": config_id, "model_name": model_name, "model_backend": backend,
        "layer_indices": json.dumps(base["layer_indices"]),
        "layer_agg": base["layer_agg"], "pooling": base["pooling"],
        "normalize_residues": base["normalize_residues"],
        "normalize": base["normalize"], "embedding_scale": base["embedding_scale"],
        "max_length": base["max_length"], "use_chunking": base["use_chunking"],
        "chunk_size": base["chunk_size"], "chunk_overlap": base["chunk_overlap"],
        "description": description, "display_name": key, "family": family,
        "param_count": param_count, "created_at": created_at,
    })
    _verify_identity(bind, config_id, key, _recipe(base, model_name, backend))


def _check_pins(rows, base) -> None:
    for pinned, key, model_name, backend, *_ in rows:
        derived = _derive_id(base, model_name, backend)
        if derived != pinned:
            raise RuntimeError(
                f"{key}: derived id {derived} != pinned {pinned}. The recipe "
                "changed. Update the pinned value deliberately, or revert the "
                "recipe; do NOT silently seed a relabelled experiment."
            )


def upgrade() -> None:
    """Retire the rung-1 rows and seed the stage-1 rows, in one transaction."""
    bind = op.get_bind()
    _check_pins(OLD, RUNG1)
    _check_pins(NEW, STAGE1)

    for old_id, key, model_name, backend, *_ in OLD:
        present = bind.execute(sa.text(
            "SELECT 1 FROM embedding_config WHERE id = CAST(:id AS uuid)"
        ), {"id": old_id}).first()
        if present is None:
            continue
        _verify_identity(bind, old_id, key, _recipe(RUNG1, model_name, backend))
        _refuse_if_referenced(bind, old_id, key)
        bind.execute(sa.text(
            "DELETE FROM embedding_config WHERE id = CAST(:id AS uuid)"
        ), {"id": old_id})

    for new_id, key, model_name, backend, family in NEW:
        _insert(bind, config_id=new_id, key=key, model_name=model_name,
                backend=backend, family=family, base=STAGE1,
                description=f"{key} | {_STAGE1_NOTE}")


def downgrade() -> None:
    """Retire the stage-1 rows and restore the rung-1 rows as they stood."""
    bind = op.get_bind()
    _check_pins(OLD, RUNG1)
    _check_pins(NEW, STAGE1)

    for new_id, key, *_ in NEW:
        _refuse_if_referenced(bind, new_id, key)
        bind.execute(sa.text(
            "DELETE FROM embedding_config WHERE id = CAST(:id AS uuid)"
        ), {"id": new_id})

    for old_id, key, model_name, backend, family, param_count, description in OLD:
        _insert(bind, config_id=old_id, key=key, model_name=model_name,
                backend=backend, family=family, base=RUNG1,
                description=description, param_count=param_count,
                created_at=_RUNG1_CREATED_AT)
