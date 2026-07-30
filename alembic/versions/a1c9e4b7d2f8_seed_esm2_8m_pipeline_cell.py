"""Seed the smallest ESM-2 as a rung-0 pipeline cell

The rung-1 grid (``e7a1c4f9b2d6``) is four cells between 453M and 1.15B
parameters. A full pass over the corpus with any of them costs the better part
of a day per cell, which means the first end-to-end proof that load, embed,
KNN, IA and evaluation compose correctly arrives a day late and is expensive to
repeat when it fails.

``facebook/esm2_t6_8M_UR50D`` is the smallest published ESM-2: 6 layers, hidden
320. It is roughly 83x smaller than esm2_650m by parameter count, so the same
528k-sequence corpus is a short run rather than a long one. It is a MECHANISM
cell, not a science cell: its job is to exercise the whole pipeline at full
corpus scale so the grid runs against a path already known to work.

It sits on the SAME matched recipe as rung-1 (mean pooling, last layer,
max_length 2048, L2-normalised), so it is directly comparable to the grid and
extends it downward on the parameter axis rather than forking it.

``param_count`` is the exact sum of ``numel()`` over the 114 tensors in
``model.safetensors``, following the convention the esmc entry established:

    esm.encoder        7,398,448
    esm.embeddings       339,906
    lm_head              103,393
    esm.contact_head         121
    TOTAL              7,841,868

Note that ``AutoModel.from_pretrained`` reports 7,840,121 over 104 tensors
instead: it drops ``lm_head`` and ``contact_head`` and adds a freshly
initialised 102,720-parameter pooler that is absent from the checkpoint and
unused by the ``esm`` backend. The checkpoint is the artifact; the loader's
instantiation is not.

Revision ID: a1c9e4b7d2f8
Revises: d4f6a8c1b3e5
Create Date: 2026-07-30 00:00:00.000000

"""
import json
import uuid
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'a1c9e4b7d2f8'
down_revision: str | Sequence[str] | None = 'd4f6a8c1b3e5'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Byte-identical to the rung-1 STD. Duplicated rather than imported because a
# migration must keep meaning even if the module it came from later changes.
STD = {
    "layer_indices": [0],
    "layer_agg": "mean",
    "pooling": "mean",
    "normalize_residues": False,
    "normalize": True,
    "embedding_scale": 1.0,
    "max_length": 2048,
    "use_chunking": False,
    "chunk_size": 512,
    "chunk_overlap": 0,
}

_NS = uuid.uuid5(uuid.NAMESPACE_URL, "https://protea/embedding-config")
_MODEL_COLS = ("model_name", "model_backend")
_IDENTITY_COLS = (*_MODEL_COLS, *sorted(STD))

KEY = "esm2_8m"
MODEL_NAME = "facebook/esm2_t6_8M_UR50D"
BACKEND = "esm"
FAMILY = "esm2"
PARAM_COUNT = 7_841_868
DIM = 320

# Tripwire, not the source of truth: upgrade() derives the id and aborts on a
# mismatch. Verified to reproduce ab430e07-...-cc2a3ca18781 for esm2_650m under
# the same function, which is what proves this derivation is the rung-1 one.
EXPECTED_ID = "b7b0d26a-f083-5dc0-afd5-b704b29e14e9"

_DESC = (
    "esm2_8m | rung0 mechanism cell: mean pool, last layer, max_length 2048, "
    "L2-normalised (same matched recipe as the rung-1 grid)"
)

_DEPENDENTS = ("sequence_embedding", "prediction_set", "dataset", "reranker_model")


def _recipe(model_name: str, model_backend: str) -> dict:
    recipe = dict(STD, model_name=model_name, model_backend=model_backend)
    if len(recipe) != len(_IDENTITY_COLS):
        raise RuntimeError(
            f"recipe has {len(recipe)} fields, expected {len(_IDENTITY_COLS)}"
        )
    return recipe


def _derive_id(model_name: str, model_backend: str) -> str:
    payload = json.dumps(
        _recipe(model_name, model_backend), sort_keys=True, separators=(",", ":")
    )
    return str(uuid.uuid5(_NS, payload))


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()

    cid = _derive_id(MODEL_NAME, BACKEND)
    if cid != EXPECTED_ID:
        raise RuntimeError(
            f"{KEY}: derived id {cid} != pinned {EXPECTED_ID}. The recipe "
            "changed. These are content-derived ids -- update the pinned value "
            "deliberately, or revert the recipe; do NOT silently seed a "
            "relabelled experiment."
        )

    # The recipe must be byte-identical to the rung-1 cells on every field the
    # grid holds fixed, or this cell is not comparable to them and the whole
    # point of adding it is lost.
    peer = bind.execute(sa.text(
        "SELECT layer_indices, layer_agg, pooling, normalize_residues,"
        " normalize, embedding_scale, max_length, use_chunking, chunk_size,"
        " chunk_overlap FROM embedding_config"
        " WHERE id = CAST('ab430e07-5586-5bdc-9b7e-cc2a3ca18781' AS uuid)"
    )).mappings().one_or_none()
    if peer is not None:
        for col in sorted(STD):
            got = list(peer[col]) if col == "layer_indices" else peer[col]
            if got != STD[col]:
                raise RuntimeError(
                    f"{KEY} would not be comparable to the rung-1 grid: {col} "
                    f"is {got!r} on esm2_650m but {STD[col]!r} here"
                )

    bind.execute(sa.text("""
        INSERT INTO embedding_config (
            id, model_name, model_backend, layer_indices, layer_agg, pooling,
            normalize_residues, normalize, embedding_scale, max_length,
            use_chunking, chunk_size, chunk_overlap,
            description, display_name, family, param_count
        ) VALUES (
            CAST(:id AS uuid), :model_name, :model_backend,
            CAST(:layer_indices AS jsonb), :layer_agg, :pooling,
            :normalize_residues, :normalize, :embedding_scale, :max_length,
            :use_chunking, :chunk_size, :chunk_overlap,
            :description, :display_name, :family, :param_count
        )
        ON CONFLICT (id) DO NOTHING
    """), {
        "id": cid,
        "model_name": MODEL_NAME,
        "model_backend": BACKEND,
        "layer_indices": json.dumps(STD["layer_indices"]),
        "layer_agg": STD["layer_agg"],
        "pooling": STD["pooling"],
        "normalize_residues": STD["normalize_residues"],
        "normalize": STD["normalize"],
        "embedding_scale": STD["embedding_scale"],
        "max_length": STD["max_length"],
        "use_chunking": STD["use_chunking"],
        "chunk_size": STD["chunk_size"],
        "chunk_overlap": STD["chunk_overlap"],
        "description": _DESC,
        "display_name": KEY,
        "family": FAMILY,
        "param_count": PARAM_COUNT,
    })

    row = bind.execute(sa.text(
        "SELECT model_name, model_backend, layer_indices, layer_agg, pooling,"
        " normalize_residues, normalize, embedding_scale, max_length,"
        " use_chunking, chunk_size, chunk_overlap"
        " FROM embedding_config WHERE id = CAST(:id AS uuid)"
    ), {"id": cid}).mappings().one_or_none()
    if row is None:
        raise RuntimeError(f"{KEY} ({cid}) missing after insert")
    expected = _recipe(MODEL_NAME, BACKEND)
    for col in _IDENTITY_COLS:
        got = list(row[col]) if col == "layer_indices" else row[col]
        if got != expected[col]:
            raise RuntimeError(
                f"{KEY} ({cid}) diverges on {col}: db={got!r} "
                f"expected={expected[col]!r}. Refusing to proceed -- this id "
                "already denotes a different configuration."
            )


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    for table in _DEPENDENTS:
        n = bind.execute(sa.text(
            f"SELECT count(*) FROM {table}"  # noqa: S608 - fixed literal
            " WHERE embedding_config_id = CAST(:id AS uuid)"
        ), {"id": EXPECTED_ID}).scalar_one()
        if n:
            raise RuntimeError(
                f"refusing to drop {KEY} ({EXPECTED_ID}): {n} {table} rows "
                "depend on it"
            )
    bind.execute(sa.text(
        "DELETE FROM embedding_config WHERE id = CAST(:id AS uuid)"
    ), {"id": EXPECTED_ID})
