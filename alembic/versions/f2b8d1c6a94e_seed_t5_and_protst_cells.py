"""Seed the ProstT5, ProtT5 and ProtST cells

Extends the rung-1 grid with the three families it was missing. ProstT5 and
ProtT5 ride the same matched recipe as the rest and are directly comparable.

ProtST is NOT. Its backend returns the whole-protein ``protein_feature``
projection (512-d, text-aligned), one full-sequence chunk per sequence, and
honours only ``normalize``: pooling, layer selection, max_length and chunking do
not apply. The recipe fields are still recorded so its id derives the same way
as every other cell, but a reader comparing it to the T5 and ESM cells is
comparing a different kind of representation, not a bigger one.

Revision ID: f2b8d1c6a94e
Revises: a1c9e4b7d2f8
Create Date: 2026-07-31 00:00:00.000000

"""
import json
import uuid
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = 'f2b8d1c6a94e'
down_revision: str | Sequence[str] | None = 'a1c9e4b7d2f8'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

STD = {
    "layer_indices": [0], "layer_agg": "mean", "pooling": "mean",
    "normalize_residues": False, "normalize": True, "embedding_scale": 1.0,
    "max_length": 2048, "use_chunking": False, "chunk_size": 512,
    "chunk_overlap": 0,
}
_NS = uuid.uuid5(uuid.NAMESPACE_URL, "https://protea/embedding-config")
_IDENTITY_COLS = ("model_name", "model_backend", *sorted(STD))

# (expected_id, key, model_name, backend, family, comparable_to_grid)
GRID = (
    ("d8d26a5e-ba2b-532f-ad81-93b67785e7be", "prostt5",
     "Rostlab/ProstT5", "t5", "t5", True),
    ("9987ca96-df70-598d-b610-a738d23dad13", "prot_t5",
     "Rostlab/prot_t5_xl_half_uniref50-enc", "t5", "t5", True),
    ("4d5d29ee-5a8c-53d2-bdc2-080187971454", "protst",
     "mila-intel/ProtST-esm1b", "protst", "protst", False),
)

_DEPENDENTS = ("sequence_embedding", "prediction_set", "dataset", "reranker_model")


def _recipe(model_name: str, model_backend: str) -> dict:
    recipe = dict(STD, model_name=model_name, model_backend=model_backend)
    if len(recipe) != len(_IDENTITY_COLS):
        raise RuntimeError(f"recipe has {len(recipe)} fields, expected {len(_IDENTITY_COLS)}")
    return recipe


def _derive_id(model_name: str, model_backend: str) -> str:
    return str(uuid.uuid5(_NS, json.dumps(
        _recipe(model_name, model_backend), sort_keys=True, separators=(",", ":"))))


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    for expected_id, key, model_name, backend, family, comparable in GRID:
        cid = _derive_id(model_name, backend)
        if cid != expected_id:
            raise RuntimeError(
                f"{key}: derived id {cid} != pinned {expected_id}. The recipe "
                "changed. Update the pinned value deliberately, or revert the "
                "recipe; do NOT silently seed a relabelled experiment."
            )
        note = (
            "rung1 matched recipe: mean pool, last layer, max_length 2048, L2"
            if comparable else
            "NOT on the matched recipe: 512-d text-aligned whole-protein "
            "projection, honours only normalize"
        )
        bind.execute(sa.text("""
            INSERT INTO embedding_config (
                id, model_name, model_backend, layer_indices, layer_agg, pooling,
                normalize_residues, normalize, embedding_scale, max_length,
                use_chunking, chunk_size, chunk_overlap,
                description, display_name, family
            ) VALUES (
                CAST(:id AS uuid), :model_name, :model_backend,
                CAST(:layer_indices AS jsonb), :layer_agg, :pooling,
                :normalize_residues, :normalize, :embedding_scale, :max_length,
                :use_chunking, :chunk_size, :chunk_overlap,
                :description, :display_name, :family
            ) ON CONFLICT (id) DO NOTHING
        """), {
            "id": cid, "model_name": model_name, "model_backend": backend,
            "layer_indices": json.dumps(STD["layer_indices"]),
            "layer_agg": STD["layer_agg"], "pooling": STD["pooling"],
            "normalize_residues": STD["normalize_residues"],
            "normalize": STD["normalize"], "embedding_scale": STD["embedding_scale"],
            "max_length": STD["max_length"], "use_chunking": STD["use_chunking"],
            "chunk_size": STD["chunk_size"], "chunk_overlap": STD["chunk_overlap"],
            "description": f"{key} | {note}",
            "display_name": key, "family": family,
        })

        row = bind.execute(sa.text(
            "SELECT model_name, model_backend, layer_indices, layer_agg, pooling,"
            " normalize_residues, normalize, embedding_scale, max_length,"
            " use_chunking, chunk_size, chunk_overlap"
            " FROM embedding_config WHERE id = CAST(:id AS uuid)"
        ), {"id": cid}).mappings().one_or_none()
        if row is None:
            raise RuntimeError(f"{key} ({cid}) missing after insert")
        expected = _recipe(model_name, backend)
        for col in _IDENTITY_COLS:
            got = list(row[col]) if col == "layer_indices" else row[col]
            if got != expected[col]:
                raise RuntimeError(
                    f"{key} ({cid}) diverges on {col}: db={got!r} "
                    f"expected={expected[col]!r}. This id already denotes a "
                    "different configuration."
                )


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    for expected_id, key, *_ in GRID:
        for table in _DEPENDENTS:
            n = bind.execute(sa.text(
                f"SELECT count(*) FROM {table}"  # noqa: S608 - fixed literal
                " WHERE embedding_config_id = CAST(:id AS uuid)"
            ), {"id": expected_id}).scalar_one()
            if n:
                raise RuntimeError(
                    f"refusing to drop {key} ({expected_id}): {n} {table} rows depend on it"
                )
        bind.execute(sa.text(
            "DELETE FROM embedding_config WHERE id = CAST(:id AS uuid)"
        ), {"id": expected_id})
