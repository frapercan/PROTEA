"""seed rung-1 matched-parameter embedding grid

CAMPAIGN.md §5 RUNG 1.  FOUR EmbeddingConfig rows under ONE parameter
recipe (last layer, mean pooling, max_length 2048, L2-normalised).

Roster (researcher decision, 2026-07-29, final for this run)
------------------------------------------------------------
``ankh_base``, ``esm2_650m``, ``esmc_600m``, ``ankh_large``.  Every
other candidate was excluded on *measured* grounds, recorded here so the
grid is not "helpfully" re-widened later:

* ProtT5 and ProstT5 -- OOM at batch_size 4 and only 81% headroom at
  batch_size 2 on the 8188 MiB RTX 4060.  No fluid fit.
* ESM-2 3B -- non-loadable on this card, confirmed three independent
  ways.
* ESM-2 150M -- excluded by CAMPAIGN §5 as written.  The supporting
  measurement could not be located; the researcher chose to respect the
  document rather than re-derive it.  Do not mint it here.

``ankh_large`` is dispatched at batch_size 2 (5008 MiB; batch_size 4
leaves only 577 MiB).  Batch size is a *dispatch* parameter, not a
config field -- it is deliberately not stored on the row, because two
runs of the same recipe at different batch sizes must share an id.

Why a migration and not POST /embeddings/configs
------------------------------------------------
``_canonicalise_config`` (protea/services/_embeddings_validation_helpers.py
:106-127) returns a hardcoded 13-key dict that contains no ``id``, no
``display_name``, no ``family`` and no ``param_count``.  The API can
therefore neither pin a UUID nor populate the metadata that
annotate.py:112 orders the serve default on.  §0bis: source control
holds the procedure.

Seeding also disarms a live hazard: annotate.py:437 mints an unpinned
``uuid4`` ESM-2 650M config whenever ``embedding_config`` is empty.  It
is empty now.

Why new ids and not the ADR-D35 canonical set
---------------------------------------------
The conflict with D35 is proven, not assumed:

* the 2026-05 corpus was never matched-parameter -- ESM-2 ran at
  max_length 1024 while Ankh ran at 2048, so a uniform-2048 grid is a
  different configuration for at least the ESM cells;
* D35's ``esm2_3b`` row is chunking-enabled, this grid is not;
* D35 records ``08234f06`` (ankh_base) as ``normalize=FALSE``, and this
  grid needs TRUE.

Reusing any of those ids would silently relabel a different experiment,
and nothing in the schema could detect it: there is no unique constraint
over the recipe tuple.  D35 itself already distinguishes configs on a
single field (``084943c6`` vs ``db4db5ed`` on precision, ``08234f06``
vs ``c2868c1a`` on layers), so this is D35's own convention, not a
departure from it.

Why normalize=True is forced
----------------------------
Not a preference.  ``sequence_embedding`` stores halfvec, which caps at
~65504, and the EmbeddingConfig docstring records Ankh-base layer 10
reaching |max| ~4.9e5.  Normalising is what keeps ``embedding_scale``
uniform at 1.0 across the grid *and* the store overflow-proof.

Identity
--------
The ids are content-derived and **computed at migration time**, never
transcribed::

    uuid5(uuid5(NAMESPACE_URL, "https://protea/embedding-config"),
          json.dumps(recipe, sort_keys=True, separators=(",", ":")))

over the 12 identity-bearing fields.  Each GRID row also carries the
value that derivation is expected to produce; ``upgrade()`` recomputes
and refuses to run on any mismatch.  So the recipe is the single source
of truth (edit STD and the ids move with it, by construction), while the
pinned value is a tripwire that makes such a move impossible to do by
accident.  v5 (vs the historical v4 rows) marks them content-derived at
a glance.

Revision ID: e7a1c4f9b2d6
Revises: b4e2a9c7d1f3
"""

from __future__ import annotations

import json
import uuid

import sqlalchemy as sa

from alembic import op

revision = "e7a1c4f9b2d6"
down_revision = "b4e2a9c7d1f3"
branch_labels = None
depends_on = None

# The matched parameter set. Identical for every cell -- this is the
# grid's entire premise (CAMPAIGN §5: the grid must be homogeneous).
STD = {
    "layer_indices": [0],        # reverse indexing: 0 == last layer
    "layer_agg": "mean",         # no-op at len(layer_indices)==1
    "pooling": "mean",           # researcher instruction 2026-07-29
    "normalize_residues": False,
    "normalize": True,           # forced: halfvec caps at ~65504
    "embedding_scale": 1.0,
    "max_length": 2048,          # researcher instruction 2026-07-29
    "use_chunking": False,
    "chunk_size": 512,
    "chunk_overlap": 0,
}

# Stable namespace for PROTEA embedding-config identity.
_NS = uuid.uuid5(uuid.NAMESPACE_URL, "https://protea/embedding-config")

# The two fields that vary across the grid. Everything else is STD.
_MODEL_COLS = ("model_name", "model_backend")

# The 12 identity-bearing fields = STD (10) + _MODEL_COLS (2).
_IDENTITY_COLS = (*_MODEL_COLS, *sorted(STD))


def _recipe(model_name: str, model_backend: str) -> dict:
    """The 12 identity-bearing fields for one cell."""
    recipe = dict(STD, model_name=model_name, model_backend=model_backend)
    if len(recipe) != len(_IDENTITY_COLS):
        raise RuntimeError(
            f"recipe has {len(recipe)} fields, expected {len(_IDENTITY_COLS)}"
        )
    return recipe


def _derive_id(model_name: str, model_backend: str) -> str:
    """Content-derived config id. The only place an id is ever produced."""
    payload = json.dumps(
        _recipe(model_name, model_backend), sort_keys=True, separators=(",", ":")
    )
    return str(uuid.uuid5(_NS, payload))


# (expected_id, key, model_name, model_backend, dim, param_count, family)
#
# expected_id is a TRIPWIRE, not the source of truth: upgrade() derives the
# id from the recipe and aborts if it does not equal this value.
GRID: tuple[tuple, ...] = (
    ("0868f1ff-907a-5e4a-9d73-c0f2ed3c2437", "ankh_base",
     "ElnaggarLab/ankh-base", "ankh", 768, 453_170_688, "ankh"),
    ("ab430e07-5586-5bdc-9b7e-cc2a3ca18781", "esm2_650m",
     "facebook/esm2_t33_650M_UR50D", "esm", 1280, 652_353_941, "esm2"),
    # 575_036_992 is the exact sum of numel() over the 368 fp32 tensors in
    # esmc_600m_2024_12_v0.pth (transformer 573_558_912 + sequence_head
    # 1_404_352 + embed 73_728). The "600M" round number and the
    # 2_300_275_866 / 4 = 575.07M file-size estimate are both wrong -- the
    # latter counts 127_898 bytes of pickle/index overhead as weights.
    ("f64daa67-d540-5b1f-80a9-4673e0c31ed9", "esmc_600m",
     "esmc_600m", "esm3c", 1152, 575_036_992, "esmc"),
    ("9f52de2b-ec6e-5e48-b440-e506b50d62bb", "ankh_large",
     "ElnaggarLab/ankh-large", "ankh", 1536, 1_151_707_648, "ankh"),
)

_DESC = "rung1 matched grid: mean pool, last layer, max_length 2048, L2-normalised"

_INSERT = sa.text("""
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
""")

# Tables that would be damaged by dropping a config out from under them.
# sequence_embedding and prediction_set are ondelete=RESTRICT (the DELETE
# would raise an opaque FK error); dataset and reranker_model are
# ondelete=SET NULL, which is worse -- they would silently lose their
# provenance pointer. Refuse in all four cases.
_DEPENDENTS = (
    "sequence_embedding",
    "prediction_set",
    "dataset",
    "reranker_model",
)


def upgrade() -> None:
    bind = op.get_bind()

    for expected_id, key, model_name, backend, _dim, params, family in GRID:
        cid = _derive_id(model_name, backend)
        if cid != expected_id:
            raise RuntimeError(
                f"rung1 seed {key}: derived id {cid} != pinned {expected_id}. "
                "The recipe changed. These are content-derived ids -- update "
                "the pinned value deliberately, or revert the recipe; do NOT "
                "silently seed a relabelled experiment."
            )
        bind.execute(_INSERT, {
            "id": cid,
            "model_name": model_name,
            "model_backend": backend,
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
            "description": f"{key} | {_DESC}",
            "display_name": key,
            "family": family,
            "param_count": params,
        })

    # The uniqueness the schema does not have, asserted at migration time.
    # Makes a second `alembic upgrade head` a true no-op rather than a
    # silent skip over a row somebody mutated underneath us.
    for expected_id, key, model_name, backend, _dim, _params, _family in GRID:
        row = bind.execute(sa.text(
            "SELECT model_name, model_backend, layer_indices, layer_agg,"
            " pooling, normalize_residues, normalize, embedding_scale,"
            " max_length, use_chunking, chunk_size, chunk_overlap"
            " FROM embedding_config WHERE id = CAST(:id AS uuid)"
        ), {"id": expected_id}).mappings().one_or_none()
        if row is None:
            raise RuntimeError(
                f"rung1 seed {key} ({expected_id}) missing after insert"
            )
        expected = _recipe(model_name, backend)
        for col in _IDENTITY_COLS:
            got = list(row[col]) if col == "layer_indices" else row[col]
            if got != expected[col]:
                raise RuntimeError(
                    f"rung1 seed {key} ({expected_id}) diverges on {col}: "
                    f"db={got!r} expected={expected[col]!r}. "
                    "Refusing to proceed -- this id already denotes a "
                    "different configuration."
                )


def downgrade() -> None:
    bind = op.get_bind()
    for expected_id, key, *_ in GRID:
        for table in _DEPENDENTS:
            n = bind.execute(sa.text(
                f"SELECT count(*) FROM {table}"  # noqa: S608 - fixed literal
                " WHERE embedding_config_id = CAST(:id AS uuid)"
            ), {"id": expected_id}).scalar_one()
            if n:
                raise RuntimeError(
                    f"refusing to drop {key} ({expected_id}): {n} {table} "
                    "rows depend on it"
                )
        bind.execute(sa.text(
            "DELETE FROM embedding_config WHERE id = CAST(:id AS uuid)"
        ), {"id": expected_id})
