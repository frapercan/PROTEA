"""Content-derived identity for an :class:`EmbeddingConfig`.

An embedding config id is a hash of the recipe, not a random number. Two
machines that build the same recipe land on the same id without talking to each
other, and two recipes that differ anywhere cannot collide. That property is
what lets a campaign span more than one machine and more than one rebuild of the
database.

The derivation lived only inside the seeding migrations, whose docstring calls
itself "the only place an id is ever produced". It was not: the API route that
creates configs constructs ``EmbeddingConfig(**validated)`` and takes the
model's ``default=uuid.uuid4``, so a config created over HTTP got a random v4 id
while the seeded ones got content-derived v5 ids. The two schemes cannot ever
agree, and the registry this campaign runs on has already been split that way
once. This module exists so there is one derivation with callers, rather than
one derivation per place that remembers to do it.

Version 5 versus version 4 in the id itself is the tell: a v4 config id was
assigned, a v5 one was derived.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from typing import Any

__all__ = ["IDENTITY_FIELDS", "NAMESPACE", "derive_embedding_config_id", "recipe_of"]

#: Stable namespace for PROTEA embedding-config identity. Changing this string
#: reissues every id in the registry, which is a migration and not an edit.
NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://protea/embedding-config")

#: The fields that constitute identity. Anything absent from this tuple can
#: change without changing the id, so adding a field here is a decision about
#: what counts as the same embedding rather than a detail.
IDENTITY_FIELDS: tuple[str, ...] = (
    "chunk_overlap",
    "chunk_size",
    "embedding_scale",
    "layer_agg",
    "layer_indices",
    "max_length",
    "model_backend",
    "model_name",
    "normalize",
    "normalize_residues",
    "pooling",
    "use_chunking",
)


def recipe_of(config: Mapping[str, Any] | Any) -> dict[str, Any]:
    """Extract the identity-bearing fields from a mapping or an ORM instance.

    Missing fields raise rather than defaulting. A recipe assembled from
    defaults would produce an id for a config nobody described, and that id
    would look exactly as authoritative as a real one.
    """
    get = config.get if isinstance(config, Mapping) else lambda k, d=None: getattr(config, k, d)
    missing = [f for f in IDENTITY_FIELDS if get(f, _ABSENT) is _ABSENT]
    if missing:
        raise ValueError(
            f"cannot derive an embedding config id: {len(missing)} identity "
            f"field(s) absent from the recipe: {', '.join(missing)}"
        )
    return {f: get(f) for f in IDENTITY_FIELDS}


def derive_embedding_config_id(config: Mapping[str, Any] | Any) -> uuid.UUID:
    """Return the content-derived id for a recipe.

    ``uuid5(NAMESPACE, json(recipe, sorted, compact))`` over
    :data:`IDENTITY_FIELDS`. The JSON encoding is pinned to sorted keys and
    separators without spaces so the same recipe cannot hash differently for
    having been serialised by a different caller.
    """
    payload = json.dumps(recipe_of(config), sort_keys=True, separators=(",", ":"))
    return uuid.uuid5(NAMESPACE, payload)


class _Absent:
    """Sentinel distinguishing "field absent" from "field present and None"."""

    def __repr__(self) -> str:  # pragma: no cover
        return "<absent>"


_ABSENT = _Absent()
