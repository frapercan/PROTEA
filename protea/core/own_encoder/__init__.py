"""The own segment encoder (``jepa-segments``) and the embedding bank it writes.

A label-free encoder of our own over the lexicon's cut: each protein is the
sequence of its lexicon segments (theme, length, order), and a predictive
encoder learns to predict, in latent space, the target encoder's representation
of hidden segments from the visible ones. It never reads a GO label or a
protein language model. Ported from the laboratory's segment arm (``jepa-tramos``
of ``jepa_lexico.py``, card jepa-lexico of 2026-11-13); the port is
``protea.core.own_encoder.model``.

It reaches the platform as an embedding bank (ADR-D62), so the search reads it
like any language model's bank:

- ``protea.core.own_encoder.train`` trains it on a cluster card on the lexicon
  cut of a declared list of member sequences and writes ``encoder.npz`` (the
  target encoder's weights, the same bytes for the same weights) with its
  record, ``own-encoder.json``;
- ``protea.core.own_encoder.vectors`` writes every bank sequence's vector (the
  target encoder's mean over the protein's segments) as the shards and the
  manifest ``load_embedding_bank`` checks.

ONE CONFIGURATION PER TRAINED ENCODER. The configuration's ``model_name`` is
``jepa-segments/<sha256 of encoder.npz>`` and its backend is ``own-encoder``, so
two encoders (an instance trained on members up to 211 and one up to 227) get two
content-derived configuration ids. The backend is a producer tag in
``VALID_BACKENDS`` and not in the leakage guard's ``FITTED_BACKENDS``: the
encoder fits no annotation, and declaring a training release for it would be
false. Its training population (the member list) is declared by digest in its
record instead. The configuration is registered through the API's configuration
create endpoint with :func:`configuration` before ``load_embedding_bank`` runs.

THE FAMILY. The segment encoder belongs to the ``world-model`` method family
(the author's decision of 2026-11-28): a learned representation of our own, not
a protein language model's and not an explainable unit. Its records say so.

This module imports nothing outside the standard library and
``protea.core.embedding_identity``, so a cluster node can import it.
"""

from __future__ import annotations

import uuid
from typing import Any

from protea.core.embedding_identity import derive_embedding_config_id

__all__ = [
    "BACKEND",
    "FAMILY",
    "LINEAGE",
    "NAME",
    "configuration",
    "configuration_id",
    "configuration_recipe",
    "model_name",
]

#: The producer tag of the configuration: written only for this encoder's banks.
BACKEND = "own-encoder"
#: The encoder's name, the first half of every ``model_name`` it gets.
NAME = "jepa-segments"
#: The lineage a configuration names in ``embedding_config.family``: a lineage of
#: its own, since it descends from no pretrained model.
LINEAGE = NAME
#: The method family (``protea.core.contracts.method.Family``) the encoder belongs
#: to; written as its slug so this module stays free of the contracts package.
FAMILY = "world-model"


def model_name(encoder_sha256: str) -> str:
    """``jepa-segments/<sha256>``: the name of the configuration of one trained encoder."""
    digest = encoder_sha256.strip().lower()
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise ValueError(f"{encoder_sha256!r} is not a sha256 of the encoder's weights")
    return f"{NAME}/{digest}"


def configuration_recipe(encoder_sha256: str) -> dict[str, Any]:
    """The identity fields of the configuration of one trained encoder, every one stated.

    The vector is the target encoder's output after its final layer norm (layer
    0, counted from the last), averaged over the protein's segments, stored as
    computed (not normalised: the search's cosine normalises it). Nothing is
    truncated by residues, so ``max_length`` is null; the encoder reads at most
    a protein's first 64 segments, a property of the weights, which the name
    already identifies.
    """
    return {
        "model_name": model_name(encoder_sha256),
        "model_backend": BACKEND,
        "layer_indices": [0],
        "layer_agg": "mean",
        "pooling": "mean",
        "normalize_residues": False,
        "normalize": False,
        "embedding_scale": 1.0,
        "max_length": None,
        "use_chunking": False,
        "chunk_size": 512,
        "chunk_overlap": 0,
    }


def configuration_id(encoder_sha256: str) -> uuid.UUID:
    """The content-derived id the platform gives the configuration of one trained encoder."""
    return derive_embedding_config_id(configuration_recipe(encoder_sha256))


def configuration(encoder_sha256: str, training: dict[str, Any]) -> dict[str, Any]:
    """The body of ``POST /embeddings/configs`` that registers one trained encoder.

    ``training`` is the encoder's record (``own-encoder.json``); the description
    names its training population by digest, its lexicon and its plan. The
    encoder is not fitted on annotations, so the body declares no training
    release and the kind ``pretrained``.
    """
    members = training["members"]
    lexicon = training["lexicon"]
    return {
        **configuration_recipe(encoder_sha256),
        "kind": "pretrained",
        "family": LINEAGE,
        "display_name": f"{NAME} {encoder_sha256[:12]}",
        "description": (
            f"The own segment encoder {NAME} (method family {FAMILY}), label-free and free of "
            f"protein language models: trained on the lexicon {lexicon['name']}'s cut of "
            f"{members['md5s']:,} member sequences (list sha256 {members['sha256']}) for a "
            f"{training['plan']['plan_minutes']:g}-minute plan, weights sha256 "
            f"{encoder_sha256}. A vector is the target encoder's mean over a protein's first "
            f"{training['plan']['max_segments']} segments, not normalised."
        ),
    }
