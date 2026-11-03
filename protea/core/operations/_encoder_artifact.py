"""Resolving what a frozen encoder artifact says about itself.

Both encoder operations need the same two answers before they can run one: where the
artifact actually is, and when it was fitted. Neither question is about the recipe, and
both have the same failure mode when unanswered, which is a plausible run that produced
the wrong thing or claimed the wrong provenance.

WHY THE ADDRESS IS NOT A PATH

A local path resolves against whichever host runs the work, and the two operations run on
different machines: one fans out to the queue that has a card, the other runs inline
wherever the operations queue is consumed. No path means the same thing on both.

WHY THE CUT IS NOT OPTIONAL

``trained_on_annotation_set_id`` is NULL for a pretrained backbone and that is its
declared meaning: an encoder that never saw our annotations has no cut, which is a
different state from a cut nobody recorded. A fitted artifact that leaves it NULL claims
something false, and the temporal gate reads the column, so the claim decides whether the
artifact may be scored in a frame at all.
"""

from __future__ import annotations

import hashlib
import re
import tempfile
import uuid

import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet

#: Fields the artifact must declare. Absent any of them the artifact is refused:
#: defaulting them would let a pooled-encoder artifact run through the residue path
#: and produce codes that look right and were computed from something else.
#:
#: ``aggregate`` joined this list once the aggregate stopped being a constant. A code
#: built from two moments and one built from the mean have the same weights and
#: different widths, so a default here would silently halve or double a corpus.
REQUIRED_META = (
    "k_residue",
    "k_sequence",
    "dict_dim",
    "in_dim",
    "layer_indices",
    "aggregate",
    "order",
    "training_release",
)

#: Which of two pipelines the map was fitted for.
#:
#: ``select-then-pool`` applies the map per residue, keeps ``k_residue``
#: atoms of each, aggregates, then keeps ``k_sequence``. It is what this
#: operation implements and what every shipped recipe uses.
#:
#: ``pool-then-select`` aggregates the residues first and applies the map
#: to the result. It is the natural control arm, and it is NOT implemented
#: here: declaring it is refused with a message saying so, rather than
#: executed as the other order.
#:
#: The distinction cannot be recovered from the weights. The map is affine,
#: so mean(X @ W + b) equals mean(X) @ W + b to within 7e-07, and an
#: artifact fitted either way has identical shapes and passes every check
#: on them. Served under the wrong order it produced a code sharing 130 of
#: 2048 atoms with the intended one, at cosine 0.10. Not a degradation, a
#: different encoder.
ORDERS = ("select-then-pool", "pool-then-select")

#: How the per-residue codes become one code. ``mean`` is the first moment alone;
#: ``moments`` concatenates the mean with the per-atom dispersion and therefore emits
#: twice the dictionary width. Both are a fixed-width reduction over residues, which is
#: what keeps a corpus pass costing one protein of memory rather than the corpus.
AGGREGATES = ("mean", "moments")


#: A key ending in ``-<hex>`` before its extension names its own content. Twelve hex
#: characters of sha256 is 48 bits, which is far past the point where a collision is a
#: sensible thing to worry about for a store holding tens of artifacts.
_ADDRESSED = re.compile(r"-([0-9a-f]{12,64})(\.[A-Za-z0-9]+)?$")


def content_digest_in(uri: str) -> str | None:
    """The digest a content-addressed key carries, or None if the key is a plain name."""
    m = _ADDRESSED.search(uri)
    return m.group(1) if m else None


def resolve_encoder_artifact(path: str | None, uri: str | None) -> str:
    """Return a local path for the artifact, fetching it from the store if addressed by one.

    A CONTENT-ADDRESSED key is cached; a plain one is not, and the difference is the whole
    point rather than an optimisation.

    The first version of this cached every key under a filename derived from the key. That
    made a stale cache unavoidable: re-uploading a corrected artifact under the same name
    left every host that had already fetched it serving the old bytes, with nothing to
    notice and no way for the publisher to reach the other machine's disk. It stalled a
    corpus run and could not be fixed from the side that found it.

    A key ending in the digest of its own content cannot go stale, because different bytes
    are a different key. So the cache needs no invalidation at all, and the failure stops
    being a bug that was fixed and becomes one that cannot be written. A plain key gets no
    cache, which costs a fetch per call and says so, because the alternative is a cache
    that is silently wrong rather than slow.

    The digest is verified against the bytes that arrive, so the key is checked rather than
    trusted, and a truncated or swapped object is refused instead of loaded.
    """
    if path:
        return path
    if not uri:
        raise ValueError("resolve_encoder_artifact needs a path or a uri and was given neither")
    from pathlib import Path as _Path

    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    digest = content_digest_in(uri)
    cache = _Path(tempfile.gettempdir()) / "protea-encoder-artifacts"
    cache.mkdir(parents=True, exist_ok=True)
    local = cache / uri.replace("/", "_")
    if digest and local.exists():
        return str(local)

    project_root = _Path(__file__).resolve().parents[3]
    store = get_artifact_store(load_settings(project_root))
    raw = store.get(uri)
    if digest:
        got = hashlib.sha256(raw).hexdigest()
        if not got.startswith(digest):
            raise ValueError(
                f"{uri} names content {digest} and the store returned {got[: len(digest)]}. "
                "The object under that key is not the one the key describes, so it is "
                "refused rather than loaded"
            )
    local.write_bytes(raw)
    return str(local)


def resolve_training_cut(session: Session, meta: dict) -> uuid.UUID:
    """The annotation set the artifact says it was fitted against.

    ``trained_on_annotation_set_id`` is NULL for a pretrained backbone and that is its
    declared meaning: an encoder that never saw our annotations has no cut, which is a
    different state from a cut nobody recorded. A fitted artifact that leaves it NULL is
    therefore claiming something false, and the temporal gate reads the column, so the
    claim is load-bearing rather than cosmetic.

    Resolved from the release the artifact declares rather than from anything about the
    corpus in front of us, because the fit happened elsewhere and only the artifact knows
    when.
    """
    release = str(meta["training_release"]).strip()
    aset = session.execute(
        select(AnnotationSet.id).where(AnnotationSet.source_version == release)
    ).scalar_one_or_none()
    if aset is None:
        raise ValueError(
            f"the artifact declares it was fitted on annotation release {release!r} and "
            "no annotation set carries that source_version. The cut cannot be recorded, "
            "and an artifact whose cut cannot be recorded cannot be checked against the "
            "frame it is about to be scored in"
        )
    return aset


def load_frozen_encoder(path: str) -> tuple[np.ndarray, np.ndarray, dict]:
    """Read the map and its recipe, refusing anything that does not declare one.

    The refusal is the point. An artifact fitted for pooled use has the same tensor
    shapes as one fitted for residue use, so nothing about the weights themselves
    would reveal the mistake, and the codes it produced would be complete and wrong.
    """
    data = np.load(path, allow_pickle=True)
    missing = [k for k in REQUIRED_META if k not in data]
    if missing:
        raise ValueError(
            f"{path} declares no {missing}; a residue-level encoder must state its "
            "recipe, and defaulting it would let a pooled-encoder artifact produce "
            "codes through this path that look right and were computed differently"
        )
    meta = {k: data[k].tolist() for k in REQUIRED_META}
    weight = np.asarray(data["W"], dtype=np.float32)
    bias = np.asarray(data["b"], dtype=np.float32)
    if weight.shape != (meta["in_dim"], meta["dict_dim"]):
        raise ValueError(
            f"the map is {weight.shape} but the recipe declares "
            f"({meta['in_dim']}, {meta['dict_dim']})"
        )
    if str(meta["aggregate"]) not in AGGREGATES:
        raise ValueError(
            f"{path} declares aggregate {meta['aggregate']!r}, which is not one of "
            f"{list(AGGREGATES)}. The aggregate decides the code's width, so an "
            "unknown one cannot be guessed"
        )
    order = str(meta["order"])
    if order not in ORDERS:
        raise ValueError(
            f"{path} declares order {order!r}, which is not one of {list(ORDERS)}. "
            "The order decides what the code IS, and it cannot be recovered from the "
            "weights: an artifact fitted either way has identical shapes"
        )
    if order == "pool-then-select":
        raise ValueError(
            f"{path} declares order 'pool-then-select', which this operation does not "
            "implement. Refused rather than run as 'select-then-pool', because the two "
            "produce different codes from the same weights and the difference is "
            "invisible in the output"
        )
    if int(meta["k_residue"]) >= int(meta["dict_dim"]):
        raise ValueError(
            f"k_residue {meta['k_residue']} selects the whole {meta['dict_dim']}-atom "
            "dictionary, which is not a sparse code. Note that under "
            "'pool-then-select' this is exactly the right value, since the map is "
            "affine and k_residue = dict_dim expresses pooling exactly; that is the "
            "hole this check used to have, and the order field is what closes it"
        )
    return weight, bias, meta
