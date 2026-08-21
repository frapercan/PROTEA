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

from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet

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
