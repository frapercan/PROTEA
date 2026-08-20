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

import tempfile
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet


def resolve_encoder_artifact(path: str | None, uri: str | None) -> str:
    """Return a local path for the artifact, fetching it from the store if addressed by one.

    The store copy is written under the platform's cache rather than beside the
    caller, so two workers on the same host share one download and a worker that
    restarts does not fetch it again.
    """
    if path:
        return path
    if not uri:
        raise ValueError("resolve_encoder_artifact needs a path or a uri and was given neither")
    from pathlib import Path as _Path

    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    project_root = _Path(__file__).resolve().parents[3]
    store = get_artifact_store(load_settings(project_root))
    cache = _Path(tempfile.gettempdir()) / "protea-encoder-artifacts"
    cache.mkdir(parents=True, exist_ok=True)
    local = cache / uri.replace("/", "_")
    if not local.exists():
        local.write_bytes(store.get(uri))
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
