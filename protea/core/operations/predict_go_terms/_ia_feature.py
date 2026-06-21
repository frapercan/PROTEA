"""Information-accretion (``IA``) per-candidate booster feature.

Split out of ``_post_knn_pipeline`` (file-size budget) but conceptually part of
the post-KNN LAFA feature stage: ``apply_ia`` attaches ``IA(t)``, the
snapshot-invariant information content the cafaeval ``f_micro_w`` objective also
weights with, to each candidate prediction dict. The matching mirrors
``apply_self_prior`` (candidate go_id string, snapshot-invariant); the IA table
is resolved from the same go_id-keyed source the evaluation consumes.
"""

from __future__ import annotations

import uuid
from functools import lru_cache
from typing import Any

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn


def apply_ia(
    session: Session,
    ontology_snapshot_id: uuid.UUID,
    prediction_dicts: list[dict[str, Any]],
    emit: EmitFn,
    *,
    ia_file: str | None = None,
) -> None:
    """Set the per-candidate ``IA`` feature = information accretion of the term.

    IA(t) is the snapshot-invariant, query-independent information content the
    cafaeval ``f_micro_w`` objective also weights with, so attaching it as a
    booster feature aligns the reranker with the metric. Values come from the
    same go_id-keyed IA table the eval consumes (resolved by
    :func:`_load_ia_feature_map`). When no IA table resolves the feature is left
    unset (LightGBM missing branch) and the run stays bit-exact to a pre-IA
    prediction; matching mirrors ``apply_self_prior`` (candidate go_id string,
    snapshot-invariant).
    """
    from protea.core.operations.predict_go_terms._post_knn_pipeline import (
        _load_go_id_and_aspect,
    )

    ia_map = _load_ia_feature_map(session, ontology_snapshot_id, ia_file, emit)
    if not ia_map:
        return
    candidate_ids = {
        int(rec["go_term_id"]) for rec in prediction_dicts if rec.get("go_term_id") is not None
    }
    go_id_by_int, _aspect = _load_go_id_and_aspect(session, candidate_ids)
    hits = 0
    for rec in prediction_dicts:
        gtid = rec.get("go_term_id")
        if gtid is None:
            continue
        cand_go = rec.get("go_id") or go_id_by_int.get(int(gtid))
        if cand_go is None:
            continue
        val = ia_map.get(cand_go)
        if val is not None:
            rec["IA"] = float(val)
            hits += 1
    emit(
        "predict_go_terms_batch.ia_done",
        None,
        {"terms_in_ia_map": len(ia_map), "candidates_marked": hits},
        "info",
    )


def _load_ia_feature_map(
    session: Session,
    ontology_snapshot_id: uuid.UUID,
    ia_file: str | None,
    emit: EmitFn,
) -> dict[str, float]:
    """Resolve + parse the go_id-keyed IA table for the ``IA`` feature.

    Priority: explicit ``ia_file`` > ``PROTEA_IA_FEATURE_PATH`` env > the
    ontology snapshot's ``ia_url`` (downloaded once, cached). Returns an empty
    map (with a warning) when none resolves so :func:`apply_ia` no-ops.
    """
    import os

    from protea.infrastructure.orm.models.annotation.ontology_snapshot import (
        OntologySnapshot,
    )

    path = ia_file or os.environ.get("PROTEA_IA_FEATURE_PATH")
    if not path:
        snap = session.get(OntologySnapshot, ontology_snapshot_id)
        url = getattr(snap, "ia_url", None) if snap is not None else None
        if url:
            path = _download_ia_table(url)
    if not path or not os.path.exists(path):
        emit(
            "predict_go_terms_batch.ia_unresolved",
            None,
            {
                "warning": "No IA table resolved (ia_file / PROTEA_IA_FEATURE_PATH / "
                "snapshot.ia_url); the IA feature is left unset.",
            },
            "warning",
        )
        return {}
    return _cached_ia_map(path)


@lru_cache(maxsize=4)
def _cached_ia_map(path: str) -> dict[str, float]:
    """Parse + cache a go_id-keyed IA TSV (raw IA, 0 .. ~19)."""
    from protea.core.operations._run_cafa_artifacts import load_ia_map

    return load_ia_map(path)


@lru_cache(maxsize=4)
def _download_ia_table(url: str) -> str:
    """Download a snapshot ``ia_url`` once to a stable temp path; cache by URL."""
    import hashlib
    import os
    import tempfile

    from protea.core.operations._run_cafa_artifacts import download_tsv

    digest = hashlib.sha256(url.encode()).hexdigest()[:12]
    dest = os.path.join(tempfile.gettempdir(), f"protea_ia_feature_{digest}.tsv")
    if not os.path.exists(dest):
        download_tsv(url, dest)
    return dest
