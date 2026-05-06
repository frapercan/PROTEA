"""V6 feature enrichment — shim + DB-bound helpers.

The pure-compute v6 pipeline lives in ``protea_method.feature_enricher``
(F2C extraction, 2026-05-07). This module wraps it for callers that
pass a SQLAlchemy ``Session``: ``enrich_v6_features`` here loads
``go_id_map`` / ``go_aspect_map`` from the database and forwards them
to the library function. The DB-bound helpers
(``_load_go_term_metadata``, ``load_parent_map``) and the local
ancestor-expansion helper (``expand_predictions_to_ancestors``,
which is pure compute but PROTEA-internal) stay here.
"""

from __future__ import annotations

import uuid
from typing import Any

import numpy as np
from sqlalchemy.orm import Session

from protea_method.feature_enricher import NEW_V6_FEATURE_KEYS
from protea_method.feature_enricher import enrich_v6_features as _lib_enrich_v6_features

from protea.infrastructure.orm.models.annotation.go_term import GOTerm


def _load_go_term_metadata(
    session: Session,
    go_term_ids: set[int],
) -> tuple[dict[int, str], dict[int, str]]:
    """Return ``(go_id_map, aspect_map)`` for the given ``GOTerm.id`` set.

    Both maps are keyed by the numeric ``go_term_id``. Values are the
    canonical ``GO:NNNNNNN`` string and the single-char aspect (``P`` /
    ``F`` / ``C``), respectively. Chunked to stay within parameter
    limits.
    """
    go_id_map: dict[int, str] = {}
    aspect_map: dict[int, str] = {}
    if not go_term_ids:
        return go_id_map, aspect_map
    from protea.config.tuning import get_tuning

    chunk_size = get_tuning().operation.annotation_chunk_size
    ids_list = list(go_term_ids)
    for i in range(0, len(ids_list), chunk_size):
        chunk = ids_list[i : i + chunk_size]
        rows = (
            session.query(GOTerm.id, GOTerm.go_id, GOTerm.aspect)
            .filter(GOTerm.id.in_(chunk))
            .all()
        )
        for gid, go_str, aspect in rows:
            go_id_map[gid] = go_str
            aspect_map[gid] = aspect or ""
    return go_id_map, aspect_map


def _collect_gtids_in_play(
    predictions: list[dict[str, Any]],
    go_map_by_aspect: dict[str, dict[str, list[dict[str, Any]]]],
) -> set[int]:
    """Every ``go_term_id`` referenced as either a candidate in
    predictions or as an annotation of a voting neighbour. The
    library has the same helper module-private; we duplicate the
    9-line body here to avoid importing across the package boundary.
    """
    gtids: set[int] = {int(pred["go_term_id"]) for pred in predictions}
    for go_map in go_map_by_aspect.values():
        for anns in go_map.values():
            for ann in anns:
                gtids.add(int(ann["go_term_id"]))
    return gtids


def enrich_v6_features(
    predictions: list[dict[str, Any]],
    *,
    session: Session,
    valid_accessions: list[str],
    query_embeddings: np.ndarray,
    neighbors_by_aspect: dict[str, list[list[tuple[str, float]]]],
    go_map_by_aspect: dict[str, dict[str, list[dict[str, Any]]]],
    pair_features: dict[tuple[str, str], dict[str, Any]],
    pca_state: tuple[np.ndarray, np.ndarray] | None,
    compute_taxonomy: bool,
) -> None:
    """Compute the 25 v6 features and merge them into each ``pred`` dict in place.

    PROTEA-side wrapper around
    ``protea_method.feature_enricher.enrich_v6_features``: collects
    the GO term ids in play, loads the metadata maps from the
    database, and forwards everything to the library function. The
    library does the Anc2Vec pool, neighbor centroids, tax voters,
    PCA projection, and per-row merge.
    """
    if not predictions:
        return

    gtids_in_play = _collect_gtids_in_play(predictions, go_map_by_aspect)
    go_id_map, go_aspect_map = _load_go_term_metadata(session, gtids_in_play)

    _lib_enrich_v6_features(
        predictions,
        go_id_map=go_id_map,
        go_aspect_map=go_aspect_map,
        valid_accessions=valid_accessions,
        query_embeddings=query_embeddings,
        neighbors_by_aspect=neighbors_by_aspect,
        go_map_by_aspect=go_map_by_aspect,
        pair_features=pair_features,
        pca_state=pca_state,
        compute_taxonomy=compute_taxonomy,
    )


def expand_predictions_to_ancestors(
    predictions: list[dict[str, Any]],
    *,
    parent_map: dict[str, set[str]] | dict[str, list[str]],
    k_limit: int,
    ia_weights: dict[str, float] | None = None,
    gt_pairs: set[tuple[str, str]] | None = None,
    label_column: str = "label",
    label_field_present: bool = False,
) -> list[dict[str, Any]]:
    """Expand each leaf prediction to its is_a / part_of ancestor closure.

    Mirrors the in-loop expansion in
    ``protea.core.training_dump_helpers._knn_transfer_and_label`` so
    the live ``predict_go_terms`` path and the offline dump helper
    share a single canonical implementation. Without it the
    candidate sets diverge: the lab dump expanded to ancestors, live
    KNN didn't, and v9 / v10 boosters scored LK / PK candidates on a
    feature distribution they never saw at training time.

    Per ``(protein_accession, aspect)`` group, adds the ancestor
    closure of each leaf go_id. When an ancestor is itself already
    a leaf candidate, votes merge into the existing record (bumping
    ``neighbor_vote_fraction`` / ``neighbor_min_distance``).
    Otherwise a synthetic record clones the closest leaf and
    overrides ``go_id``. Synthetic records inherit the leaf's
    per-pair features verbatim (alignment, taxonomy, anc2vec,
    emb_pca) which matches the train-side convention.
    """
    if not predictions:
        return predictions

    pm: dict[str, frozenset[str]] = {
        c: frozenset(parents) for c, parents in (parent_map or {}).items()
    }
    closure: dict[str, frozenset[str]] = {}

    def _ancestors(gid: str) -> frozenset[str]:
        cached = closure.get(gid)
        if cached is not None:
            return cached
        seen: set[str] = set()
        stack = [gid]
        while stack:
            node = stack.pop()
            for parent in pm.get(node, ()):
                if parent not in seen:
                    seen.add(parent)
                    stack.append(parent)
        result = frozenset(seen)
        closure[gid] = result
        return result

    def _ia_weight(anc_gid: str, leaf_gid: str) -> float:
        if not ia_weights:
            return 1.0
        anc_w = float(ia_weights.get(anc_gid, 0.0))
        leaf_w = float(ia_weights.get(leaf_gid, 0.0))
        if leaf_w <= 0.0:
            return 1.0
        return anc_w / leaf_w

    k_limit_f = float(k_limit) if k_limit > 0 else 1.0

    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for rec in predictions:
        key = (rec.get("protein_accession", ""), rec.get("aspect", ""))
        groups.setdefault(key, []).append(rec)

    out: list[dict[str, Any]] = []
    for (q_acc, _aspect), recs in groups.items():
        leaf_by_gid: dict[str, dict[str, Any]] = {r["go_id"]: r for r in recs}
        synth: dict[str, dict[str, Any]] = {}
        for leaf_gid, leaf_rec in list(leaf_by_gid.items()):
            leaf_d = float(leaf_rec.get("distance", 1.0))
            for anc in _ancestors(leaf_gid):
                w = _ia_weight(anc, leaf_gid)
                if anc in leaf_by_gid:
                    leaf_anc = leaf_by_gid[anc]
                    leaf_anc["neighbor_vote_fraction"] = min(
                        1.0,
                        float(leaf_anc.get("neighbor_vote_fraction", 0.0))
                        + w / k_limit_f,
                    )
                    lmd = float(leaf_rec.get("neighbor_min_distance", leaf_d))
                    cur_md = float(leaf_anc.get("neighbor_min_distance", leaf_d))
                    if lmd < cur_md:
                        leaf_anc["neighbor_min_distance"] = lmd
                    continue
                entry = synth.get(anc)
                if entry is None or leaf_d < float(entry.get("distance", float("inf"))):
                    base = dict(leaf_rec)
                    base["go_id"] = anc
                    if label_field_present:
                        base[label_column] = (
                            1 if (gt_pairs and (q_acc, anc) in gt_pairs) else 0
                        )
                    prior_frac = (
                        float(entry["neighbor_vote_fraction"])
                        if entry is not None
                        else 0.0
                    )
                    base["neighbor_vote_fraction"] = min(1.0, prior_frac + w / k_limit_f)
                    synth[anc] = base
                else:
                    entry["neighbor_vote_fraction"] = min(
                        1.0,
                        float(entry["neighbor_vote_fraction"]) + w / k_limit_f,
                    )

        out.extend(leaf_by_gid.values())
        out.extend(synth.values())
    return out


def load_parent_map(session: Session, snapshot_id: uuid.UUID) -> dict[str, set[str]]:
    """``{child_go_id: {parent_go_id, ...}}`` for is_a + part_of edges in a
    given :class:`OntologySnapshot`. Used by the ancestor-expansion helper
    above; both the live ``predict_go_terms`` path and offline
    ``the dump helper`` should load it through this function so the closure
    they pass to :func:`expand_predictions_to_ancestors` is identical."""
    from sqlalchemy import text

    rows = session.execute(
        text(
            "SELECT c.go_id AS child, p.go_id AS parent "
            "FROM go_term_relationship r "
            "JOIN go_term c ON c.id = r.child_go_term_id "
            "JOIN go_term p ON p.id = r.parent_go_term_id "
            "WHERE r.ontology_snapshot_id = :snap_id "
            "AND r.relation_type IN ('is_a', 'part_of')"
        ),
        {"snap_id": snapshot_id},
    ).fetchall()
    parent_map: dict[str, set[str]] = {}
    for child, parent in rows:
        parent_map.setdefault(str(child), set()).add(str(parent))
    return parent_map


__all__ = [
    "NEW_V6_FEATURE_KEYS",
    "enrich_v6_features",
    "expand_predictions_to_ancestors",
    "load_parent_map",
]
