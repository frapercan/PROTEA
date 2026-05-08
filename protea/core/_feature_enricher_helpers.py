"""Per-group helpers extracted from ``expand_predictions_to_ancestors``.

The ancestor-expansion loop in ``feature_enricher`` mutates either an
existing leaf record (when an ancestor coincides with a sibling leaf
prediction) or a synthetic ancestor record (cloned from the leaf with
``go_id`` overridden). Both branches were inlined in the orchestrator,
pushing it well past the master plan v3.2 §3 method-LOC ceiling. This
module hosts the two branch helpers + the small label-config bundle so
the orchestrator can stay readable and short.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, NamedTuple


def make_ancestor_closure(
    parent_map: dict[str, set[str]] | dict[str, list[str]] | None,
) -> Callable[[str], frozenset[str]]:
    """Return a memoized ``go_id -> ancestor closure`` callable.

    The orchestrator calls the result repeatedly for every leaf GO id;
    caching is_a / part_of closures here keeps the inner loop O(1)
    after the first visit. ``parent_map`` is normalised to
    ``frozenset`` parents up front so subsequent traversal cannot
    accidentally mutate the caller's dict.
    """
    pm: dict[str, frozenset[str]] = {
        c: frozenset(parents) for c, parents in (parent_map or {}).items()
    }
    cache: dict[str, frozenset[str]] = {}

    def _ancestors(gid: str) -> frozenset[str]:
        cached = cache.get(gid)
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
        cache[gid] = result
        return result

    return _ancestors


def compute_ia_weight(
    anc_gid: str,
    leaf_gid: str,
    ia_weights: dict[str, float] | None,
) -> float:
    """Weight contributed by ``leaf_gid`` to its ancestor ``anc_gid``.

    Without IA weights every edge counts as 1.0. When weights are
    provided, the contribution is the IA ratio anc/leaf (with a guard
    against zero-weight leaves to avoid division blow-ups when an
    incomplete IA file leaves a term at IA=0).
    """
    if not ia_weights:
        return 1.0
    anc_w = float(ia_weights.get(anc_gid, 0.0))
    leaf_w = float(ia_weights.get(leaf_gid, 0.0))
    if leaf_w <= 0.0:
        return 1.0
    return anc_w / leaf_w


class LabelConfig(NamedTuple):
    """Bundle for the per-query label-injection knobs.

    Carries the query accession (used as half of the ``(q_acc, anc)``
    ground-truth lookup) plus the optional ``gt_pairs`` set and the
    column / presence flags that drive whether a synthesized ancestor
    record should carry a label at all. The training dump path passes
    a populated set; the live ``predict_go_terms`` path leaves
    ``gt_pairs`` as ``None`` and ``present`` as ``False``.
    """

    q_acc: str
    gt_pairs: set[tuple[str, str]] | None
    column: str
    present: bool


def merge_into_existing_leaf(
    leaf_anc: dict[str, Any],
    leaf_rec: dict[str, Any],
    vote_increment: float,
    leaf_d: float,
) -> None:
    """Fold a leaf-as-ancestor vote into the existing leaf candidate.

    Called when the ancestor of a leaf prediction is itself already a
    leaf candidate for the same ``(protein, aspect)`` group. Bumps the
    target's ``neighbor_vote_fraction`` and lowers its
    ``neighbor_min_distance`` if the contributing leaf is closer.
    """
    leaf_anc["neighbor_vote_fraction"] = min(
        1.0,
        float(leaf_anc.get("neighbor_vote_fraction", 0.0)) + vote_increment,
    )
    lmd = float(leaf_rec.get("neighbor_min_distance", leaf_d))
    cur_md = float(leaf_anc.get("neighbor_min_distance", leaf_d))
    if lmd < cur_md:
        leaf_anc["neighbor_min_distance"] = lmd


def update_synth_entry(
    synth: dict[str, dict[str, Any]],
    anc: str,
    leaf_rec: dict[str, Any],
    leaf_d: float,
    vote_increment: float,
    label_ctx: LabelConfig,
) -> None:
    """Insert or update a synthetic ancestor record.

    When the ancestor is not already a leaf candidate the record is
    fabricated from the closest contributing leaf. If a synthetic entry
    already exists, the closer leaf wins the per-pair feature payload
    (alignment, taxonomy, anc2vec, emb_pca) so the booster sees a
    consistent training-time view; otherwise only the vote fraction is
    bumped.
    """
    entry = synth.get(anc)
    if entry is None or leaf_d < float(entry.get("distance", float("inf"))):
        base = dict(leaf_rec)
        base["go_id"] = anc
        if label_ctx.present:
            base[label_ctx.column] = (
                1
                if label_ctx.gt_pairs and (label_ctx.q_acc, anc) in label_ctx.gt_pairs
                else 0
            )
        prior_frac = (
            float(entry["neighbor_vote_fraction"]) if entry is not None else 0.0
        )
        base["neighbor_vote_fraction"] = min(1.0, prior_frac + vote_increment)
        synth[anc] = base
    else:
        entry["neighbor_vote_fraction"] = min(
            1.0,
            float(entry["neighbor_vote_fraction"]) + vote_increment,
        )
