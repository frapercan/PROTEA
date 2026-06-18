"""Re-stamp the VARIABLE classifier columns on a consolidated export frame.

NFR-ARCH "export decouple", variable half. Given a consolidated export frame
(the cached STABLE ``train.parquet`` / ``eval.parquet`` with the classifier
columns zero-filled), recompute ``classifier_score`` / ``classifier_present``
for the configured classifier and join them back in. This is the only step a
classifier A/B re-runs once the stable table is cached.

It reuses the EXACT predict-path classifier producer (``load_concat_features``
/ ``get_classifier`` / ``resolve_go_term_ids``), so the values are identical to
what the inline export applier
(``_export_features._union_classifier_candidates``) stamps onto existing rows.
The only difference is the carrier: a pandas frame keyed by
``(protein_accession, go_term_id)`` instead of the per-query record dicts. The
classifier reads ONLY the query protein's frozen PLM embeddings, so this
post-pass is leakage-equivalent to the inline producer no matter which stable
table it re-stamps.

NOTE: this frame re-stamp is STAMP-ONLY: it sets the score on candidates the
frozen stable table already carries and cannot APPEND the classifier-only rows
the inline applier unions into the pool. It is the variable half of the
stable-feature cache (a per-classifier-score A/B knob), not the candidate-pool
union; see ``_export_stable_cache_runner.run_with_stable_cache`` for the
limitation this implies for a classifier-on cached export.

The consolidated export frame names the GO id column ``go_term_id`` but stores
the GO id STRING (``GO:0000099``) there -- ``parquet_export._load_train_shards``
renames ``go_id`` -> ``go_term_id`` without converting to the integer id -- so
the join key matches the classifier producer's ``go_id`` strings directly.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd
    from sqlalchemy.orm import Session


@dataclass(frozen=True)
class ClassifierStampSpec:
    """Which classifier to run and which columns to stamp it into.

    Bundles the variant selectors and the target column names so
    :func:`apply_classifier_to_frame` stays a 4-argument call (one stamp per
    variant in the phase-2 multi-column loop). ``model_path`` / ``seed_dir`` /
    ``seed_paths`` select the classifier exactly like the corresponding env
    vars; when all are ``None`` the env-selected default classifier is used
    (byte-identical to the inline export applier). The default column names are
    the canonical single-classifier columns, so the default export is
    unaffected.
    """

    model_path: str | None = None
    seed_dir: str | None = None
    seed_paths: str | None = None
    score_column: str = "classifier_score"
    present_column: str = "classifier_present"


def apply_classifier_to_frame(
    session: Session,
    frame: pd.DataFrame,
    ontology_snapshot_id: uuid.UUID,
    spec: ClassifierStampSpec | None = None,
) -> None:
    """Fill the spec's score / present columns on ``frame`` in place.

    Mutates ``frame`` so each ``(protein_accession, go_term_id)`` candidate the
    classifier proposes carries its real score; rows the classifier does not
    propose keep their zero-fill default.

    No-op when the frame is empty or carries no ``protein_accession`` /
    ``go_term_id`` columns, so a cache hit on an empty split is safe.
    """
    spec = spec or ClassifierStampSpec()
    if frame.empty or "protein_accession" not in frame.columns:
        return
    if "go_term_id" not in frame.columns:
        return
    from protea.core.classifier_producer import load_concat_features, resolve_go_term_ids

    accessions = [a for a in dict.fromkeys(frame["protein_accession"].tolist()) if a]
    if not accessions:
        return
    features, valid = load_concat_features(session, accessions)
    if not valid:
        return
    preds = _classifier_for_spec(spec).predict(features, valid)
    go_ids = {pr.go_id for pr in preds}
    gid_by_go = resolve_go_term_ids(session, go_ids, ontology_snapshot_id)
    _stamp_predictions(frame, preds, gid_by_go, spec.score_column, spec.present_column)


def _classifier_for_spec(spec: ClassifierStampSpec) -> Any:
    """Resolve the classifier for a stamp spec.

    An explicit ``model_path`` forces the single-checkpoint path. With no
    explicit selector the env-selected default classifier is returned (the
    seed-averaged champion when ``PROTEA_CLASSIFIER_SEED_*`` is set), so the
    default re-stamp is byte-identical to the inline export applier.
    """
    from protea.core.classifier_producer import get_classifier, resolve_seed_paths

    if spec.model_path:
        return get_classifier(model_path=spec.model_path)
    # TODO(phase-2): thread seed_dir / seed_paths into a per-variant
    # SeedAveragedClassifier without mutating process env. ``resolve_seed_paths``
    # already accepts explicit args; the remaining work is a small cache keyed
    # by the resolved paths so repeated variants do not reload checkpoints.
    if spec.seed_dir is not None or spec.seed_paths is not None:
        resolve_seed_paths(seed_dir=spec.seed_dir, seed_paths=spec.seed_paths)
    return get_classifier()


def _stamp_predictions(
    frame: pd.DataFrame,
    preds: list[Any],
    gid_by_go: dict[str, int],
    score_column: str,
    present_column: str,
) -> None:
    """Write classifier outputs onto the frame's matching candidate rows.

    Keys by ``(protein_accession, go_term_id-string)``. The classifier
    producer yields ``go_id`` strings; ``gid_by_go`` is unused for the join
    (the frame already stores the GO id string under ``go_term_id``) but is
    resolved upstream to mirror the inline applier's contract and to drop
    predictions for terms absent from the snapshot vocabulary.
    """
    import numpy as np

    n = len(frame)
    scores = np.zeros(n, dtype=np.float64)
    present = np.zeros(n, dtype=np.float64)
    row_of: dict[tuple[str, str], int] = {}
    accs = frame["protein_accession"].tolist()
    gids = frame["go_term_id"].tolist()
    for i, (acc, gid) in enumerate(zip(accs, gids, strict=False)):
        row_of[(acc, str(gid))] = i
    for pr in preds:
        if pr.go_id not in gid_by_go:
            continue
        row = row_of.get((pr.accession, pr.go_id))
        if row is not None:
            scores[row] = float(pr.score)
            present[row] = 1.0
    frame[score_column] = scores
    frame[present_column] = present


__all__ = ("ClassifierStampSpec", "apply_classifier_to_frame")
