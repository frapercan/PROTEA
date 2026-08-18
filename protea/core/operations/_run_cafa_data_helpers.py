"""Pre-cafaeval data preparation helpers extracted from
``RunCafaEvaluationOperation.execute``.

These pure functions operate on the in-memory :class:`EvaluationData`
buckets before cafaeval is invoked: they restrict the ground truth
to the actually-predicted protein cohort, which is the standard CAFA
practice that prevents delta proteins outside the PredictionSet's
query coverage from hurting Fmax / coverage.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Collection
from typing import Any

from sqlalchemy import distinct, select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.evaluation import EvaluationData
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction

#: Below this many ground-truth proteins the restriction gate stays silent. The
#: real incidents were over 6,216; anything two orders of magnitude smaller is not
#: an evaluation whose cohort ratio means anything.
MIN_GATED_COHORT = 50


def restriction_fraction(before: int, after: int) -> float:
    """Share of the evaluation cohort the restriction removed."""
    if before <= 0:
        return 0.0
    return (before - after) / before


def _refuse_a_restricted_cohort(before: int, after: int, dropped: float) -> None:
    """Stop an evaluation whose cohort no longer describes the evaluation set.

    Dropping ground-truth proteins the prediction set never covered is correct
    CAFA practice for a FINISHED run, and it is a trap for an unfinished one. It
    turns "we predicted only two thirds of the targets" into "we scored the cohort
    we predicted", so a partially written prediction set produces a metric that is
    plausible, well formed, and indistinguishable on the board from a complete one.

    On 2026-08-18 two evaluations scored 66 and 83 per cent of the population and
    reached the board that way. The counts this function now gates on were already
    being emitted at the time. Nothing divided them.

    A run that means to score a reduced cohort raises the threshold deliberately,
    which puts the decision in the payload's environment rather than in an
    unexamined default.
    """
    from protea.config.tuning import get_tuning

    if before < MIN_GATED_COHORT:
        # Calibration, kept rather than hidden: the guard first hit five existing
        # tests whose mocked sessions return no predicted accessions at all, over
        # ground truths of two proteins. Reading every hit is what the standing
        # rule asks for, and what they say is that a cohort this small is a
        # fixture or a degenerate case, not a measurement. A CAFA evaluation over
        # a handful of proteins has worse problems than its restriction ratio.
        return

    limit = get_tuning().operation.max_ground_truth_restriction
    if limit >= 1.0 or dropped <= limit:
        return
    raise ValueError(
        f"restricting the ground truth to the predicted cohort dropped "
        f"{dropped:.1%} of it, {before:,} proteins down to {after:,}, above the "
        f"{limit:.0%} allowed. The usual cause is evaluating a prediction set "
        "that is still being written: gate on the prediction job reaching "
        "SUCCEEDED rather than on rows existing. To score a reduced cohort "
        "deliberately, raise "
        "PROTEA_TUNING__operation__max_ground_truth_restriction"
    )


def restrict_data_to_predicted(
    session: Session,
    *,
    prediction_set_id: uuid.UUID,
    data: EvaluationData,
    emit: EmitFn,
) -> EvaluationData:
    """Drop ground-truth proteins not present in the PredictionSet cohort.

    Without this filter, delta proteins that the PredictionSet never
    actually predicted (e.g. the booster failed for them, or they
    were excluded at query time) would still count as "missing"
    against Fmax / coverage. CAFA practice is to evaluate only on
    the predicted cohort.

    Returns a new :class:`EvaluationData` with each of the five
    ``{protein → set[go_id]}`` mappings filtered to the predicted
    accessions. Emits a ``gt_restricted_to_predicted`` event with
    before/after counts for the audit trail.
    """
    predicted_set: set[str] = set(
        session.execute(
            select(distinct(GOPrediction.protein_accession)).where(
                GOPrediction.prediction_set_id == prediction_set_id
            )
        )
        .scalars()
        .all()
    )
    orig_counts = (len(data.nk), len(data.lk), len(data.pk))
    new_data = type(data)(
        nk={k: v for k, v in data.nk.items() if k in predicted_set},
        lk={k: v for k, v in data.lk.items() if k in predicted_set},
        pk={k: v for k, v in data.pk.items() if k in predicted_set},
        pk_known={k: v for k, v in data.pk_known.items() if k in predicted_set},
        known={k: v for k, v in data.known.items() if k in predicted_set},
    )
    before = sum(orig_counts)
    after = len(new_data.nk) + len(new_data.lk) + len(new_data.pk)
    dropped = restriction_fraction(before, after)
    emit(
        "run_cafa_evaluation.gt_restricted_to_predicted",
        None,
        {
            "predicted_proteins": len(predicted_set),
            "nk_before": orig_counts[0],
            "nk_after": len(new_data.nk),
            "lk_before": orig_counts[1],
            "lk_after": len(new_data.lk),
            "pk_before": orig_counts[2],
            "pk_after": len(new_data.pk),
            # The fraction is computed here rather than left to a reader, because
            # the counts were always emitted and nobody ever divided them.
            "dropped_fraction": round(dropped, 4),
        },
        "warning" if dropped > 0 else "info",
    )
    _refuse_a_restricted_cohort(before, after, dropped)
    return new_data


def restrict_data_to_protein_fold(
    data: EvaluationData,
    *,
    folds: int,
    fold: int,
    seed: int,
    emit: EmitFn,
) -> EvaluationData:
    """Keep only proteins whose deterministic hash-fold equals ``fold``.

    A LEAN internal train/valid split for score HPO (A-SCORE.2 beat-pooled
    guard): the per-cell champion is selected on one fold and confirmed on the
    held-out fold, with no new EvaluationSet rows and no homology clustering. The
    fold is ``blake2b(f"{seed}:{protein}") % folds`` so it is stable across runs
    and independent of cell / config. ``folds <= 1`` returns the data unchanged.
    """
    if folds <= 1:
        return data

    def _in_fold(protein: str) -> bool:
        h = hashlib.blake2b(f"{seed}:{protein}".encode(), digest_size=8).digest()
        return (int.from_bytes(h, "big") % folds) == fold

    orig_counts = (len(data.nk), len(data.lk), len(data.pk))
    new_data = type(data)(
        nk={k: v for k, v in data.nk.items() if _in_fold(k)},
        lk={k: v for k, v in data.lk.items() if _in_fold(k)},
        pk={k: v for k, v in data.pk.items() if _in_fold(k)},
        pk_known={k: v for k, v in data.pk_known.items() if _in_fold(k)},
        known={k: v for k, v in data.known.items() if _in_fold(k)},
    )
    emit(
        "run_cafa_evaluation.gt_restricted_to_fold",
        None,
        {
            "folds": folds,
            "fold": fold,
            "seed": seed,
            "nk_before": orig_counts[0],
            "nk_after": len(new_data.nk),
            "lk_before": orig_counts[1],
            "lk_after": len(new_data.lk),
            "pk_before": orig_counts[2],
            "pk_after": len(new_data.pk),
        },
        "info",
    )
    return new_data


def restrict_data_to_protein_set(
    data: EvaluationData,
    *,
    accessions: Collection[str],
    label: str,
    emit: EmitFn,
) -> EvaluationData:
    """Keep only proteins named in ``accessions``.

    The general form of the fold restriction. A fold is a deterministic hash
    partition and answers "is this protein in slice N"; this answers "is this
    protein in a set someone computed elsewhere", which is what a stratum needs.
    The strata in :mod:`protea.core.strata` are resolved from the retrieved
    neighbourhood (length, homology, taxonomy, propagation gap), and none of
    those are derivable from the accession alone, so the membership has to
    arrive as data rather than as a predicate.

    ``label`` names the stratum in the emitted event. It is required because an
    unlabelled subset restriction produces a result that cannot be told from a
    full-population one afterwards, and this campaign has already lost a grid to
    exactly that.

    An empty ``accessions`` raises. Silently evaluating nothing and reporting a
    score over it is the failure this argument exists to prevent.
    """
    wanted = frozenset(accessions)
    if not wanted:
        raise ValueError(
            f"the protein subset {label!r} is empty, so there is nothing to "
            f"evaluate; an empty restriction would score zero proteins and "
            f"report it as a result"
        )
    orig_counts = (len(data.nk), len(data.lk), len(data.pk))
    new_data = type(data)(
        nk={k: v for k, v in data.nk.items() if k in wanted},
        lk={k: v for k, v in data.lk.items() if k in wanted},
        pk={k: v for k, v in data.pk.items() if k in wanted},
        pk_known={k: v for k, v in data.pk_known.items() if k in wanted},
        known={k: v for k, v in data.known.items() if k in wanted},
    )
    emit(
        "run_cafa_evaluation.gt_restricted_to_subset",
        None,
        {
            "subset": label,
            "requested": len(wanted),
            "nk_before": orig_counts[0],
            "nk_after": len(new_data.nk),
            "lk_before": orig_counts[1],
            "lk_after": len(new_data.lk),
            "pk_before": orig_counts[2],
            "pk_after": len(new_data.pk),
        },
        "info",
    )
    return new_data


def write_terms_of_interest(
    toi_path: str,
    toi_go_ids: list[str],
    *,
    emit: EmitFn,
) -> None:
    """Persist the CAFA6 terms-of-interest file (one GO id per line, sorted).

    cafaeval's ``-toi`` flag restricts evaluation to this set so that
    terms not in the frozen pivot graph (e.g. new since t-1) are
    excluded from scoring.
    """
    with open(toi_path, "w") as f:
        for go_id in sorted(toi_go_ids):
            f.write(f"{go_id}\n")
    emit(
        "run_cafa_evaluation.toi_written",
        None,
        {"toi_terms": len(toi_go_ids), "path": toi_path},
        "info",
    )


def write_ground_truth_files(
    artifacts_root: Any,
    data: EvaluationData,
) -> dict[str, str]:
    """Write the five ``gt_*.tsv`` / ``known_terms.tsv`` files used by cafaeval.

    Returns a dict ``{label → path}`` so the caller can pass each path
    to the per-setting cafaeval driver. ``artifacts_root`` is a
    :class:`pathlib.Path` (or anything convertible via ``str``); kept
    typed as :data:`Any` so the helper does not import ``pathlib``
    just for one signature.
    """
    import os

    from protea.core.operations._run_cafa_artifacts import write_gt

    gt_dir = str(artifacts_root)
    paths = {
        "nk": os.path.join(gt_dir, "gt_NK.tsv"),
        "lk": os.path.join(gt_dir, "gt_LK.tsv"),
        "pk": os.path.join(gt_dir, "gt_PK.tsv"),
        "known": os.path.join(gt_dir, "known_terms.tsv"),
        "pk_known": os.path.join(gt_dir, "pk_known_terms.tsv"),
    }
    write_gt(data.nk, paths["nk"])
    write_gt(data.lk, paths["lk"])
    write_gt(data.pk, paths["pk"])
    write_gt(data.known, paths["known"])
    write_gt(data.pk_known, paths["pk_known"])
    return paths
