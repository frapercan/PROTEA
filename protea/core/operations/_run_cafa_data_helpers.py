"""The files the evaluator writes for cafaeval: the window's truth and its terms of interest.

The evaluator scores the window's whole population: every target of every
category, an unpredicted one as a failure, with coverage beside it, as LAFA
scores (decided on 2026-11-20). Until then this module also restricted the
truth to the proteins a prediction set had predicted, by default, behind a
gate on how much it dropped, and held two more restrictions, by hash fold and
by protein set, whose callers had gone. The default read the predicted proteins
from ``go_prediction`` only, so from #1246 on it found none in a scored set,
whose scores live in ``prediction_score``, dropped every target, and refused
the evaluation.
"""

from __future__ import annotations

from typing import Any

from protea.core.contracts.operation import EmitFn
from protea.core.evaluation import EvaluationData


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
