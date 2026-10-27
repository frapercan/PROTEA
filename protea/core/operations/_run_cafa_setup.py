"""Setup-phase helpers extracted from ``run_cafa_evaluation``.

Holds the two ``NamedTuple`` bundles threaded through the run pipeline
plus the small functions that load the term universe and emit the
setup events. Living in a sibling keeps the operation file lean and
under the master-plan v3.2 §3 LOC budget.

Re-exported by ``run_cafa_evaluation`` for backwards compatibility.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.evaluation import EvaluationData
from protea.core.operations._run_cafa_eval_driver import CafaEvalRunContext
from protea.core.operations._run_cafa_ia_frame import ia_frame
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
from protea.infrastructure.storage import ArtifactStore

if TYPE_CHECKING:  # pragma: no cover - typing only
    # Imported for the annotation alone. run_cafa_evaluation imports this module,
    # so a runtime import would close a cycle; `from __future__ import
    # annotations` already makes the annotation a string, which is why nothing
    # crashed and only the linter noticed the name was never bound.
    from protea.core.operations.run_cafa_evaluation import RunCafaEvaluationPayload


#: The terms-of-interest rules, by the name a payload asks for them with.
#:
#: ``pivot_terms`` is what the platform has always done and stays the default,
#: so no stored result moves and no frame changes under anyone.
#:
#: ``live_in_both_ends`` is the rule the lab's long window uses, and it differs
#: from the default twice over. It drops the obsolete, which the default keeps:
#: the pivot of the 211 -> 227 window carries 47,328 terms of which 3,770 are
#: obsolete, and cafaeval's own ``obo_parser`` excludes obsolete terms from the
#: graphs it builds, so the default hands the scorer a universe 3,770 terms
#: wider than anything it can score. And it consults BOTH ends rather than one:
#: a term obsolete by t1 is scored by the default as though it still existed.
#: On that window the two rules are 43,558 terms against 39,028.
TOI_RULES: tuple[str, ...] = ("pivot_terms", "live_in_both_ends")


class _EvalInputs(NamedTuple):
    """Validated entities + supporting data shared across the run pipeline."""

    eval_set_id: uuid.UUID
    pred_set_id: uuid.UUID
    eval_set: EvaluationSet
    pred_set: PredictionSet
    data: EvaluationData
    snapshot: OntologySnapshot
    pivot_snapshot_id: uuid.UUID
    toi_go_ids: list[str]
    #: Which rule produced ``toi_go_ids``. Carried so the setup event can name
    #: it: a term universe reported without its rule leaves two runs scoring
    #: different quantities to be told apart by their counts.
    toi_rule: str = TOI_RULES[0]


class _PipelineCtx(NamedTuple):
    """Bundle of run-pipeline inputs threaded through the tempdir block."""

    inputs: _EvalInputs
    scoring_snapshot: ScoringConfig | None
    reranker_models: dict[str, Any]
    result_id: uuid.UUID
    artifact_store: ArtifactStore


def _window_end_snapshots(session: Session, eval_set: EvaluationSet) -> tuple[uuid.UUID, uuid.UUID]:
    """The ontology each end's corpus was loaded under.

    Read from the two annotation sets and NOT from
    ``old_native_snapshot_id`` / ``new_native_snapshot_id``, which look like
    the obvious source and are the wrong one. Those two fields do two jobs:
    they say which graph closes each side, and they are the only other record
    of which ontologies the window spans. Asking for a closure under t0's
    graph alone means setting both of them to t0, which is a legitimate and
    supported request -- and it erases t1. On the 211 -> 227 validation window
    that was done, both fields read ``releases/2022-07-01``, and a rule
    reading them would silently degrade to the single-snapshot case while
    reporting that it had consulted two.

    The annotation sets carry their own ``ontology_snapshot_id``, fixed when
    the corpus was loaded and independent of any later closure choice, so they
    answer the question the fields were overloaded with. On that same window
    they read ``releases/2022-07-01`` and ``releases/2025-07-22``.
    """
    old_set = session.get(AnnotationSet, eval_set.old_annotation_set_id)
    new_set = session.get(AnnotationSet, eval_set.new_annotation_set_id)
    if old_set is None or new_set is None:
        raise ValueError(
            f"EvaluationSet {eval_set.id} names an AnnotationSet that is gone; "
            "the terms of interest cannot be resolved from its two ends"
        )
    return old_set.ontology_snapshot_id, new_set.ontology_snapshot_id


def _live_go_ids(session: Session, snapshot_id: uuid.UUID) -> set[str]:
    """The non-obsolete GO ids of one snapshot."""
    return {
        gid
        for (gid,) in session.query(GOTerm.go_id)
        .filter(
            GOTerm.ontology_snapshot_id == snapshot_id,
            GOTerm.is_obsolete.is_(False),
        )
        .all()
    }


def _load_terms_of_interest(
    session: Session,
    pivot_snapshot_id: uuid.UUID,
    eval_set: EvaluationSet | None = None,
    rule: str | None = None,
) -> list[str]:
    """The terms cafaeval's ``-toi`` restricts precision and recall to.

    ``pivot_terms`` (the default) returns every GO id under the pivot
    snapshot, obsolete ones included, which is what the platform has always
    sent. ``live_in_both_ends`` returns the ids that are non-obsolete in the
    ontology of BOTH ends of the window; see :data:`TOI_RULES` for what
    separates them and :func:`_window_end_snapshots` for where the two
    ontologies are read from.

    Sorted, so the file the run writes is a function of the rule and the
    window and not of a query plan. The unsorted version was harmless only
    because the consumer happens not to care.
    """
    rule = rule or TOI_RULES[0]
    if rule not in TOI_RULES:
        raise ValueError(f"unknown toi_rule {rule!r}; expected one of {TOI_RULES}")
    if rule == "pivot_terms":
        return sorted(
            gid
            for (gid,) in session.query(GOTerm.go_id)
            .filter(GOTerm.ontology_snapshot_id == pivot_snapshot_id)
            .all()
        )
    if eval_set is None:
        raise ValueError(
            f"toi_rule {rule!r} is a property of the window's two ends, so it "
            "cannot be resolved without the EvaluationSet"
        )
    old_snapshot, new_snapshot = _window_end_snapshots(session, eval_set)
    return sorted(_live_go_ids(session, old_snapshot) & _live_go_ids(session, new_snapshot))


def _emit_evaluation_setup_events(emit: EmitFn, inputs: _EvalInputs) -> None:
    """Fire the ``start`` + ``delta_done`` events the UI gates on.

    Order matters: ``start`` first (so the UI can flip to running)
    then ``delta_done`` once the per-category counts are computed.
    """
    emit(
        "run_cafa_evaluation.start",
        None,
        {
            "evaluation_set_id": str(inputs.eval_set_id),
            "prediction_set_id": str(inputs.pred_set_id),
            "pivot_ontology_snapshot_id": str(inputs.pivot_snapshot_id),
            "mode": (inputs.eval_set.stats or {}).get("mode", "same_snapshot"),
            "obo_url": inputs.snapshot.obo_url,
            # Which rule produced the scored term universe, and how big it
            # came out. Two runs under different rules score different
            # quantities, so a run that does not say which one it used leaves
            # the difference to be guessed from the number.
            "toi_rule": inputs.toi_rule,
            "toi_terms": len(inputs.toi_go_ids),
        },
        "info",
    )
    emit(
        "run_cafa_evaluation.delta_done",
        None,
        {
            "nk_proteins": inputs.data.nk_proteins,
            "lk_proteins": inputs.data.lk_proteins,
            "pk_proteins": inputs.data.pk_proteins,
        },
        "info",
    )


class StagedInputs(NamedTuple):
    """What the staging step produced, named.

    It had no name, so the function that consumes it took eleven arguments and
    the size guard was right to object: a parameter list that long is usually a
    missing noun. These seven values are exactly what staging writes or resolves,
    and nothing else in the pipeline produces them.
    """

    obo_path: str
    ia_path: str | None
    gt_paths: dict[str, str]
    toi_path: str
    data: Any
    delta_proteins: set[str]
    has_rerankers: bool


def bundle_run_context(
    p: RunCafaEvaluationPayload,
    ctx: _PipelineCtx,
    artifacts_root: Path,
    staged: StagedInputs,
    emit: EmitFn,
) -> CafaEvalRunContext:
    """Name what was staged, and nothing else.

    Every field here is a value the staging step already produced, so this
    is the one place where a caller can see the whole cafaeval input surface
    at once, including the three frame markers that have to travel with the
    grid artefact or two runs under different frames publish as a method
    difference.
    """
    inputs = ctx.inputs
    return CafaEvalRunContext(
        pred_set_id=inputs.pred_set_id,
        delta_proteins=staged.delta_proteins,
        max_distance=p.max_distance,
        max_k_position=p.max_k_position,
        max_sequence_rank=p.max_sequence_rank,
        artifacts_root=artifacts_root,
        has_rerankers=staged.has_rerankers,
        reranker_models=ctx.reranker_models,
        scoring_config_snapshot=ctx.scoring_snapshot,
        data=staged.data,
        obo_path=staged.obo_path,
        nk_path=staged.gt_paths["nk"],
        lk_path=staged.gt_paths["lk"],
        pk_path=staged.gt_paths["pk"],
        pk_known_path=staged.gt_paths["pk_known"],
        ia_path=staged.ia_path,
        toi_path=staged.toi_path,
        shared_pred_dir=os.path.join(str(artifacts_root), "predictions"),
        ontology_snapshot_id=str(inputs.snapshot.id),
        evaluation_set_id=str(inputs.eval_set_id),
        information_accretion_frame=ia_frame(p, staged.ia_path, emit),
        th_step=p.th_step,
        max_terms=p.max_terms,
        softprop=p.softprop,
        interpro_graft=p.interpro_graft,
        interpro_protein2ipr_file=p.interpro_protein2ipr_file,
        interpro_ipr2go_file=p.interpro_ipr2go_file,
        interpro_graft_weight=p.interpro_graft_weight,
    )
