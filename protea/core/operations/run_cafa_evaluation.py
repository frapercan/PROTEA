from __future__ import annotations

import os
import tempfile
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from protea.core._artifact_identity import sha256_of_file
from protea.core._evaluation_leakage import refuse_uncertifiable_encoding
from protea.core.contracts.operation import EmitFn, OperationResult
from protea.core.evaluation import load_evaluation_data_for_set
from protea.core.ia import PROPAGATE_RELATIONS, strongly_connected_components
from protea.core.operations import _run_cafa_artifacts as _artifacts
from protea.core.operations import _run_cafa_data_helpers as _data
from protea.core.operations import _run_cafa_summary as _summary
from protea.core.operations._band_guard import refuse_if_the_band_is_foreign
from protea.core.operations._evaluation_artifacts import (
    resolve_ia_file,
    resolve_obo,
)
from protea.core.operations._holdout_guard import Claim, refuse_if_it_reads_the_holdout
from protea.core.operations._method_score_predictions import refuse_unless_scored
from protea.core.operations._run_cafa_eval_driver import (
    CafaEvalRunContext,
    evaluate_all_settings,
)

# Re-exports for backwards compatibility with existing imports.
# Helpers live in ``_run_cafa_helpers`` so this file can stay close
# to the master-plan v3.2 §3 LOC ceiling (<800).
from protea.core.operations._run_cafa_helpers import (  # noqa: F401
    _NS_LABELS,
    _NS_SHORT,
    _NUMERIC_ORM_COLS,
    ResultRow,
    _patch_query_known_features,
    _record_from_pred,
    artifacts_record,
    build_result_row,
    eval_artifact_key,
)
from protea.core.operations._run_cafa_payload import RunCafaEvaluationPayload
from protea.core.operations._run_cafa_scale import read_and_report
from protea.core.operations._run_cafa_setup import (  # noqa: F401
    StagedInputs,
    _emit_evaluation_setup_events,
    _EvalInputs,
    _PipelineCtx,
    bundle_run_context,
    population_record,
    terms_of_interest_record,
    window_terms_of_interest,
)
from protea.core.operations.seal_evaluation_frames import seal_one
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import ArtifactStore, get_artifact_store

#: The keys of a result that are not settings: what was uploaded, and the terms
#: and the population the settings were scored over.
_NOT_SETTINGS: tuple[str, ...] = ("artifacts", "terms_of_interest", "population")

#: The leakage role a result takes from its window's role when the request names
#: none. A frame's fitting (``training``) and selection (``validation``) windows
#: feed selection, as ADR-D40's ``valid`` does; ADR-D40's ``test`` is a test. A
#: role not listed derives nothing, and the result is left unstamped.
DERIVED_LEAKAGE_ROLES: dict[str, str] = {
    "valid": "select",
    "test": "test",
    "training": "select",
    "validation": "select",
}


def _refuse_a_cyclic_snapshot(session: Session, snapshot: Any, emit: EmitFn) -> None:
    """Refuse a snapshot whose True Path Rule edges close a cycle, by name.

    cafaeval cannot score one. ``Graph.top_sort`` is Kahn's algorithm and raises
    ``Exception("The sparse matrix doesn't represent an acyclic graph")`` from
    inside ``Graph.__init__``, so the run dies while parsing the OBO, with a
    message that names neither the snapshot nor a term. Reproduced on the real
    file: ``obo_parser`` on ``releases/2024-06-10`` raises exactly that.

    This says which snapshot and which terms, and says it BEFORE a 31 MB
    download and a ground-truth write. It does not fix anything: condensing the
    cycle for scoring is a separate decision, and until it is taken a cyclic
    snapshot has no score. What it stops is an operator reading an opaque
    exception and looking for the fault in the predictions.

    The check runs against the relationship table rather than the OBO because
    the answer is the same and the table is already indexed by snapshot: no
    parse, no download. ``compute_information_accretion`` CONDENSES the same
    cycles rather than refusing them, because an IA over a condensed component
    is well defined (PR #1011) while a cafaeval traversal order is not yet.
    """
    components = strongly_connected_components(_parent_edges(session, snapshot.id))
    if not components:
        return
    terms = sorted(t for comp in components for t in comp)
    emit(
        "run_cafa_evaluation.cyclic_snapshot",
        None,
        {
            "snapshot_id": str(snapshot.id),
            "obo_version": snapshot.obo_version,
            "components": len(components),
            "terms": terms,
        },
        "error",
    )
    raise ValueError(
        f"ontology snapshot {snapshot.id} ({snapshot.obo_version}) has "
        f"{len(components)} True Path Rule cycle(s) over {len(terms)} terms "
        f"({', '.join(terms[:8])}); cafaeval's top_sort cannot order it, so "
        f"this transition cannot be scored. The IA of the same snapshot CAN be "
        f"computed: compute_information_accretion condenses the cycles."
    )


def _parent_edges(session: Session, snapshot_id: Any) -> dict[str, set[str]]:
    """``child -> {parent}`` over the True Path Rule edges of one snapshot."""
    rows = session.execute(
        text(
            "select c.go_id, p.go_id "
            "from go_term_relationship r "
            "join go_term c on c.id = r.child_go_term_id "
            "join go_term p on p.id = r.parent_go_term_id "
            "where r.ontology_snapshot_id = :s and r.relation_type in :rels"
        ).bindparams(bindparam("rels", expanding=True)),
        {"s": snapshot_id, "rels": list(PROPAGATE_RELATIONS)},
    )
    parents: dict[str, set[str]] = defaultdict(set)
    for child, parent in rows:
        parents[child].add(parent)
    return dict(parents)


class RunCafaEvaluationOperation:
    """Runs the CAFA evaluator against NK, LK and PK settings.

    It scores what a method stored and nothing else: the prediction set's
    ``method_score``, read as it is. Scoring candidates, reranking them,
    cutting them to a depth or filtering them is the method's work, done when
    it predicts (``score_prediction_set``, a reranker at prediction time) and
    written in its record; a set that stores candidates without scores is
    refused before anything is staged.

    Steps:

    1. Load ``EvaluationSet`` and ``PredictionSet`` from DB.
    2. Compute evaluation data (delta NK/LK + known terms) with full
       NOT propagation.
    3. Download the OBO file from the ontology snapshot URL.
    4. Resolve the Information Accretion (IA) file: if ``ia_file`` is set
       in the payload, use that path directly; otherwise, if the
       ``OntologySnapshot`` has an ``ia_url``, download it to a temporary
       file; if neither is set, cafaeval runs with uniform IC=1. IA weights
       make rare GO terms count more than common ones and are recommended
       for publishable evaluations.
    5. Write temp files: ground-truth NK/LK, known-terms, and the method's
       stored scores (CAFA format).
    6. Call ``cafa_eval`` for each setting (NK, LK, PK).
    7. Parse per-namespace Fmax / precision / recall / coverage from
       results.
    8. Persist an ``EvaluationResult`` row with all metrics.
    """

    name = "run_cafa_evaluation"
    description = (
        "Run the CAFA evaluator (NK/LK/PK) against an EvaluationSet using a "
        "PredictionSet, optionally weighted by Information Accretion."
    )

    def summarize_payload(self, payload: dict[str, Any], *, session: Session | None = None) -> str:
        return _summary.summarize_payload(payload or {}, session)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = RunCafaEvaluationPayload.model_validate(contract_payload(payload))
        inputs = self._load_evaluation_inputs(session, p, emit)
        self._stamp_window_role(inputs.eval_set, p.window_role, emit)
        # Pre-generate result_id so the artifact-store prefix matches the DB row.
        result_id = uuid.uuid4()
        artifact_store = get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))
        ctx = _PipelineCtx(inputs=inputs, result_id=result_id, artifact_store=artifact_store)
        results = self._run_evaluation_pipeline(session, p, ctx, emit)
        provenance = self._build_eval_provenance(p, inputs.eval_set)
        eval_result = build_result_row(
            ResultRow(
                result_id=result_id,
                inputs=inputs,
                payload=payload,
                results=results,
                provenance=provenance,
            )
        )
        session.add(eval_result)
        session.flush()
        frame_seal = seal_one(session, result_id, emit)
        emit(
            "run_cafa_evaluation.done",
            None,
            {
                "evaluation_result_id": str(result_id),
                "frame_digest": frame_seal,
                "settings_evaluated": [k for k in results if k not in _NOT_SETTINGS],
                "artifacts_prefix": f"eval_artifacts/{result_id}/",
                "artifacts_count": len((results.get("artifacts") or {}).get("keys") or []),
            },
            "info",
        )
        return OperationResult(result={"evaluation_result_id": str(result_id), "results": results})

    @staticmethod
    def _load_evaluation_inputs(
        session: Session, p: RunCafaEvaluationPayload, emit: EmitFn
    ) -> _EvalInputs:
        """Validate eval/pred set FKs, compute the delta data, resolve the
        pivot snapshot + terms-of-interest, and emit the start / delta_done
        events that downstream listeners use to gate progress UI."""
        eval_set_id = uuid.UUID(p.evaluation_set_id)
        pred_set_id = uuid.UUID(p.prediction_set_id)
        eval_set = session.get(EvaluationSet, eval_set_id)
        if eval_set is None:
            raise ValueError(f"EvaluationSet {eval_set_id} not found")
        pred_set = session.get(PredictionSet, pred_set_id)
        if pred_set is None:
            raise ValueError(f"PredictionSet {pred_set_id} not found")
        refuse_if_it_reads_the_holdout(
            session,
            eval_set.new_annotation_set_id,
            claim=Claim(p.holdout_waiver, p.prospective_declaration),
            old_annotation_set_id=eval_set.old_annotation_set_id,
            context="scoring at",
            emit=emit,
        )
        refuse_uncertifiable_encoding(session, pred_set, eval_set)
        # Before anything is staged: the evaluator reads stored scores, so a
        # set of candidates without them is refused by name, with the step
        # that scores them, rather than failing deep in the writer.
        refuse_unless_scored(session, pred_set_id)

        emit("run_cafa_evaluation.computing_delta", None, {}, "info")
        data, pivot_snapshot_id = load_evaluation_data_for_set(session, eval_set)
        snapshot = session.get(OntologySnapshot, pivot_snapshot_id)
        if snapshot is None:
            raise ValueError(f"Pivot OntologySnapshot {pivot_snapshot_id} not found")
        toi_rule, toi_go_ids = window_terms_of_interest(session, eval_set, pivot_snapshot_id)
        inputs = _EvalInputs(
            eval_set_id=eval_set_id,
            pred_set_id=pred_set_id,
            eval_set=eval_set,
            pred_set=pred_set,
            data=data,
            snapshot=snapshot,
            pivot_snapshot_id=pivot_snapshot_id,
            toi_go_ids=toi_go_ids,
            toi_rule=toi_rule,
        )
        _emit_evaluation_setup_events(emit, inputs)
        if data.delta_proteins == 0:
            raise ValueError("No delta proteins found; cannot evaluate")
        return inputs

    @staticmethod
    def _stamp_window_role(eval_set: EvaluationSet, window_role: str | None, emit: EmitFn) -> None:
        """Stamp ``window_role`` onto the EvaluationSet when the payload carries
        one and the set is not already bound. Non-destructive: an existing
        window_role is never overwritten (use generate_evaluation_set's rebind
        path for that). Persisted by the no-op commit in the eval pipeline."""
        if window_role is None or eval_set.window_role is not None:
            return
        eval_set.window_role = window_role
        emit(
            "run_cafa_evaluation.window_role_stamped",
            None,
            {"evaluation_set_id": str(eval_set.id), "window_role": window_role},
            "info",
        )

    @staticmethod
    def _build_eval_provenance(
        p: RunCafaEvaluationPayload, eval_set: EvaluationSet
    ) -> tuple[str | None, str | None, str | None, dict[str, bool] | None]:
        """Resolve the four EvaluationResult provenance markers (slice
        F-METHOD-EVAL-SURFACE) from the payload:

        - ``leakage_role``: explicit, else derived from the (now possibly
          stamped) EvaluationSet ``window_role`` (:data:`DERIVED_LEAKAGE_ROLES`:
          the fitting and selection windows of a frame, and ADR-D40's 'valid',
          feed selection; ADR-D40's 'test' is a test).
        - ``arms_enabled``: as the caller labels the method, or None. It is no
          longer derived: the evaluator runs no arm, and what the method ran is
          in its prediction set's record.
        """
        leakage_role = p.leakage_role
        if leakage_role is None:
            leakage_role = DERIVED_LEAKAGE_ROLES.get(eval_set.window_role or "")
        return p.frame, p.temporal_window, leakage_role, p.arms_enabled

    def _run_evaluation_pipeline(
        self,
        session: Session,
        p: RunCafaEvaluationPayload,
        ctx: _PipelineCtx,
        emit: EmitFn,
    ) -> dict[str, Any]:
        """Stage cafaeval inputs in a tempdir, run the per-setting evaluator,
        upload artifacts, return the merged results dict (with ``artifacts``)."""
        with tempfile.TemporaryDirectory(prefix="protea_cafa_") as tmpdir:
            artifacts_root = Path(tmpdir) / "artifacts"
            artifacts_root.mkdir(parents=True, exist_ok=True)
            run_data = self._stage_evaluator_inputs(session, p, ctx, artifacts_root, emit)
            # No-op commit: releases the DB connection back to the pool before
            # cafaeval forks worker processes via multiprocessing.Pool. Forked
            # children would otherwise inherit SQLAlchemy connection-pool
            # locks, causing an indefinite deadlock on first use. commit()
            # (vs session.close()) keeps ORM objects so BaseWorker can still
            # update job.status after execute().
            session.commit()
            results = evaluate_all_settings(ctx=run_data, emit=emit)
            uploaded = self._upload_artifacts(
                ctx.artifact_store, ctx.result_id, artifacts_root, emit
            )
            results["artifacts"] = artifacts_record(uploaded)
            results["terms_of_interest"] = terms_of_interest_record(
                ctx.inputs.toi_rule, ctx.inputs.toi_go_ids, run_data.toi_path
            )
            results["population"] = population_record(ctx.inputs.data)
            # How the scores were read before cafaeval thresholded them (ADR-D55,
            # section 6) and on which grid: two results of one scored set can
            # differ in these alone, and the record has to say which is which.
            results["reading"] = {"score_reading": p.score_reading, "th_step": p.th_step}
            return results

    def _stage_evaluator_inputs(
        self,
        session: Session,
        p: RunCafaEvaluationPayload,
        ctx: _PipelineCtx,
        artifacts_root: Path,
        emit: EmitFn,
    ) -> CafaEvalRunContext:
        """Download OBO and IA, restrict the ground truth, write its files.

        The bundling is separate. The docstring used to join the two with an
        "and", which is usually where a method has two jobs: staging touches the
        filesystem and the store, and bundling only names what was staged.
        """
        inputs = ctx.inputs
        _refuse_a_cyclic_snapshot(session, inputs.snapshot, emit)
        tmpdir = str(artifacts_root.parent)
        obo_path = resolve_obo(tmpdir, inputs.snapshot, emit)
        ia_path = self._resolve_and_gate_ia(tmpdir, inputs.snapshot, p, emit, session)
        data = inputs.data
        gt_paths = _data.write_ground_truth_files(artifacts_root, data)
        toi_path = self._write_terms_of_interest(artifacts_root, inputs.toi_go_ids, emit)

        delta_proteins = set(data.nk) | set(data.lk) | set(data.pk)
        emit(
            "run_cafa_evaluation.writing_predictions",
            None,
            {"delta_proteins": len(delta_proteins)},
            "info",
        )
        self._write_shared_prediction_file(session, ctx, artifacts_root, delta_proteins)
        # Read once, here: every setting reads this file.
        read_and_report(
            os.path.join(str(artifacts_root), "predictions", "predictions.tsv"),
            reading=p.score_reading,
            th_step=p.th_step,
            label="shared",
            emit=emit,
        )
        return bundle_run_context(
            p,
            ctx,
            artifacts_root,
            StagedInputs(obo_path, ia_path, gt_paths, toi_path),
            emit,
        )

    @staticmethod
    def _write_terms_of_interest(artifacts_root: Path, toi_go_ids: list[str], emit: EmitFn) -> str:
        """Write the window's terms of interest where cafaeval and the artifacts read them."""
        toi_path = os.path.join(str(artifacts_root), "terms_of_interest.txt")
        _data.write_terms_of_interest(toi_path, toi_go_ids, emit=emit)
        return toi_path

    @staticmethod
    def _write_shared_prediction_file(
        session: Session,
        ctx: _PipelineCtx,
        artifacts_root: Path,
        delta_proteins: set[str],
    ) -> None:
        """Write the one predictions.tsv every setting reads: the stored scores."""
        pred_dir = os.path.join(str(artifacts_root), "predictions")
        os.makedirs(pred_dir, exist_ok=True)
        _artifacts.write_predictions(
            session,
            _artifacts.WritePredictionsContext(
                pred_set_id=ctx.inputs.pred_set_id,
                delta_proteins=delta_proteins,
                path=os.path.join(pred_dir, "predictions.tsv"),
            ),
        )

    @staticmethod
    def _resolve_and_gate_ia(
        tmpdir: str,
        snapshot: OntologySnapshot,
        p: RunCafaEvaluationPayload,
        emit: EmitFn,
        session: Session | None = None,
    ) -> str | None:
        """Resolve the IA table and apply the band guard to it, in that order."""
        ia_path = resolve_ia_file(
            tmpdir,
            snapshot,
            p.ia_file,
            emit,
            session,
            p.information_accretion_set_id,
        )
        # The guard gets the SOURCE, not the tempfile: a downloaded copy is
        # always called ia.tsv and would tokenise to that. A set pinned by id is
        # checked by the content sha256 its row records (the digest
        # resolve_ia_from_set has just verified the bytes against), and its
        # canonical key, which still resolves the rows pinned before the digest.
        refuse_if_the_band_is_foreign(
            p.band,
            snapshot,
            p.ia_file,
            emit,
            session=session,
            ia_set_id=p.information_accretion_set_id,
        )
        return ia_path

    @staticmethod
    def _upload_artifacts(
        artifact_store: ArtifactStore,
        result_id: uuid.UUID,
        artifacts_root: Path,
        emit: EmitFn,
    ) -> dict[str, str]:
        """Upload every file under ``artifacts_root`` to the result's prefix.

        Returns each key with the sha256 of the file uploaded under it, read
        from the file before the upload, so the record names the bytes written.
        """
        uploaded: dict[str, str] = {}
        for path in sorted(artifacts_root.rglob("*")):
            if not path.is_file():
                continue
            relpath = path.relative_to(artifacts_root).as_posix()
            key = eval_artifact_key(result_id, relpath)
            uploaded[key] = sha256_of_file(path)
            artifact_store.put(key, path)
        emit(
            "run_cafa_evaluation.artifacts_uploaded",
            None,
            {"count": len(uploaded), "prefix": f"eval_artifacts/{result_id}/"},
            "info",
        )
        return uploaded

    # Backwards-compat shims: tests patch and call these names. The bodies
    # live in ``_run_cafa_artifacts``; these instance methods delegate so
    # ``mock.patch.object(op, "_download_obo")`` and direct
    # ``self.op._download_obo(...)`` invocations continue to work.

    def _download_obo(self, url: str, dest: str) -> None:
        _artifacts.download_obo(url, dest)

    def _download_tsv(self, url: str, dest: str) -> None:
        _artifacts.download_tsv(url, dest)

    def _write_gt(self, annotations: dict[str, set[str]], path: str) -> None:
        _artifacts.write_gt(annotations, path)

    def _write_predictions(self, *args: Any, **kwargs: Any) -> None:
        _artifacts.write_predictions(*args, **kwargs)

    def _parse_results(self, dfs_best: dict) -> dict[str, Any]:
        return _artifacts.parse_results(dfs_best)
