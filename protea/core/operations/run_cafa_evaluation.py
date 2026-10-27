from __future__ import annotations

import os
import tempfile
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from protea.core._evaluation_leakage import refuse_uncertifiable_encoding
from protea.core.contracts.operation import EmitFn, OperationResult
from protea.core.evaluation import load_evaluation_data_for_set
from protea.core.ia import PROPAGATE_RELATIONS, strongly_connected_components
from protea.core.operations import _run_cafa_artifacts as _artifacts
from protea.core.operations import _run_cafa_data_helpers as _data
from protea.core.operations import _run_cafa_summary as _summary
from protea.core.operations._band_guard import refuse_if_the_band_is_foreign
from protea.core.operations._depth_unit_guard import (
    assert_depth_unit_is_available,
)
from protea.core.operations._evaluation_artifacts import (
    resolve_ia_file,
    resolve_obo,
)
from protea.core.operations._holdout_guard import refuse_if_it_reads_the_holdout
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
    build_result_row,
    eval_artifact_key,
)
from protea.core.operations._run_cafa_payload import RunCafaEvaluationPayload
from protea.core.operations._run_cafa_reranker_loader import (
    load_reranker_models_for_payload,
)
from protea.core.operations._run_cafa_setup import (  # noqa: F401
    StagedInputs,
    _emit_evaluation_setup_events,
    _EvalInputs,
    _load_terms_of_interest,
    _PipelineCtx,
    bundle_run_context,
)
from protea.core.operations.compute_information_accretion import ia_key_for
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.embedding.scoring_config import ScoringConfig
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import ArtifactStore, get_artifact_store


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
    5. Write temp files: ground-truth NK/LK, known-terms, predictions
       (CAFA format).
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
        scoring_snapshot = self._resolve_scoring_snapshot(session, p.scoring_config_id)
        reranker_models, reranker_config_snapshot = load_reranker_models_for_payload(
            session,
            rerankers_nested=p.rerankers,
            reranker_id_nk=p.reranker_id_nk,
            reranker_id_lk=p.reranker_id_lk,
            reranker_id_pk=p.reranker_id_pk,
            emit=emit,
        )
        # Pre-generate result_id so the artifact-store prefix matches the DB row.
        result_id = uuid.uuid4()
        artifact_store = get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))
        ctx = _PipelineCtx(
            inputs=inputs,
            scoring_snapshot=scoring_snapshot,
            reranker_models=reranker_models,
            result_id=result_id,
            artifact_store=artifact_store,
        )
        results = self._run_evaluation_pipeline(session, p, ctx, emit)
        first_reranker_id, reranker_config_snapshot = self._finalize_reranker_config(
            reranker_config_snapshot, reranker_models, p
        )
        provenance = self._build_eval_provenance(p, inputs.eval_set, bool(reranker_models))
        eval_result = build_result_row(
            ResultRow(
                result_id=result_id,
                inputs=inputs,
                payload=payload,
                scoring_config_id=p.scoring_config_id,
                first_reranker_id=first_reranker_id,
                reranker_config_snapshot=reranker_config_snapshot,
                results=results,
                provenance=provenance,
                max_sequence_rank=p.max_sequence_rank,
                max_k_position=p.max_k_position,
            )
        )
        session.add(eval_result)
        session.flush()
        emit(
            "run_cafa_evaluation.done",
            None,
            {
                "evaluation_result_id": str(result_id),
                "settings_evaluated": [k for k in results.keys() if k != "artifacts"],
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
            waiver=p.holdout_waiver,
            context="scoring at",
            emit=emit,
        )
        refuse_uncertifiable_encoding(session, pred_set, eval_set)
        # Before anything is staged or scored: a sequence depth asked of
        # candidates that carry no sequence rank selects no rows at all,
        # because NULL <= 2 is null and null is not true, so the run would
        # score an empty set and report success.
        assert_depth_unit_is_available(
            session,
            pred_set_id,
            max_sequence_rank=p.max_sequence_rank,
            max_k_position=p.max_k_position,
        )

        emit("run_cafa_evaluation.computing_delta", None, {}, "info")
        data, pivot_snapshot_id = load_evaluation_data_for_set(session, eval_set)
        snapshot = session.get(OntologySnapshot, pivot_snapshot_id)
        if snapshot is None:
            raise ValueError(f"Pivot OntologySnapshot {pivot_snapshot_id} not found")
        inputs = _EvalInputs(
            eval_set_id=eval_set_id,
            pred_set_id=pred_set_id,
            eval_set=eval_set,
            pred_set=pred_set,
            data=data,
            snapshot=snapshot,
            pivot_snapshot_id=pivot_snapshot_id,
            toi_go_ids=_load_terms_of_interest(session, pivot_snapshot_id, eval_set, p.toi_rule),
            toi_rule=p.toi_rule,
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
        p: RunCafaEvaluationPayload, eval_set: EvaluationSet, has_rerankers: bool
    ) -> tuple[str | None, str | None, str | None, dict[str, bool] | None]:
        """Resolve the four EvaluationResult provenance markers (slice
        F-METHOD-EVAL-SURFACE) from the payload, with safe derivations so a
        caller only has to pass ``window_role`` to get a self-describing row:

        - ``leakage_role``: explicit, else derived from the (now possibly
          stamped) EvaluationSet ``window_role`` ('valid'->'select',
          'test'->'test').
        - ``arms_enabled``: explicit, else derived from the run (KNN always on;
          reranker on when a reranker model was supplied; ``interpro_graft`` on
          when the payload opted into the InterPro BP graft and the run had
          rerankers, the only path where the graft actually applies).
        """
        leakage_role = p.leakage_role
        if leakage_role is None:
            leakage_role = {"valid": "select", "test": "test"}.get(eval_set.window_role or "")
            leakage_role = leakage_role or None
        arms_enabled = p.arms_enabled
        if arms_enabled is None:
            arms_enabled = {
                "knn": True,
                "reranker": has_rerankers,
                "mlp_tower": False,
                "interpro": False,
                # The InterPro BP graft (PR #700) only runs in the per-setting
                # reranker path; without rerankers the arm is skipped with a
                # warning. Record it as on only when the payload opted in AND the
                # run had rerankers, so a grafted number is not mislabelled as
                # interpro_graft:false.
                "interpro_graft": bool(p.interpro_graft and has_rerankers),
            }
        return p.frame, p.temporal_window, leakage_role, arms_enabled

    @staticmethod
    def _resolve_scoring_snapshot(
        session: Session, scoring_config_id: str | None
    ) -> ScoringConfig | None:
        """Materialise an in-memory snapshot of the ScoringConfig before the
        no-op commit so cafaeval workers don't need a session."""
        if not scoring_config_id:
            return None
        sc = session.get(ScoringConfig, uuid.UUID(scoring_config_id))
        if sc is None:
            raise ValueError(f"ScoringConfig {scoring_config_id} not found")
        return ScoringConfig(
            formula=sc.formula,
            weights=dict(sc.weights),
            params=dict(sc.params) if sc.params else None,
        )

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
            results = evaluate_all_settings(session, ctx=run_data, emit=emit)
            uploaded_keys = self._upload_artifacts(
                ctx.artifact_store, ctx.result_id, artifacts_root, emit
            )
            results["artifacts"] = {"keys": uploaded_keys}
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
        if p.restrict_gt_to_predicted:
            data = _data.restrict_data_to_predicted(
                session, prediction_set_id=inputs.pred_set_id, data=data, emit=emit
            )
        gt_paths = _data.write_ground_truth_files(artifacts_root, data)
        toi_path = self._resolve_toi_file(artifacts_root, p, inputs.toi_go_ids, emit)

        delta_proteins = set(data.nk) | set(data.lk) | set(data.pk)
        emit(
            "run_cafa_evaluation.writing_predictions",
            None,
            {"delta_proteins": len(delta_proteins)},
            "info",
        )
        has_rerankers = bool(ctx.reranker_models)
        if not has_rerankers:
            self._write_shared_prediction_file(session, p, ctx, artifacts_root, delta_proteins)
        return bundle_run_context(
            p,
            ctx,
            artifacts_root,
            StagedInputs(
                obo_path, ia_path, gt_paths, toi_path, data, delta_proteins, has_rerankers
            ),
            emit,
        )

    @staticmethod
    def _resolve_toi_file(
        artifacts_root: Path,
        p: RunCafaEvaluationPayload,
        toi_go_ids: list[str],
        emit: EmitFn,
    ) -> str:
        """Resolve the cafaeval terms-of-interest path.

        When ``p.toi_file`` is set, use that exact file (the LAFA-parity
        path: score against LAFA's release-specific TOI). Otherwise write
        the pivot-snapshot term universe. See docs/EVAL_LAFA_PARITY.md.
        """
        if p.toi_file:
            emit("run_cafa_evaluation.toi_external", None, {"path": p.toi_file}, "info")
            return p.toi_file
        toi_path = os.path.join(str(artifacts_root), "terms_of_interest.txt")
        _data.write_terms_of_interest(toi_path, toi_go_ids, emit=emit)
        return toi_path

    @staticmethod
    def _write_shared_prediction_file(
        session: Session,
        p: RunCafaEvaluationPayload,
        ctx: _PipelineCtx,
        artifacts_root: Path,
        delta_proteins: set[str],
    ) -> None:
        """Write the single shared predictions.tsv (no-rerankers path)."""
        pred_dir = os.path.join(str(artifacts_root), "predictions")
        os.makedirs(pred_dir, exist_ok=True)
        _artifacts.write_predictions(
            session,
            _artifacts.WritePredictionsContext(
                pred_set_id=ctx.inputs.pred_set_id,
                delta_proteins=delta_proteins,
                max_distance=p.max_distance,
                max_k_position=p.max_k_position,
                max_sequence_rank=p.max_sequence_rank,
                path=os.path.join(pred_dir, "predictions.tsv"),
            ),
            scoring_config=ctx.scoring_snapshot,
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
        # The guard gets the SOURCE reference, not the tempfile: a downloaded
        # copy is always called ia.tsv and would tokenise to that. When the IA
        # was pinned by set id, the canonical key for that set is the reference,
        # and ia_key_for is the same builder compute_information_accretion wrote
        # it with, so the two cannot drift.
        band_ref = (
            ia_key_for(p.information_accretion_set_id)
            if p.information_accretion_set_id
            else p.ia_file
        )
        refuse_if_the_band_is_foreign(p.band, snapshot, band_ref, emit)
        return ia_path

    @staticmethod
    def _upload_artifacts(
        artifact_store: ArtifactStore,
        result_id: uuid.UUID,
        artifacts_root: Path,
        emit: EmitFn,
    ) -> list[str]:
        """Walk ``artifacts_root``, upload every file under the
        ``eval_artifacts/{result_id}/`` prefix, return the list of keys."""
        uploaded_keys: list[str] = []
        for path in sorted(artifacts_root.rglob("*")):
            if not path.is_file():
                continue
            relpath = path.relative_to(artifacts_root).as_posix()
            key = eval_artifact_key(result_id, relpath)
            artifact_store.put(key, path)
            uploaded_keys.append(key)
        emit(
            "run_cafa_evaluation.artifacts_uploaded",
            None,
            {"count": len(uploaded_keys), "prefix": f"eval_artifacts/{result_id}/"},
            "info",
        )
        return uploaded_keys

    @staticmethod
    def _finalize_reranker_config(
        reranker_config_snapshot: dict[str, Any] | None,
        reranker_models: dict[str, Any],
        p: RunCafaEvaluationPayload,
    ) -> tuple[uuid.UUID | None, dict[str, Any] | None]:
        """Pick the representative ``reranker_model_id`` for backwards-compat
        and synthesise the ``reranker_config`` snapshot from the flat
        per-category fields when the nested mapping is empty."""
        first_reranker_id: uuid.UUID | None = None
        if reranker_config_snapshot:
            for _cat_map in reranker_config_snapshot.values():
                for _rid_str in _cat_map.values():
                    first_reranker_id = uuid.UUID(_rid_str)
                    break
                if first_reranker_id:
                    break
            return first_reranker_id, reranker_config_snapshot
        if reranker_models:
            reranker_config_snapshot = {}
            for setting, field in [
                ("nk", p.reranker_id_nk),
                ("lk", p.reranker_id_lk),
                ("pk", p.reranker_id_pk),
            ]:
                if field:
                    reranker_config_snapshot[setting] = {"all": field}
                    if first_reranker_id is None:
                        first_reranker_id = uuid.UUID(field)
        return first_reranker_id, reranker_config_snapshot

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

    def _write_predictions_reranked(self, *args: Any, **kwargs: Any) -> None:
        _artifacts.write_predictions_reranked(*args, **kwargs)

    def _write_predictions_per_aspect(self, *args: Any, **kwargs: Any) -> None:
        _artifacts.write_predictions_per_aspect(*args, **kwargs)

    def _parse_results(self, dfs_best: dict) -> dict[str, Any]:
        return _artifacts.parse_results(dfs_best)
