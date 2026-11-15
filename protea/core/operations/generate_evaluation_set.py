from __future__ import annotations

import tempfile
import uuid
from pathlib import Path
from typing import Any, NamedTuple

from pydantic import field_validator, model_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core._artifact_identity import sha256_of_file
from protea.core.code_revision import (
    code_revision,
    is_identifying,
    revisions_conflict,
)
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.evaluation import (
    GAIN_RULES,
    NOT_CODE_RULES,
    TRUTH_PROFILES,
    ReconciledWindow,
    compute_evaluation_data,
    compute_evaluation_data_reconciled,
    groundtruth_key_for,
    serialize_evaluation_data_to_parquet,
)
from protea.core.operations._holdout_guard import Claim, refuse_if_the_set_reads_the_holdout
from protea.core.utils import contract_payload, job_id_from_payload
from protea.core.window_declarations import (
    SAME_SNAPSHOT_RULES,
    WINDOW_ROLES,
    TruthRules,
    WindowGraphs,
    closing_graphs_by_default,
    closure_applied,
    named_truth_rules,
    payload_with_named_rules,
    refuse_unless_named_rules_apply,
)
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store


class _Identity(NamedTuple):
    """The seven fields that decide which delta a row holds.

    Kept as one object because they travel together: every lookup, every
    refusal and every insert needs all of them, and a signature that took
    them loose is a signature somebody can call with four.

    ``truth_profile`` is the sixth and the only one that is not an id. It
    belongs here rather than beside ``window_role`` because it changes the
    delta: under ``projected`` a t1 term with no node at t0 becomes its first
    live ancestors instead of being dropped. Two profiles over the same five
    ids are two different truths and coexist, which is why the database key
    holds it too.
    """

    old_set_id: uuid.UUID
    new_set_id: uuid.UUID
    pivot_id: uuid.UUID
    old_native: uuid.UUID
    new_native: uuid.UUID
    truth_profile: str = TRUTH_PROFILES[0]
    toi_rule: str = GAIN_RULES[0]
    not_codes: str = NOT_CODE_RULES[0]


class GenerateEvaluationSetPayload(ProteaPayload, frozen=True):
    old_annotation_set_id: str
    new_annotation_set_id: str
    pivot_ontology_snapshot_id: str | None = None
    # Cross-OBO override: propagate each side's annotations under an EXPLICIT
    # ontology snapshot (its native DAG), decoupled from the annotation set's
    # stored ``ontology_snapshot_id``. The reconcile already resolves go_id text
    # and loads the native DAG by snapshot id, so these select the propagation
    # graph WITHOUT touching annotation rows. ``None`` falls back to each set's
    # stored snapshot (existing behaviour). Use when an annotation set is bound
    # to a wrong/too-new OBO (the phantom-gap: t0 propagated under a churned
    # graph that marks pre-window experimental annotations as new knowledge).
    old_native_snapshot_id: str | None = None
    new_native_snapshot_id: str | None = None
    # ADR D40: bind the produced set to a rolling-origin protocol window.
    # ``"valid"`` (selection + threshold tuning) | ``"test"`` (report once)
    # | ``None`` (unbound). Defaults to None so existing callers are
    # unaffected. ADR-D60 adds the roles of a frame: ``"training"``,
    # ``"validation"`` and ``"test_spent"``.
    window_role: str | None = None
    # A named set of truth rules (``protea.core.window_declarations``), e.g.
    # ``"october-headline-frame"``. It fills ``truth_profile``, ``toi_rule``
    # and ``not_codes`` and refuses a payload that asks for others, and it
    # fixes the closure: under the October rules both sides close under the
    # graph the t0 corpus was loaded under, so both natives (and with them the
    # pivot) default to that graph. A build that would apply anything else is
    # refused before the delta is computed. None leaves every default as it was.
    truth_rule_set: str | None = None
    # How the t1 side's truth is built. ``as_closed`` (the default) is what
    # every stored window did: a term the pivot cannot name is dropped by the
    # closure. ``projected`` replaces it by its first live ancestors at t0,
    # same aspect, before the binding rule. It is part of the window's
    # identity, so asking for the other profile over the same two corpora
    # builds a second window rather than colliding with the first.
    truth_profile: str = TRUTH_PROFILES[0]
    # Which terms the GAIN may contain. ``pivot_terms`` (the default) is what
    # every stored window did; ``live_in_both_ends`` restricts it before the
    # delta is classified, which is what the lab's frame does. Independent of
    # the profile: the projection decides what a gain CAN be, this decides
    # which gains COUNT.
    toi_rule: str = GAIN_RULES[0]
    # Which NOT annotations may take truth away. ``any`` (the default) is what
    # every stored window did: every NOT row whatever its evidence.
    # ``experimental`` restricts the negation to the thirteen experimental
    # codes, which is the lab's frame. Independent of the other two: they
    # decide what a gain can be and which gains count, this decides which
    # denials bind.
    not_codes: str = NOT_CODE_RULES[0]

    @field_validator("truth_profile")
    @classmethod
    def _known_profile(cls, value: str) -> str:
        if value not in TRUTH_PROFILES:
            raise ValueError(f"unknown truth_profile {value!r}; expected one of {TRUTH_PROFILES}")
        return value

    @field_validator("toi_rule")
    @classmethod
    def _known_gain_rule(cls, value: str) -> str:
        if value not in GAIN_RULES:
            raise ValueError(f"unknown toi_rule {value!r}; expected one of {GAIN_RULES}")
        return value

    @field_validator("not_codes")
    @classmethod
    def _known_not_rule(cls, value: str) -> str:
        if value not in NOT_CODE_RULES:
            raise ValueError(f"unknown not_codes {value!r}; expected one of {NOT_CODE_RULES}")
        return value
    #: The sentence a caller must repeat verbatim to build a window that reads
    #: past the board's mark. Deliberately not a boolean: the holdout is meant
    #: to be scored once, at the end, and a flag that can be set by reflex is a
    #: flag that gets set by reflex. See split_registry.HOLDOUT_WAIVER.
    holdout_waiver: str | None = None
    #: The other sentence: a band BEYOND the spent holdout, which begins at or
    #: after it and therefore cannot be a second look at it. Separate from the
    #: waiver because the waiver says of itself that it is the single pass, and
    #: that pass is written on a row already. Honoured only when the window's
    #: start is at or after the band's end; a straddle is refused by name. See
    #: split_registry.PROSPECTIVE_DECLARATION.
    prospective_declaration: str | None = None

    @field_validator("old_annotation_set_id", "new_annotation_set_id", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @field_validator(
        "pivot_ontology_snapshot_id",
        "old_native_snapshot_id",
        "new_native_snapshot_id",
        mode="before",
    )
    @classmethod
    def snapshot_opt_non_empty(cls, v):
        if v is None:
            return None
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string or null")
        return v.strip()

    @field_validator("window_role", mode="before")
    @classmethod
    def window_role_in_vocab(cls, v):
        if v is None:
            return None
        if not isinstance(v, str) or v not in WINDOW_ROLES:
            raise ValueError(f"must be one of {WINDOW_ROLES}, or null")
        return v

    @model_validator(mode="before")
    @classmethod
    def _fill_from_named_rule_set(cls, data: Any) -> Any:
        """Fill the three rule fields from ``truth_rule_set`` before they validate.

        Before and not after, because after validation a field left out and a
        field sent with its default look the same, and only the first may be
        filled from the set.
        """
        return payload_with_named_rules(data) if isinstance(data, dict) else data


def _reuse_verdict(served: str | None) -> tuple[str, str]:
    """``(level, note)`` for serving a row built by ``served``.

    WHY IT EXISTS. The eight identity fields say what the delta is by
    DECLARATION and the code says it by IMPLEMENTATION, and they are
    different quantities: on 2026-11-07 a fix to the synonym map changed the
    delta of a whole class of windows with every identity field untouched. So
    a re-submission after that fix got the OLD truth back under an ``info``
    event, and the only way to notice was to go and look. ``3bd720c9`` is
    such a row today.

    ``revisions_conflict`` and never ``!=``: it returns False when either side
    is unidentifying, which is NOT agreement, and the caller is expected to
    say so out loud. That is why the unknown case gets its own sentence and
    the same ``warning`` level as a known conflict. A dirty tree is
    unidentifying too, so the fleet running out of an edited worktree lands
    here rather than claiming a match.
    """
    running = code_revision()
    if served is None:
        return "warning", (
            "the row served carries no code_revision, so nothing can say whether its "
            "truth was built by the code running now. It predates the column."
        )
    if revisions_conflict(served, running):
        return "warning", (
            f"the row served was built by {served[:12]} and this process is "
            f"{running[:12]}. Its stored truth is that other code's, and the identity "
            "fields cannot tell them apart. Delete it and regenerate if you need this "
            "revision's truth."
        )
    if not is_identifying(served) or not is_identifying(running):
        return "warning", (
            f"one of the two revisions is unidentifying ({served[:12]} served, "
            f"{running[:12]} running), so they cannot be compared. This is not "
            "agreement."
        )
    return "info", "the row served was built by the code running now."


class _Ask(NamedTuple):
    """What a call asks of the row it gets, beside the identity that finds it.

    ``rules`` is what THIS call's build applies, which a served row is compared
    against; ``rule_set`` is the name the payload gave, if any.
    """

    window_role: str | None
    rules: TruthRules
    rule_set: str | None = None


def _rules_applied(
    mode: str, ident: _Identity, ends: tuple[AnnotationSet, AnnotationSet]
) -> TruthRules:
    """The truth rules the path ``mode`` applies to this identity.

    Read from the path and not from the payload. ``same_snapshot`` runs
    ``compute_evaluation_data``, which takes none of the three rules, so the
    request in ``ident`` does not reach it. ``reconciled`` hands all three to
    ``compute_evaluation_data_reconciled``, and its closure is how the three
    resolved graphs relate to the two corpora's own ontologies.
    """
    if mode == "same_snapshot":
        return SAME_SNAPSHOT_RULES
    old_set, new_set = ends
    graphs = WindowGraphs(
        old_set.ontology_snapshot_id,
        new_set.ontology_snapshot_id,
        ident.old_native,
        ident.new_native,
        ident.pivot_id,
    )
    return TruthRules(ident.truth_profile, ident.toi_rule, ident.not_codes, closure_applied(graphs))


def _declared_rules(
    rule_set: str | None,
    mode: str,
    ident: _Identity,
    ends: tuple[AnnotationSet, AnnotationSet],
    emit: EmitFn,
) -> TruthRules:
    """The rules this build applies, refused or reported when they are not the asked ones.

    A named rule set is a promise, so a build that would break it is refused
    before anything is computed. Three fields sent loose are a request, and the
    same-snapshot path does not read them: that build goes ahead, as it always
    did, and says out loud that the row's rule columns hold a request it did
    not apply. The row's ``truth_rules`` holds what it did.
    """
    applied = _rules_applied(mode, ident, ends)
    refuse_unless_named_rules_apply(rule_set, applied)
    asked = {
        "profile": ident.truth_profile,
        "toi_rule": ident.toi_rule,
        "not_codes": ident.not_codes,
    }
    if asked != {key: value for key, value in applied.as_json().items() if key in asked}:
        emit(
            "generate_evaluation_set.rules_not_applied",
            None,
            {
                "mode": mode,
                "asked": asked,
                "applied": applied.as_json(),
                "note": (
                    "both corpora and every graph are one snapshot, so the same-snapshot "
                    "path ran and it reads none of the three rules"
                ),
            },
            "warning",
        )
    return applied


def _served_rules(row: Any) -> dict[str, str] | None:
    """The stored rules of a served row, or None when it holds none (legacy)."""
    stored = getattr(row, "truth_rules", None)
    return dict(stored) if isinstance(stored, dict) else None


def _served_result(row: EvaluationSet) -> dict[str, Any]:
    """The result a re-submission gets: the served row's id, uri and stats."""
    return {
        "evaluation_set_id": str(row.id),
        "groundtruth_uri": row.groundtruth_uri,
        **dict(row.stats or {}),
    }


def _identity_fields(row_id: uuid.UUID, ident: _Identity) -> dict[str, str]:
    """The ids of a served row's identity, as the reuse event names them."""
    return {
        "evaluation_set_id": str(row_id),
        "old_annotation_set_id": str(ident.old_set_id),
        "new_annotation_set_id": str(ident.new_set_id),
        "pivot_snapshot_id": str(ident.pivot_id),
        "old_native_snapshot_id": str(ident.old_native),
        "new_native_snapshot_id": str(ident.new_native),
    }


def _reuse_rules_note(served: dict[str, str] | None, ask: _Ask) -> str | None:
    """A sentence when the served row's rules are not what this call applies.

    A legacy row is only worth a sentence when a rule set was named: then the
    caller asked for a promise the row cannot show, and a frame will refuse it.
    """
    if served is None:
        if ask.rule_set is None:
            return None
        return (
            f"The row served predates declared truth rules, so it cannot show it was built "
            f"under {ask.rule_set!r}, and a frame will refuse it as legacy."
        )
    if served != ask.rules.as_json():
        return (
            f"The row served declares {served} and this revision applies "
            f"{ask.rules.as_json()} for the same identity."
        )
    return None


class GenerateEvaluationSetOperation:
    """Computes the CAFA evaluation delta between two GOA annotation sets.

    Applies experimental evidence code filtering, NOT-qualifier exclusion with
    GO DAG descendant propagation, and classifies delta proteins into NK/LK.

    Stores an EvaluationSet row with summary statistics.  The actual ground-truth
    rows are computed on-demand by the download endpoints using the same logic.
    """

    name = "generate_evaluation_set"
    description = (
        "Compute the CAFA delta between an old and a new GOA annotation set, "
        "split delta proteins into NK/LK and persist an EvaluationSet. "
        "Supports cross-OBO reconciliation via an optional pivot snapshot."
    )

    def summarize_payload(self, payload: dict[str, Any], *, session: Session | None = None) -> str:
        p = payload or {}
        old_raw = p.get("old_annotation_set_id")
        new_raw = p.get("new_annotation_set_id")
        if old_raw and new_raw and session is not None:
            try:
                old = session.get(AnnotationSet, uuid.UUID(str(old_raw)))
                new = session.get(AnnotationSet, uuid.UUID(str(new_raw)))
            except Exception:
                old = new = None
            if old is not None and new is not None:
                ov = old.source_version or str(old.id)[:8]
                nv = new.source_version or str(new.id)[:8]
                return f"{old.source}@{ov} → {new.source}@{nv}"
        if old_raw and new_raw:
            return f"{str(old_raw)[:8]} → {str(new_raw)[:8]}"
        return ""

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = GenerateEvaluationSetPayload.model_validate(contract_payload(payload))
        old_set_id = uuid.UUID(p.old_annotation_set_id)
        new_set_id = uuid.UUID(p.new_annotation_set_id)
        old_set, new_set, old_native, new_native, pivot_id = self._resolve_eval_inputs(
            session, p, old_set_id, new_set_id
        )
        # Before anything is built: the holdout is scored once, at the end.
        refuse_if_the_set_reads_the_holdout(
            new_set,
            claim=Claim(p.holdout_waiver, p.prospective_declaration),
            old_set=old_set,
            context="building the window ending at",
            emit=emit,
        )
        mode = self._delta_mode(old_set, new_set, old_native, new_native, pivot_id)

        ident = _Identity(
            old_set_id,
            new_set_id,
            pivot_id,
            old_native,
            new_native,
            p.truth_profile,
            p.toi_rule,
            p.not_codes,
        )
        # Before the reuse lookup, so a served row is compared with these.
        applied = _declared_rules(p.truth_rule_set, mode, ident, (old_set, new_set), emit)
        ask = _Ask(p.window_role, applied, p.truth_rule_set)
        short = self._maybe_reuse_existing(session, ident, ask, emit)
        if short is not None:
            return short

        data, stats = self._compute_delta(session, ident, mode, emit)
        self._refuse_empty_delta(session, ident, stats)

        eval_set = self._new_row(ident, ask, stats, job_id_from_payload(payload))
        session.add(eval_set)
        session.flush()
        uri = self._persist_groundtruth(eval_set, data, emit)
        result = {"evaluation_set_id": str(eval_set.id), "groundtruth_uri": uri, **stats}
        emit("generate_evaluation_set.done", None, result, "info")
        return OperationResult(result=result)

    @staticmethod
    def _new_row(
        ident: _Identity, ask: _Ask, stats: dict[str, Any], job_id: uuid.UUID | None
    ) -> EvaluationSet:
        """The row this build inserts: its identity, its role and the rules it APPLIED.

        ``truth_rules`` comes from ``ask.rules``, which :func:`_declared_rules`
        read from the path taken, and never from the payload's three fields,
        which are the request and go to the identity columns as before.
        """
        return EvaluationSet(
            old_annotation_set_id=ident.old_set_id,
            new_annotation_set_id=ident.new_set_id,
            pivot_snapshot_id=ident.pivot_id,
            old_native_snapshot_id=ident.old_native,
            new_native_snapshot_id=ident.new_native,
            truth_profile=ident.truth_profile,
            toi_rule=ident.toi_rule,
            not_codes=ident.not_codes,
            stats=stats,
            window_role=ask.window_role,
            truth_rules=ask.rules.as_json(),
            # Stamped at import, not read from the tree now: the fleet runs out
            # of a working tree that is edited between runs, so the tree can
            # have moved since this process loaded its code.
            code_revision=code_revision(),
            job_id=job_id,
        )

    @staticmethod
    def _delta_mode(
        old_set: AnnotationSet,
        new_set: AnnotationSet,
        old_native: uuid.UUID,
        new_native: uuid.UUID,
        pivot_id: uuid.UUID,
    ) -> str:
        """Which delta path is VALID here, which is not the same as which was asked for.

        The fast path resolves annotations through ``go_term.id``, and that
        column is scoped to a snapshot: the same GO accession has one row per
        snapshot, so an id minted under one graph matches nothing under
        another. It is therefore only correct when both annotation sets are
        BOUND to the graph being used -- a property of the sets, not of the
        arguments.

        This used to test ``old_native == new_native == pivot_id``, which is a
        property of the arguments alone. The two agree whenever no override is
        passed, and diverge exactly when one is: asking for both sides under a
        pivot the sets are not bound to satisfied the old test, took the fast
        path, and matched none of the 11.2 million annotations of the 220 ->
        227 window. Every one was dropped by ``_load_experimental_annotations_by_ns``,
        which drops terms of unknown aspect without comment, and the operation
        stored a perfectly well-formed EvaluationSet holding zero of everything.

        So the test is on all five, and the sets' own bindings lead. The
        reconciled path resolves by ``go_id`` text and is correct in every case;
        the fast path is an optimisation, and an optimisation that can be wrong
        has to prove it is not.
        """
        bound_alike = old_set.ontology_snapshot_id == new_set.ontology_snapshot_id == pivot_id
        asked_alike = old_native == new_native == pivot_id
        return "same_snapshot" if bound_alike and asked_alike else "reconciled"

    def _compute_delta(
        self, session: Session, ident: _Identity, mode: str, emit: EmitFn
    ) -> tuple[Any, dict[str, Any]]:
        """Run the delta under this identity and return it with its stats.

        The two compute paths are one decision, taken once here: if all three
        snapshots coincide there is nothing to reconcile, and the cheaper
        same-snapshot path gives the same answer. Anything else needs each side
        closed under its own DAG and intersected with the pivot.
        """
        emit(
            "generate_evaluation_set.start",
            None,
            {
                "old_annotation_set_id": str(ident.old_set_id),
                "new_annotation_set_id": str(ident.new_set_id),
                "old_ontology_snapshot_id": str(ident.old_native),
                "new_ontology_snapshot_id": str(ident.new_native),
                "pivot_ontology_snapshot_id": str(ident.pivot_id),
                "mode": mode,
            },
            "info",
        )
        emit(
            "generate_evaluation_set.computing_delta",
            None,
            {
                "mode": mode,
                "truth_profile": ident.truth_profile,
                "toi_rule": ident.toi_rule,
                "not_codes": ident.not_codes,
            },
            "info",
        )
        if mode == "same_snapshot":
            data = compute_evaluation_data(
                session, ident.old_set_id, ident.new_set_id, ident.pivot_id
            )
        else:
            data = compute_evaluation_data_reconciled(
                session,
                ReconciledWindow(
                    ident.old_set_id,
                    ident.new_set_id,
                    ident.old_native,
                    ident.new_native,
                    ident.pivot_id,
                    ident.truth_profile,
                    ident.toi_rule,
                    ident.not_codes,
                ),
            )
        stats = data.stats()
        stats["mode"] = mode
        stats["pivot_ontology_snapshot_id"] = str(ident.pivot_id)
        stats["truth_profile"] = ident.truth_profile
        stats["toi_rule"] = ident.toi_rule
        stats["not_codes"] = ident.not_codes
        emit("generate_evaluation_set.delta_done", None, stats, "info")
        return data, stats

    @staticmethod
    def _refuse_empty_delta(session: Session, ident: _Identity, stats: dict[str, Any]) -> None:
        """Refuse a delta of nothing taken over corpora of something.

        An EvaluationSet with zero delta proteins is a legitimate result only
        when there was nothing to find. When the two corpora hold millions of
        annotations between them and the delta is empty, the terms did not
        resolve -- and nothing downstream can tell the two apart, because the
        row that comes out is well formed either way. That is how a silently
        empty ground truth reaches an evaluation and reports a clean zero.

        The check is cheap and it can fail, which is the only kind worth
        having: it counts the annotations the two sets actually hold and only
        raises when they are not empty either.
        """
        if int(stats.get("delta_proteins") or 0):
            return
        held = session.execute(
            text(
                "SELECT count(*) FROM protein_go_annotation "
                "WHERE annotation_set_id IN (:old_id, :new_id)"
            ),
            {"old_id": ident.old_set_id, "new_id": ident.new_set_id},
        ).scalar_one()
        if not held:
            return
        raise ValueError(
            f"the delta is empty but the two annotation sets hold {held} annotations. "
            "Their terms did not resolve under the graphs asked for "
            f"(pivot {str(ident.pivot_id)[:8]}, natives {str(ident.old_native)[:8]}/"
            f"{str(ident.new_native)[:8]}). Storing this would be a ground truth of "
            "nothing that reports a clean zero."
        )

    def _existing_set(self, session: Session, ident: _Identity) -> EvaluationSet | None:
        """The EvaluationSet already stored under this identity, if any.

        The identity is the five fields the delta depends on, not the two it
        used to be. A pair of annotation sets read under a different
        propagation graph is a different measurement: on GOA 220 -> 227 the
        choice moves the PK bucket by 21 percent of its annotations. Looking
        one up by the pair alone would serve a caller a set that answers a
        question they did not ask.
        """
        return (
            session.query(EvaluationSet)
            .filter_by(
                old_annotation_set_id=ident.old_set_id,
                new_annotation_set_id=ident.new_set_id,
                pivot_snapshot_id=ident.pivot_id,
                old_native_snapshot_id=ident.old_native,
                new_native_snapshot_id=ident.new_native,
                truth_profile=ident.truth_profile,
                toi_rule=ident.toi_rule,
                not_codes=ident.not_codes,
            )
            .one_or_none()
        )

    def _maybe_reuse_existing(
        self,
        session: Session,
        ident: _Identity,
        ask: _Ask,
        emit: EmitFn,
    ) -> OperationResult | None:
        """Return the existing EvaluationSet's result, or None if absent.

        Idempotency: if an EvaluationSet already exists under this identity,
        return its summary instead of computing and inserting a duplicate. The
        DB-level UNIQUE constraint enforces it at the schema layer (alembic
        ``d4b8c2f10a37_evaluation_set_identity_includes_graphs``); this
        short-circuit avoids paying the delta compute cost on a re-submission.

        A caller asking for the same pair under a DIFFERENT propagation graph
        does not land here: no row matches, and the delta is computed. That is
        the point of widening the key. It used to raise instead, which made two
        legitimate measurements mutually exclusive.

        ADR D40: a re-submission MAY carry a ``window_role`` to (re)bind an
        already-computed set to a protocol window. Designating the window
        is metadata only, so it is applied in place without recomputing
        the delta.

        AND IT SAYS WITH WHAT CODE THE ROW IT SERVES WAS BUILT, which it did
        not: see :func:`_reuse_verdict`. It still SERVES it, because refusing
        would change what every re-submission does and that is the
        researcher's call; what was wrong was the silence, not the serving.
        And under which truth rules, the same way: see :func:`_reuse_rules_note`.
        """
        existing = self._existing_set(session, ident)
        if existing is None:
            return None
        self._rebind_window_role(session, existing, ask.window_role, emit)
        level, nota = _reuse_verdict(getattr(existing, "code_revision", None))
        served = _served_rules(existing)
        rules_note = _reuse_rules_note(served, ask)
        if rules_note is not None:
            level, nota = "warning", f"{nota} {rules_note}"
        result = _served_result(existing)
        emit(
            "generate_evaluation_set.idempotent_reuse",
            None,
            {
                **_identity_fields(existing.id, ident),
                "served_code_revision": getattr(existing, "code_revision", None),
                "running_code_revision": code_revision(),
                "served_truth_rules": served,
                "truth_rules_applied_now": ask.rules.as_json(),
                "note": nota,
            },
            level,
        )
        return OperationResult(result=result)

    @staticmethod
    def _rebind_window_role(
        session: Session,
        existing: EvaluationSet,
        window_role: str | None,
        emit: EmitFn,
    ) -> None:
        """Re-designate an existing set's protocol window in place (ADR D40).

        No-op unless a ``window_role`` is supplied and differs from the
        current one. Metadata only: the delta is never recomputed.
        """
        if window_role is None or existing.window_role == window_role:
            return
        existing.window_role = window_role
        session.flush()
        emit(
            "generate_evaluation_set.window_role_set",
            None,
            {"evaluation_set_id": str(existing.id), "window_role": window_role},
            "info",
        )

    def _resolve_eval_inputs(
        self,
        session: Session,
        p: GenerateEvaluationSetPayload,
        old_set_id: uuid.UUID,
        new_set_id: uuid.UUID,
    ) -> tuple[AnnotationSet, AnnotationSet, uuid.UUID, uuid.UUID, uuid.UUID]:
        """Validate the two annotation sets exist + resolve the propagation snapshots.

        Returns ``(old_set, new_set, old_native, new_native, pivot)``. Each native
        defaults to its annotation set's stored ``ontology_snapshot_id`` but can be
        overridden explicitly (cross-OBO), and the pivot defaults to ``new_native``.

        A named rule set moves the natives' DEFAULTS to its closure, and nothing
        else: under ``october-headline-frame`` both default to the graph the t0
        corpus was loaded under, so the pivot follows them there. An explicit
        override still wins, and the build is then refused if it moves the
        closure the set names.
        """
        old_set = session.get(AnnotationSet, old_set_id)
        if old_set is None:
            raise ValueError(f"AnnotationSet {old_set_id} not found")
        new_set = session.get(AnnotationSet, new_set_id)
        if new_set is None:
            raise ValueError(f"AnnotationSet {new_set_id} not found")

        def _resolve_snapshot(raw: str | None, fallback: uuid.UUID) -> uuid.UUID:
            if raw is None:
                return fallback
            sid = uuid.UUID(raw)
            if session.get(OntologySnapshot, sid) is None:
                raise ValueError(f"OntologySnapshot {sid} not found")
            return sid

        closure = named_truth_rules(p.truth_rule_set).closure if p.truth_rule_set else "per_side"
        old_default, new_default = closing_graphs_by_default(
            closure, old_set.ontology_snapshot_id, new_set.ontology_snapshot_id
        )
        old_native = _resolve_snapshot(p.old_native_snapshot_id, old_default)
        new_native = _resolve_snapshot(p.new_native_snapshot_id, new_default)
        pivot_id = _resolve_snapshot(p.pivot_ontology_snapshot_id, new_native)
        return old_set, new_set, old_native, new_native, pivot_id

    def _persist_groundtruth(
        self,
        eval_set: EvaluationSet,
        data: Any,
        emit: EmitFn,
    ) -> str:
        """Serialise the full ground-truth parquet to the artifact store.

        Downstream consumers (the dump helper, cafaeval) read this
        parquet via ``load_evaluation_data_for_set`` instead of
        recomputing the delta.
        """
        project_root = Path(__file__).resolve().parents[3]
        store = get_artifact_store(load_settings(project_root))
        key = groundtruth_key_for(eval_set.id)
        with tempfile.TemporaryDirectory(prefix="protea_eval_gt_") as tmp:
            local_path = Path(tmp) / "groundtruth.parquet"
            serialize_evaluation_data_to_parquet(data, local_path)
            # Hashed from the file we are about to send, never from what comes
            # back: a digest taken after a round trip would agree with
            # whatever the store returned, including the wrong object.
            digest = sha256_of_file(local_path)
            uri = store.put(key, local_path)
        eval_set.groundtruth_uri = uri
        eval_set.groundtruth_sha256 = digest
        emit(
            "generate_evaluation_set.groundtruth_persisted",
            None,
            {
                "evaluation_set_id": str(eval_set.id),
                "uri": uri,
                "key": key,
                "sha256": digest,
            },
            "info",
        )
        return uri
