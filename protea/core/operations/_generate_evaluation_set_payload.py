"""The payload of ``generate_evaluation_set``, in a module of its own.

Split out of the operation's module when the window gained its target
population and the module passed the 800-line gate, as ``_run_cafa_payload``
was for ``run_cafa_evaluation``. The operation re-exports it, so every caller
keeps importing it from ``protea.core.operations.generate_evaluation_set``.
"""

from __future__ import annotations

import uuid
from typing import Any

from pydantic import field_validator, model_validator

from protea.core._binding_rule import REQUESTABLE_BINDING_RULES
from protea.core._window_population import TARGET_POPULATIONS, refuse_unknown_population
from protea.core.contracts.operation import ProteaPayload
from protea.core.evaluation import GAIN_RULES, NOT_CODE_RULES, TRUTH_PROFILES
from protea.core.window_declarations import WINDOW_ROLES, payload_with_named_rules


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
    # ``"october-headline-frame"`` or ``"lafa-frame"``. It fills
    # ``truth_profile``, ``toi_rule``, ``not_codes`` and ``binding_rule``, and the
    # target population when it fixes one (LAFA's frame does), and refuses a
    # payload that asks for others; and it fixes the closure: under the October
    # rules both sides close under the graph the t0 corpus was loaded under, so
    # both natives (and with them the pivot) default to that graph, and under
    # LAFA's each side closes under its own graph and the pivot is t0's. A build
    # that would apply anything else is refused before the delta is computed.
    # None leaves every default as it was.
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
    # Which protein-binding rule builds the F side (``protea.core._binding_rule``).
    # ``descendants_both_ends`` (the default) is what every stored window did:
    # PROTEA's own rule, on direct annotations at both ends. ``cafa`` is CAFA's
    # and LAFA's: a protein's F side is dropped at t1 when its propagated F set is
    # binding only, and t0 is left alone. Part of the identity: it changes the
    # delta.
    binding_rule: str = REQUESTABLE_BINDING_RULES[0]
    # Which proteins the window may score (``protea.core._window_population``).
    # ``every_protein`` (the default) is what every stored window did: any
    # protein that gained. ``superset_at_t0`` keeps the superset of the photo
    # of the window's start, the Swiss-Prot list of its paired release, which
    # is what the author chose for the four windows of the frame (ADR-D60,
    # 2026-11-16). Part of the identity: it changes the delta.
    target_population: str = TARGET_POPULATIONS[0]
    # A declared sample of the window's targets (a ``window_sample`` query set):
    # the window is then its parent's (the sample's window) kept to the sample's
    # proteins, its truth the parent's verified truth filtered, never recomputed.
    # Orthogonal to ``target_population``, which a named rule set may fix. Part of
    # the identity. None scores the whole population, as every stored window does.
    target_query_set_id: str | None = None

    @field_validator("target_query_set_id", mode="before")
    @classmethod
    def _a_query_set_id(cls, value: Any) -> str | None:
        return None if value is None else str(uuid.UUID(str(value)))

    @field_validator("target_population")
    @classmethod
    def _known_population(cls, value: str) -> str:
        return refuse_unknown_population(value)

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

    @field_validator("binding_rule")
    @classmethod
    def _known_binding_rule(cls, value: str) -> str:
        if value not in REQUESTABLE_BINDING_RULES:
            raise ValueError(
                f"unknown binding_rule {value!r}; expected one of {REQUESTABLE_BINDING_RULES}"
            )
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
        """Fill the rule fields (and a fixed population) from ``truth_rule_set`` before they validate.

        Before and not after, because after validation a field left out and a
        field sent with its default look the same, and only the first may be
        filled from the set.
        """
        return payload_with_named_rules(data) if isinstance(data, dict) else data
