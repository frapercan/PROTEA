"""What a window declares: its role in a frame and the truth rules it was built under.

ADR-D60 decision 1 puts frames in the universe: windows with a role
(``training``, ``validation`` or ``test_spent``) and declared truth rules,
stored with each window. This module holds both vocabularies, the rule set the
author chose on 2026-11-15, and the helper that refuses to treat windows built
under different rules as one frame.

THE ROLES. ``valid`` and ``test`` are ADR-D40's and stay, because stored rows
carry them. The three of ADR-D60 say what a window is for in a frame:

* ``training``: its gains may be fitted on;
* ``validation``: a method instance is selected and read on it, never fitted
  on it (ADR-D60: nothing is fitted on annotations after the instance cut, and
  selecting on a later window is allowed);
* ``test_spent``: a test window whose single read has happened. It is kept so
  the evaluator can refuse it by name; the 227 -> 230 window is one, spent on
  2026-10-27.

A role is the window's role in the frame. Which windows one method instance
fits on is that method's own declaration (ADR-D60 decision 3), because one
window can train one instance and validate another.

THE RULES ARE THE ONES THE CODE APPLIED, NEVER THE ONES THE PAYLOAD ASKED FOR.
``truth_profile``, ``toi_rule`` and ``not_codes`` on ``evaluation_set`` are the
request, and on one path the request is not what runs: when both corpora and
every graph are one snapshot, ``generate_evaluation_set`` takes
``compute_evaluation_data``, which takes no rule argument at all. So the rules a
window declares are derived from the path the build took and the graphs it
resolved, in :data:`SAME_SNAPSHOT_RULES` and :func:`closure_applied`.

THE CLOSURE is the fourth rule, and the one with no column of its own. The three
graph ids are on the row, but ids cannot be compared across windows at different
cuts, and a frame is exactly such a list. What compares is how the graphs relate
to the window's own two ends: the ontology each end's corpus was loaded under.

THE BINDING RULE is the fifth (:mod:`protea.core._binding_rule`): which
protein-binding rule built the F side, if any. Rows stored before it was
declared were given the rule their path applied when the migration added it:
``none`` on the same-snapshot path, ``descendants_both_ends`` on every other.

A row written before the rules were stored holds NULL, and NULL reads as
legacy: "nothing says", never "the defaults".
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, NamedTuple, Protocol

from sqlalchemy.orm import Session

from protea.core._binding_rule import BINDING_RULES
from protea.core._truth_rules import GAIN_RULES, NOT_CODE_RULES, TRUTH_PROFILES
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet

#: ADR-D40's two roles. Kept because stored rows carry them.
LEGACY_WINDOW_ROLES: tuple[str, ...] = ("valid", "test")

#: ADR-D60's three. See the module docstring for what each one allows.
FRAME_WINDOW_ROLES: tuple[str, ...] = ("training", "validation", "test_spent")

#: Every value ``evaluation_set.window_role`` accepts besides NULL. The CHECK
#: constraint ``ck_evaluation_set_window_role`` lists the same five.
WINDOW_ROLES: tuple[str, ...] = LEGACY_WINDOW_ROLES + FRAME_WINDOW_ROLES

#: How the graphs a build used relate to the window's two ends.
#:
#: * ``per_side``: each side closed under the graph its own corpus was loaded
#:   under, and the pivot is the t1 one. What a call naming no graph gets.
#: * ``t0_graph``: both sides closed under the t0 corpus's graph, which is also
#:   the pivot. The October headline frame.
#: * ``t1_graph``: both sides and the pivot at the t1 corpus's graph.
#: * ``explicit``: any other combination, which only an explicit override can
#:   produce. The row's own three graph ids say which; a frame helper cannot
#:   compare it across cuts and refuses it.
#: * ``direct_terms``: no graph closes either side at build time, which is the
#:   same-snapshot path. See :data:`SAME_SNAPSHOT_RULES`.
CLOSURES: tuple[str, ...] = ("per_side", "t0_graph", "t1_graph", "explicit", "direct_terms")

#: The closures a named rule set may fix: the ones stated by the window's ends.
NAMEABLE_CLOSURES: tuple[str, ...] = ("per_side", "t0_graph", "t1_graph")

#: The payload fields a named rule set fixes, in :class:`TruthRules` order.
RULE_FIELDS: tuple[str, ...] = ("truth_profile", "toi_rule", "not_codes", "binding_rule")

_KEYS: tuple[str, ...] = ("profile", "toi_rule", "not_codes", "closure", "binding")


class MixedTruthRulesError(ValueError):
    """Windows offered as one frame do not show one set of truth rules."""


@dataclass(frozen=True)
class TruthRules:
    """The five rules that decide what a window's truth IS.

    ``profile``, ``toi_rule`` and ``not_codes`` take the vocabularies of
    :mod:`protea.core._truth_rules`; ``closure`` takes :data:`CLOSURES` and
    ``binding`` takes :data:`protea.core._binding_rule.BINDING_RULES`. An
    unknown value raises at construction, so a stored set that reads back is a
    set the code knows how to apply.
    """

    profile: str
    toi_rule: str
    not_codes: str
    closure: str
    binding: str

    def __post_init__(self) -> None:
        for name, value, vocabulary in (
            ("profile", self.profile, TRUTH_PROFILES),
            ("toi_rule", self.toi_rule, GAIN_RULES),
            ("not_codes", self.not_codes, NOT_CODE_RULES),
            ("closure", self.closure, CLOSURES),
            ("binding", self.binding, BINDING_RULES),
        ):
            if value not in vocabulary:
                raise ValueError(f"unknown {name} {value!r}; expected one of {vocabulary}")

    def as_json(self) -> dict[str, str]:
        """The shape stored in ``evaluation_set.truth_rules``."""
        return {
            "profile": self.profile,
            "toi_rule": self.toi_rule,
            "not_codes": self.not_codes,
            "closure": self.closure,
            "binding": self.binding,
        }

    @classmethod
    def from_json(cls, stored: Mapping[str, Any]) -> TruthRules:
        """Read a stored set back, refusing a key this code does not know.

        Strict on the keys in both directions: a set with a fifth rule was
        written by code that applies a rule this one does not, and reading it
        as these four would merge two truths under one name.
        """
        if not isinstance(stored, Mapping) or set(stored) != set(_KEYS):
            raise ValueError(f"stored truth rules {stored!r} do not have exactly the keys {_KEYS}")
        return cls(*(stored[key] for key in _KEYS))

    def describe(self) -> str:
        """One line for a refusal or an event."""
        return ", ".join(f"{key}={value}" for key, value in self.as_json().items())


#: The rules ``compute_evaluation_data`` applies, whatever the payload asked.
#:
#: It takes no rule argument (a test pins its signature), so it never projects,
#: never trims the gain to the terms of interest and reads every NOT row
#: whatever its evidence: the first value of each vocabulary. And it closes
#: nothing at build time: the gain is computed on the direct terms of the one
#: graph both corpora were loaded under.
SAME_SNAPSHOT_RULES = TruthRules(
    profile=TRUTH_PROFILES[0],
    toi_rule=GAIN_RULES[0],
    not_codes=NOT_CODE_RULES[0],
    closure="direct_terms",
    # ``compute_evaluation_data`` runs no protein-binding rule at all.
    binding="none",
)

#: The truth rules of the October headline frame, ``evaluation_set ea0f4334``:
#: experimental gains with t1 terms projected onto t0's live terms, only terms
#: live at both ends, NOT rows only when experimental, and both sides closed
#: under the graph of t0, which is also the pivot. The author chose them for
#: the four training windows on 2026-11-15.
OCTOBER_HEADLINE_FRAME = TruthRules(
    profile="projected",
    toi_rule="live_in_both_ends",
    not_codes="experimental",
    closure="t0_graph",
    binding="descendants_both_ends",
)

#: The truth rules of the benchmark: the October headline frame with CAFA's and
#: LAFA's protein-binding rule in place of PROTEA's own, so that the F side of a
#: window is the one the community scores. The author chose it on 2026-11-20,
#: together with the target population ``superset_at_t0`` (ADR-D60), which is a
#: field of its own and not a truth rule.
BENCHMARK_FRAME = TruthRules(
    profile="projected",
    toi_rule="live_in_both_ends",
    not_codes="experimental",
    closure="t0_graph",
    binding="cafa",
)

#: Rule sets a ``generate_evaluation_set`` payload can name, by a name that says
#: whose choice each one is. Every closure here must be in
#: :data:`NAMEABLE_CLOSURES`, or the operation could not resolve its graphs.
NAMED_TRUTH_RULES: Mapping[str, TruthRules] = MappingProxyType(
    {"october-headline-frame": OCTOBER_HEADLINE_FRAME, "benchmark-frame": BENCHMARK_FRAME}
)


def named_truth_rules(name: object) -> TruthRules:
    """The rule set registered under ``name``, or a refusal listing the known ones."""
    if not isinstance(name, str) or name not in NAMED_TRUTH_RULES:
        raise ValueError(
            f"unknown truth_rule_set {name!r}; expected one of {tuple(NAMED_TRUTH_RULES)}"
        )
    return NAMED_TRUTH_RULES[name]


def payload_with_named_rules(payload: Mapping[str, Any]) -> dict[str, Any]:
    """The payload with the four rule fields its named rule set fixes filled in.

    A field the payload already carries must agree with the set: a payload that
    names ``october-headline-frame`` and asks for ``as_closed`` asks for two
    truths, and picking either would be a guess. The closure is not a payload
    field; :func:`closing_graphs_by_default` resolves it against the corpora.
    """
    filled = dict(payload)
    name = filled.get("truth_rule_set")
    if name is None:
        return filled
    rules = named_truth_rules(name)
    for field, value in zip(
        RULE_FIELDS, (rules.profile, rules.toi_rule, rules.not_codes, rules.binding), strict=True
    ):
        asked = filled.setdefault(field, value)
        if asked != value:
            raise ValueError(
                f"truth_rule_set {name!r} fixes {field} at {value!r} and the payload "
                f"asks for {asked!r}"
            )
    return filled


def closing_graphs_by_default(
    closure: str, t0_era: uuid.UUID, t1_era: uuid.UUID
) -> tuple[uuid.UUID, uuid.UUID]:
    """The graphs that close the t0 and t1 sides when the payload names neither.

    ``t0_era`` and ``t1_era`` are the ontologies the two corpora were loaded
    under. ``per_side`` is what every call that names no rule set gets, which is
    what the operation always did. The pivot is not here: it keeps defaulting
    to the graph that closes t1.
    """
    defaults = {
        "per_side": (t0_era, t1_era),
        "t0_graph": (t0_era, t0_era),
        "t1_graph": (t1_era, t1_era),
    }
    if closure not in defaults:
        raise ValueError(
            f"closure {closure!r} cannot be resolved; it is one of {NAMEABLE_CLOSURES}"
        )
    return defaults[closure]


class WindowGraphs(NamedTuple):
    """The window's two ends and the three graphs a reconciled build used.

    ``t0_era`` and ``t1_era`` are ``AnnotationSet.ontology_snapshot_id`` of the
    two corpora, fixed when each was loaded. The other three are the resolved
    graphs stored on the row.
    """

    t0_era: uuid.UUID
    t1_era: uuid.UUID
    old_native: uuid.UUID
    new_native: uuid.UUID
    pivot: uuid.UUID


def closure_applied(graphs: WindowGraphs) -> str:
    """How the graphs of a reconciled build relate to the window's own ends.

    Read from the RESOLVED graphs, so an override that happens to name a
    window's own graph is that closure, and one naming any other graph is
    ``explicit``. Never ``direct_terms``: that is a path, not a set of graphs,
    and the caller knows which path it took.
    """
    used = (graphs.old_native, graphs.new_native, graphs.pivot)
    t0, t1 = graphs.t0_era, graphs.t1_era
    if used == (t0, t0, t0):
        return "t0_graph"
    if used == (t0, t1, t1):
        return "per_side"
    if used == (t1, t1, t1):
        return "t1_graph"
    return "explicit"


def refuse_unless_named_rules_apply(name: str | None, applied: TruthRules) -> None:
    """Refuse a build that would not apply the rule set its payload names.

    Two ways to get here, both refused before anything is computed: an
    explicit graph override that moves the closure, and two corpora loaded
    under one snapshot, whose same-snapshot path applies none of the rules.
    """
    if name is None:
        return
    named = named_truth_rules(name)
    if applied != named:
        raise ValueError(
            f"truth_rule_set {name!r} declares {named.describe()}, and this window would be "
            f"built under {applied.describe()}. A graph override moves the closure, and "
            "closure=direct_terms means both corpora and every graph are one snapshot, a path "
            "that applies none of the rules. Built without the rule set, the window declares "
            "what it is"
        )


class DeclaredWindow(Protocol):
    """What the frame helper reads from a window: its id and its stored rules."""

    @property
    def id(self) -> uuid.UUID: ...

    @property
    def truth_rules(self) -> Mapping[str, Any] | None: ...


def stored_truth_rules(window: DeclaredWindow) -> TruthRules | None:
    """The rules a window was built under, or None for a legacy row."""
    stored = window.truth_rules
    return None if stored is None else TruthRules.from_json(stored)


def common_truth_rules(windows: Sequence[DeclaredWindow]) -> TruthRules:
    """The one set of truth rules every window was built under, or a refusal.

    Three refusals before the comparison, because each is a window whose rules
    cannot be compared at all and "cannot say" is not agreement:

    * no windows;
    * a legacy window, whose rules were never stored;
    * an ``explicit`` closure, whose graphs no label relates to its ends.
    """
    if not windows:
        raise MixedTruthRulesError("no windows were given, so there are no rules to share")
    declared = [(window.id, stored_truth_rules(window)) for window in windows]
    legacy = [window_id for window_id, rules in declared if rules is None]
    if legacy:
        raise MixedTruthRulesError(
            f"{len(legacy)} of {len(windows)} windows predate declared truth rules (legacy): "
            f"{_ids(legacy)}. Nothing says they were built under the rules of the others"
        )
    by_rules: dict[TruthRules, list[uuid.UUID]] = {}
    for window_id, rules in declared:
        if rules is not None:
            by_rules.setdefault(rules, []).append(window_id)
    unstated = [i for rules, ids in by_rules.items() if rules.closure == "explicit" for i in ids]
    if unstated:
        raise MixedTruthRulesError(
            f"windows {_ids(unstated)} were closed under explicit graphs, which no label "
            "relates to their own ends, so they cannot be compared across cuts"
        )
    if len(by_rules) > 1:
        groups = "; ".join(f"{rules.describe()} for {_ids(ids)}" for rules, ids in by_rules.items())
        raise MixedTruthRulesError(f"the windows were built under different truth rules: {groups}")
    return next(iter(by_rules))


def common_truth_rules_of(session: Session, evaluation_set_ids: Iterable[uuid.UUID]) -> TruthRules:
    """:func:`common_truth_rules` over windows named by id, refusing an id with no row."""
    wanted = set(evaluation_set_ids)
    rows = session.query(EvaluationSet).filter(EvaluationSet.id.in_(wanted)).all()
    missing = wanted - {row.id for row in rows}
    if missing:
        raise MixedTruthRulesError(f"no evaluation_set row for {_ids(missing)}")
    return common_truth_rules(rows)


def _ids(ids: Iterable[uuid.UUID]) -> str:
    """Sorted, comma separated, in full: a refusal is read to find the rows."""
    return ", ".join(sorted(str(i) for i in ids))


__all__ = [
    "CLOSURES",
    "FRAME_WINDOW_ROLES",
    "LEGACY_WINDOW_ROLES",
    "NAMEABLE_CLOSURES",
    "NAMED_TRUTH_RULES",
    "OCTOBER_HEADLINE_FRAME",
    "RULE_FIELDS",
    "SAME_SNAPSHOT_RULES",
    "WINDOW_ROLES",
    "DeclaredWindow",
    "MixedTruthRulesError",
    "TruthRules",
    "WindowGraphs",
    "closing_graphs_by_default",
    "closure_applied",
    "common_truth_rules",
    "common_truth_rules_of",
    "named_truth_rules",
    "payload_with_named_rules",
    "refuse_unless_named_rules_apply",
    "stored_truth_rules",
]
