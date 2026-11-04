"""Evaluated explorations: the closed vocabularies and the rules' fingerprint (ADR-D55).

An exploration ties one falsifiable hypothesis to one evaluation window, rules
frozen before any number is read, the arms that answer it, a report generated
from rows and a review in two steps. The four tables that hold it live in
``protea.infrastructure.orm.models.exploration``; this module holds what they
share and what has to agree between the database, the server and the pages.

Every vocabulary here is closed. The tables enforce each one with a CHECK
constraint built from the same tuple, so a value the server does not know
cannot be written by a migration, a script or a hand-typed UPDATE either.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

__all__ = [
    "ACTOR_TYPES",
    "ARCHIVE_REASONS",
    "DECISIONS",
    "EVENT_TYPES",
    "GRADES",
    "KINDS",
    "LINK_ROLES",
    "LINK_TARGETS",
    "REFERENCE_KINDS",
    "STATES",
    "TRANSITIONS",
    "VERDICTS",
    "check_in",
    "rules_fingerprint",
]

#: ``one_axis``: two arms that differ only in the declared field.
#: ``full_method``: two different methods, declared by the author and marked as
#: not attributable to any component. ``instrument``: a check of the platform
#: itself against a predicate written beforehand.
KINDS: tuple[str, ...] = ("one_axis", "full_method", "instrument")

#: The four states. Archived is not one of them: it is a mark with a reason.
STATES: tuple[str, ...] = ("draft", "running", "reported", "decided")

#: Each state's only successor. A decided exploration is never reopened:
#: repeating it, or answering it again, is another exploration that links to it.
TRANSITIONS: dict[str, str] = {"draft": "running", "running": "reported", "reported": "decided"}

#: Why an exploration was archived. Superseded and withdrawn ones stay visible,
#: struck through.
ARCHIVE_REASONS: tuple[str, ...] = ("abandoned", "closed", "superseded", "withdrawn")

#: The author's decision. Accepting means the measure and its reading hold, not
#: that the hypothesis is true: an accepted "against" is a result.
DECISIONS: tuple[str, ...] = ("accepted", "rejected")

#: Computed by the server from the window's role and the checklist. A thesis
#: figure comes from a new window read once.
GRADES: tuple[str, ...] = ("thesis", "development", "check", "exploratory")

#: The server's reading of the panels the rules declared as deciding, from the
#: paired comparison's own states. ``blocked`` is a reading that may not be made:
#: a blocking check failed, or a deciding panel's maximum sits at an edge. First
#: version, published with the migration as ADR-D55 says.
VERDICTS: tuple[str, ...] = ("for", "against", "equal", "underpowered", "blocked")

#: What a link says about its target.
LINK_ROLES: tuple[str, ...] = (
    "input",
    "arm",
    "result",
    "strata",
    "comparison",
    "job",
    "temporary",
    "reference",
    "supports",
    "supersedes",
)

#: The columns of the exclusive arc, one per kind of target, each a foreign key
#: with ``RESTRICT``, plus the store key that travels with its sha256. Exactly
#: one is set on every link.
LINK_TARGETS: tuple[str, ...] = (
    "job_id",
    "prediction_set_id",
    "evaluation_result_id",
    "model_weights_id",
    "embedding_config_id",
    "query_set_id",
    "annotation_set_id",
    "information_accretion_set_id",
    "external_reference_id",
    "target_exploration_id",
    "store_key",
)

#: Who wrote an event. An API key is not the author: the decision is refused to
#: it, whatever role it carries.
ACTOR_TYPES: tuple[str, ...] = ("user", "api_key", "server")

#: The append-only log's vocabulary.
EVENT_TYPES: tuple[str, ...] = (
    "created",
    "launched",
    "amendment",
    "step_failed",
    "report",
    "check_answered",
    "objection",
    "reply",
    "reviewed",
    "decision",
    "deployed",
    "archived",
    "marked_for_deletion",
    "deleted",
)

#: What a published figure by others comes from.
REFERENCE_KINDS: tuple[str, ...] = (
    "paper",
    "preprint",
    "challenge",
    "leaderboard",
    "software",
    "dataset",
)


def check_in(column: str, values: tuple[str, ...], *, nullable: bool = False) -> str:
    """The CHECK expression that closes ``column`` over ``values``.

    Built from the tuple the server validates against, so the two cannot drift.
    """
    listed = ", ".join(f"'{v}'" for v in values)
    clause = f"{column} IN ({listed})"
    return f"{column} IS NULL OR {clause}" if nullable else clause


def rules_fingerprint(rules: Mapping[str, Any]) -> str:
    """The sha256 of the rules in canonical form.

    Sorted keys and separators without spaces, the encoding the embedding
    config ids and the frame seal already use, so the same rules cannot
    fingerprint differently for having been serialised by another caller.
    Non-ASCII text is kept as written rather than escaped, because the rules
    hold the author's prose and two spellings of one character would otherwise
    be two fingerprints.
    """
    canonical = json.dumps(rules, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
