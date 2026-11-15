"""The method contract: a method has an identity of its own (ADR-D60, decision 3).

WHAT THIS ENDS. Until this module a method could enter the record only as an
embedding configuration, because ``prediction_set.embedding_config_id`` was NOT
NULL. A method that is not a nearest-neighbour vote over one embedding had no row
of its own and had to pass as a configuration (``register_ingested_method`` says
so in its first paragraph). The three method families of ADR-D60 cannot share a
universe on those terms: two of them are not an embedding at all.

WHAT A METHOD DECLARES. A :class:`MethodSpec` names the method's family, its
code, the instance cut it lives at (a release and an ontology snapshot), its
recipe, the bytes it pins by sha256, how it runs and whether it explains its
predictions. A :class:`TrainingIdentity` says how its fitted pins were trained:
the windows it was fitted on and held out on, by evaluation set id, the digests
of its data, the revisions of the code that trained it and the pretrained
weights it took as declared exceptions. The leakage guard reads the instance,
the windows and the pins against the frame an evaluation scores
(``protea.core._evaluation_leakage``).

THE IDENTIFIER IS DERIVED, NEVER ASSIGNED. ``method_id`` is
``uuid5(METHOD_NAMESPACE, canonical_json(spec.document()))``. The same spec is
the same method on any machine and in any database, and two specs that differ
anywhere cannot share an id. The canonical JSON is the encoding
``protea.core.embedding_identity`` already pins for configurations: sorted keys,
separators without spaces, ASCII escapes. Two collections in the document are
sets and are sorted before they are written, so the order a caller listed them
in cannot make a second method out of the first: the pins (by role, digest and
references) and the references of each pin. ``spec_sha256`` is the sha256 of the
same bytes, kept because a uuid5 keeps 122 bits of a SHA-1.

THE NAMESPACE IS PINNED. :data:`METHOD_NAMESPACE` is written out as a literal so
the derivation can be recomputed from this file alone, and
``tests/test_method_contract.py`` pins both the literal and how it was made.
Changing it reissues every method id in the record, which is a migration and not
an edit.

This module imports nothing outside the standard library.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Literal

__all__ = [
    "DATED_REFERENCES",
    "METHOD_NAMESPACE",
    "RUNTIMES",
    "WINDOW_ROLES",
    "Digest",
    "Family",
    "InstanceCut",
    "MethodSpec",
    "Pin",
    "Runtime",
    "TrainingIdentity",
    "TrainingWindow",
    "WindowRole",
    "canonical_json",
    "derive_method_id",
    "row_reference",
]

#: ``uuid5(uuid.NAMESPACE_URL, "https://protea/method")``, the recipe
#: ``protea.core.embedding_identity`` uses for configurations, written out so the
#: value does not depend on recomputing it.
METHOD_NAMESPACE = uuid.UUID("c034d65c-55e2-5fe7-8594-0dbc9ee317bd")


class Family(StrEnum):
    """The three method families of ADR-D60, decision 2.

    The word is *family* because *arm* already names a published, evaluated
    configuration (``protea/api/routers/_arm_identity.py``). The descriptors
    and the introductions live in ``protea.core.families``.
    """

    EMBEDDING_RERANKER = "embedding-reranker"
    WORLD_MODEL = "world-model"
    EXPLAINABLE_UNITS = "explainable-units"


Runtime = Literal["native", "container"]
#: ``native`` runs as platform operations; ``container`` runs from an image the
#: platform pins (ADR-D53).
RUNTIMES: tuple[str, ...] = ("native", "container")

WindowRole = Literal["fit", "holdout"]
#: A training window is either fitted on or held out for selection. Both saw
#: labels, so the leakage guard dates both.
WINDOW_ROLES: tuple[str, ...] = ("fit", "holdout")

#: A sha256 in lowercase hexadecimal.
Digest = str

#: The platform rows a pin may name in its references, spelled
#: ``"<table>:<uuid>"``. Each carries a release the leakage guard can read: an
#: annotation set its own, an evaluation set the release it ends at, and a
#: registered weights file the annotation set it was fitted on. Any other
#: reference is a locator (a store key, an image digest, a repository) that
#: dates nothing.
DATED_REFERENCES: tuple[str, ...] = ("annotation_set", "evaluation_set", "model_weights")

_SLUG = re.compile(r"[a-z0-9][a-z0-9-]*")
_SLUG_MAX = 64
_SHA256 = re.compile(r"[0-9a-f]{64}")


def _slug(value: object, what: str) -> str:
    """Refuse anything but the lowercase slug ADR-D58 gives a short descriptor."""
    if not isinstance(value, str) or len(value) > _SLUG_MAX or not _SLUG.fullmatch(value):
        raise ValueError(
            f"{what} {value!r} is not a lowercase slug of at most {_SLUG_MAX} characters "
            "matching [a-z0-9][a-z0-9-]*. It is written into keys and file names and read "
            "back as one, so it cannot carry spaces, capitals or punctuation."
        )
    return value


def _digest(value: object, what: str) -> Digest:
    if not isinstance(value, str) or not _SHA256.fullmatch(value):
        raise ValueError(f"{what} {value!r} is not a sha256 in lowercase hexadecimal")
    return value


def _text(value: object, what: str) -> str:
    """A non-empty string with no whitespace: a revision, a reference, a name."""
    if not isinstance(value, str) or not value or any(c.isspace() for c in value):
        raise ValueError(f"{what} {value!r} must be a non-empty string without whitespace")
    return value


def _sequence(value: object, what: str) -> tuple[Any, ...]:
    """``value`` as a tuple, refusing a string, which iterates as its characters.

    A field that holds several values and is given one string would otherwise
    be read character by character: ``pretrained_exceptions="ankh-base"`` was
    nine exceptions, and ``pins=""`` was a method with no pins.
    """
    if isinstance(value, (str, bytes)) or not isinstance(value, (tuple, list)):
        raise ValueError(f"{what} must be a tuple, not a {type(value).__name__}")
    return tuple(value)


def _as_uuid(value: object, what: str) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(str(value))
    except ValueError as exc:
        raise ValueError(f"{what} {value!r} is not a uuid") from exc


def _plain(value: Any, path: str) -> Any:
    """``value`` as plain JSON values, refusing what has no single spelling.

    Keys must be strings, because ``json.dumps`` writes the integer 1 and the
    string "1" as the same key and two recipes would collapse into one. Floats
    must be finite, because NaN and infinity are not JSON. Anything else that is
    not a JSON value (a uuid, a date, a set) is refused rather than converted,
    since every conversion is a spelling somebody else could choose differently.
    """
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} is {value!r}, which has no JSON spelling")
        return value
    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path} has the key {key!r}; keys must be strings")
            out[key] = _plain(item, f"{path}.{key}")
        return out
    if isinstance(value, (list, tuple)):
        return [_plain(item, f"{path}[{i}]") for i, item in enumerate(value)]
    raise ValueError(
        f"{path} is a {type(value).__name__}, which is not a JSON value. Spell it as one "
        "(a uuid or a date as its text) so the canonical form has one spelling for it."
    )


def _freeze(value: Any) -> Any:
    """A read-only copy of a plain JSON value, so a spec cannot change under its id."""
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def canonical_json(value: Any) -> str:
    """The one spelling of a JSON value this contract hashes.

    Sorted keys, separators without spaces and ASCII escapes, the encoding
    ``derive_embedding_config_id`` pins, so the same value cannot hash
    differently for having been serialised by a different caller.

    Raises:
        ValueError: when ``value`` holds something with no single JSON spelling.
    """
    return json.dumps(
        _plain(value, "$"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def row_reference(ref: str) -> tuple[str, uuid.UUID] | None:
    """The platform row a pin reference names, or None for a plain locator.

    Raises:
        ValueError: when the reference starts with a dated table and the rest is
            not a uuid, which is a typo that would otherwise date nothing.
    """
    table, sep, rest = ref.partition(":")
    if not sep or table not in DATED_REFERENCES:
        return None
    return table, _as_uuid(rest, f"the {table} reference {ref!r}")


def _one_spelling(ref: str) -> str:
    """A dated reference written the one way, so two spellings of a uuid are one pin."""
    row = row_reference(ref)
    return ref if row is None else f"{row[0]}:{row[1]}"


@dataclass(frozen=True)
class InstanceCut:
    """Where a method lives: a release of the corpus and its ontology snapshot."""

    release: int
    ontology_snapshot_id: uuid.UUID

    def __post_init__(self) -> None:
        if isinstance(self.release, bool) or not isinstance(self.release, int):
            raise ValueError(f"the instance release {self.release!r} is not an integer")
        if self.release <= 0:
            raise ValueError(f"the instance release {self.release} is not a release ordinal")
        snapshot = _as_uuid(self.ontology_snapshot_id, "the instance ontology_snapshot_id")
        object.__setattr__(self, "ontology_snapshot_id", snapshot)


@dataclass(frozen=True)
class Pin:
    """Bytes a method depends on, named by sha256, with where to find them.

    ``role`` says what the bytes are to the method (a slug such as ``plm`` or
    ``reranker``). ``refs`` are references to the bytes: a platform row spelled
    ``"<table>:<uuid>"`` (see :data:`DATED_REFERENCES`) or a locator. They are a
    set, so they are kept sorted, a dated one is written with the uuid in its
    canonical form, and a repeated one is refused.
    """

    role: str
    sha256: Digest
    refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _slug(self.role, "the pin role")
        _digest(self.sha256, f"the {self.role} pin's sha256")
        given = _sequence(self.refs, f"the {self.role} pin's refs")
        refs = tuple(_one_spelling(_text(ref, f"a {self.role} pin reference")) for ref in given)
        if len(set(refs)) != len(refs):
            raise ValueError(f"the {self.role} pin lists a reference more than once: {refs}")
        object.__setattr__(self, "refs", tuple(sorted(refs)))

    def document(self) -> dict[str, Any]:
        return {"role": self.role, "sha256": self.sha256, "refs": list(self.refs)}


@dataclass(frozen=True)
class TrainingWindow:
    """One window a training read labels from, named by its evaluation set id."""

    evaluation_set_id: uuid.UUID
    role: WindowRole

    def __post_init__(self) -> None:
        window = _as_uuid(self.evaluation_set_id, "the training window evaluation_set_id")
        object.__setattr__(self, "evaluation_set_id", window)
        if self.role not in WINDOW_ROLES:
            raise ValueError(
                f"the training window role {self.role!r} is not one of {list(WINDOW_ROLES)}"
            )


@dataclass(frozen=True)
class TrainingIdentity:
    """How a method's fitted pins were trained.

    ``windows`` are the windows it was fitted on and ``holdout`` the one it was
    selected on, all by evaluation set id; both read labels, so the leakage
    guard dates both. ``data`` holds the sha256 of every dataset it read. The
    three revisions name the code: this repository, the six sibling packages it
    pins by commit, and the evaluator that scored its selection.

    ``label_free`` says the training read no GO label at all, the case of a
    lexicon or a vocabulary fitted on sequences, so it names no window; a
    training that read labels names at least one, because labels from nowhere
    declared cannot be dated. ``pretrained_exceptions`` names the label-free
    pretrained weights it took as declared exceptions (ADR-D60, the author's
    decision on weights of 2026-11-15).
    """

    windows: tuple[TrainingWindow, ...]
    holdout: TrainingWindow | None
    data: tuple[Digest, ...]
    code_revision: str
    sibling_revisions: Mapping[str, str]
    evaluator_revision: str
    label_free: bool
    pretrained_exceptions: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.label_free, bool):
            raise ValueError(f"label_free {self.label_free!r} is not a boolean")
        self._check_windows()
        data = _sequence(self.data, "the training data digests")
        object.__setattr__(self, "data", tuple(_digest(d, "a training data digest") for d in data))
        _text(self.code_revision, "the code_revision")
        _text(self.evaluator_revision, "the evaluator_revision")
        if not isinstance(self.sibling_revisions, Mapping):
            raise ValueError("the sibling revisions must be a mapping of package to revision")
        siblings = {
            _text(name, "a sibling package"): _text(rev, f"the {name} revision")
            for name, rev in self.sibling_revisions.items()
        }
        object.__setattr__(self, "sibling_revisions", MappingProxyType(siblings))
        given = _sequence(self.pretrained_exceptions, "the pretrained exceptions")
        exceptions = tuple(_text(e, "a pretrained exception") for e in given)
        object.__setattr__(self, "pretrained_exceptions", exceptions)

    def _check_windows(self) -> None:
        windows = _sequence(self.windows, "the training windows")
        object.__setattr__(self, "windows", windows)
        given = (*windows, self.holdout) if self.holdout is not None else windows
        if not all(isinstance(w, TrainingWindow) for w in given):
            raise ValueError("the training windows must be TrainingWindow values")
        if any(w.role != "fit" for w in windows):
            raise ValueError("every training window must have the role 'fit'")
        if self.holdout is not None and self.holdout.role != "holdout":
            raise ValueError("the holdout window must have the role 'holdout'")
        ids = [w.evaluation_set_id for w in self.all_windows()]
        if len(set(ids)) != len(ids):
            raise ValueError("an evaluation set is named twice among the training windows")
        if self.label_free and ids:
            raise ValueError(
                "a label-free training names no window: a window is a delta of labels, so "
                "naming one contradicts the claim that no label was read"
            )
        if not self.label_free and not windows:
            raise ValueError(
                "a training that read labels must name the windows it was fitted on; labels "
                "from nowhere declared cannot be dated against a frame"
            )

    def all_windows(self) -> tuple[TrainingWindow, ...]:
        """The fit windows and the holdout, the set the leakage guard dates."""
        return self.windows if self.holdout is None else (*self.windows, self.holdout)

    def __hash__(self) -> int:
        return hash((self.windows, self.holdout, self.data, self.code_revision))


@dataclass(frozen=True, eq=False)
class MethodSpec:
    """Everything a method's identity is derived from (ADR-D60, decision 3).

    The recipe is checked and frozen when the spec is built, so it holds plain
    JSON values only and cannot change under the id derived from it. The pins
    are a set: kept sorted, and two pins with one role and one digest are
    refused.

    Two specs are equal when their canonical forms are, which is when they
    derive one id. Field equality would not do: Python holds ``5 == 5.0`` and
    ``True == 1``, and JSON spells each pair two ways.
    """

    family: Family
    code: str
    instance: InstanceCut
    recipe: Mapping[str, Any]
    pins: tuple[Pin, ...]
    runtime: Runtime
    explains: bool

    def __post_init__(self) -> None:
        if self.family not in tuple(Family):
            raise ValueError(f"{self.family!r} is not one of {[f.value for f in Family]}")
        object.__setattr__(self, "family", Family(self.family))
        _slug(self.code, "the method code")
        if not isinstance(self.instance, InstanceCut):
            raise ValueError("the instance must be an InstanceCut")
        if not isinstance(self.recipe, Mapping):
            raise ValueError("the recipe must be a mapping")
        object.__setattr__(self, "recipe", _freeze(_plain(self.recipe, "the recipe")))
        given = _sequence(self.pins, "the pins")
        if not all(isinstance(p, Pin) for p in given):
            raise ValueError("the pins must be Pin values")
        pins = tuple(sorted(given, key=lambda p: (p.role, p.sha256, p.refs)))
        named = [(p.role, p.sha256) for p in pins]
        if len(set(named)) != len(named):
            raise ValueError("a pin is listed twice with one role and one sha256")
        object.__setattr__(self, "pins", pins)
        if self.runtime not in RUNTIMES:
            raise ValueError(f"the runtime {self.runtime!r} is not one of {list(RUNTIMES)}")
        if not isinstance(self.explains, bool):
            raise ValueError(f"explains {self.explains!r} is not a boolean")

    def document(self) -> dict[str, Any]:
        """The spec as the plain JSON document its identity is derived from."""
        return {
            "family": self.family.value,
            "code": self.code,
            "instance": {
                "release": self.instance.release,
                "ontology_snapshot_id": str(self.instance.ontology_snapshot_id),
            },
            "recipe": _plain(self.recipe, "the recipe"),
            "pins": [pin.document() for pin in self.pins],
            "runtime": self.runtime,
            "explains": self.explains,
        }

    def canonical_json(self) -> str:
        return canonical_json(self.document())

    @property
    def method_id(self) -> uuid.UUID:
        """``uuid5(METHOD_NAMESPACE, canonical JSON)``: the same spec, the same id."""
        return uuid.uuid5(METHOD_NAMESPACE, self.canonical_json())

    @property
    def spec_sha256(self) -> Digest:
        """The sha256 of the same canonical bytes, at full strength."""
        return hashlib.sha256(self.canonical_json().encode("ascii")).hexdigest()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MethodSpec):
            return NotImplemented
        return self.canonical_json() == other.canonical_json()

    def __hash__(self) -> int:
        return hash(self.canonical_json())


def derive_method_id(spec: MethodSpec) -> uuid.UUID:
    """The id a spec derives, spelled like ``derive_embedding_config_id``."""
    return spec.method_id
