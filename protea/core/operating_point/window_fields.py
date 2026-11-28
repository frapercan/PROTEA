"""The fields one arm's sets may differ in across windows, listed once.

A reading pools an arm's sets at several windows (the declared threshold of
``declare_operating_threshold``, and every fit that reads the fitting windows of
one arm), so it has to tell one arm at three windows from three arms. The
method seal (:mod:`protea.core.method_seal`) says what a set's method is; this
module says which of those fields name the WINDOW rather than the method, and
compares everything else.

**What may differ between windows.**

- columns :data:`WINDOW_VARYING_COLUMNS`: the donor corpus, the ontology and the
  query set, each built at the window's opening cut;
- ``meta`` keys :data:`WINDOW_VARYING_META`: the window a native method read,
  and the digest of the population a run was handed;
- subfields :data:`WINDOW_VARYING_SUBFIELDS`: the donor set inside the donor
  policy, compared through the rule each set was declared by (its flags and its
  sample, read from the declaring job's payload), and the statistics row
  inside an embedding transform with the numbers derived from it;
- the inputs of an applied set (``input_identities``), each input compared
  through the same masking, recursively;
- for a set that names a method: the instance (release and snapshot) and the
  pins whose role is dated by the cut (:data:`WINDOW_VARYING_PIN_ROLES`); the
  rest of the spec (family, code, recipe, runtime, explanations and every other
  pin) must agree.

Everything else is required to agree, field by field, and a refusal names each
field that does not with its value at every window. A field in no list is
compared, so a new identity field is held equal until it is listed here.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from protea.core.method_seal import method_identity

if TYPE_CHECKING:  # pragma: no cover - typing only
    from sqlalchemy.orm import Session

#: ``prediction_set`` columns built at each window's opening cut.
WINDOW_VARYING_COLUMNS: tuple[str, ...] = (
    "annotation_set_id",
    "ontology_snapshot_id",
    "query_set_id",
)

#: ``prediction_set.meta`` keys that name the window a set read.
WINDOW_VARYING_META: tuple[str, ...] = ("evaluation_set_id", "query_population")

#: Inside an identity field, the subfields dated by the cut. The donor set is
#: compared through its declared rule (:attr:`ArmAtWindow.donor_rule`), never
#: dropped; the statistics row of a transform and the numbers read from it vary
#: with the photo, while the transform's name does not.
WINDOW_VARYING_SUBFIELDS: Mapping[str, tuple[str, ...]] = {
    "donor_policy": ("donor_set_id",),
    "embedding_transform": (
        "statistics_id",
        "statistics_sha256",
        "n",
        "shrinkage_intensity",
        "removed_directions",
    ),
}

#: The pin roles a method's spec dates by the cut it reads.
WINDOW_VARYING_PIN_ROLES: tuple[str, ...] = (
    "photo",
    "donor-set",
    "homology-table",
    "homology-self-scores",
    "space-statistics",
    "segment-families",
    "head-weights",
)

#: What a window-varying value is replaced by before the comparison.
VARIES_BY_WINDOW = "<varies by window>"


class WindowArmsDiffer(ValueError):
    """Sets pooled as one arm at several windows differ in a field that names the method."""


@dataclass(frozen=True)
class ArmAtWindow:
    """One set of an arm at one window, with what the masking needs to read beside it.

    ``row`` is the whole ``prediction_set`` row (every column, ``meta`` as a
    mapping); ``spec`` the method's spec document when the set names a method;
    ``donor_rule`` the rule its donor set was declared by; ``inputs`` the input
    sets of an applied set, by label.
    """

    row: Mapping[str, Any]
    spec: Mapping[str, Any] | None = None
    donor_rule: Mapping[str, Any] | None = None
    inputs: Mapping[str, ArmAtWindow] = field(default_factory=dict)


def _masked_spec(spec: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    """The spec document without its instance and with its cut-dated pins reduced to their role."""
    if spec is None:
        return None
    pins = [
        {"role": pin["role"]} if pin["role"] in WINDOW_VARYING_PIN_ROLES else dict(pin)
        for pin in spec.get("pins", ())
    ]
    masked = {key: value for key, value in spec.items() if key not in ("instance", "pins")}
    masked["pins"] = sorted(pins, key=lambda pin: (pin["role"], pin.get("sha256", "")))
    return masked


def _masked_block(name: str, value: Any, arm: ArmAtWindow) -> Any:
    """One identity field with its window-varying subfields masked."""
    varying = WINDOW_VARYING_SUBFIELDS.get(name)
    if varying is None or not isinstance(value, Mapping):
        return value
    masked = {key: (VARIES_BY_WINDOW if key in varying else item) for key, item in value.items()}
    if name == "donor_policy" and value.get("donor_set_id") is not None:
        masked["donor_set_id"] = {"declared_rule": dict(arm.donor_rule or {})}
    return masked


def masked_identity(arm: ArmAtWindow) -> dict[str, Any]:
    """The arm's method identity with every window-varying field masked, inputs recursively."""
    identity = method_identity(arm.row, name=str(arm.row.get("id")))
    for name in (*WINDOW_VARYING_COLUMNS, *WINDOW_VARYING_META):
        if name in identity:
            identity[name] = VARIES_BY_WINDOW
    for name in WINDOW_VARYING_SUBFIELDS:
        if name in identity:
            identity[name] = _masked_block(name, identity[name], arm)
    if identity.get("method_id") is not None:
        identity["method_id"] = _masked_spec(arm.spec)
    if identity.get("input_identities") is not None:
        identity["input_identities"] = {
            label: masked_identity(arm.inputs[label]) if label in arm.inputs else VARIES_BY_WINDOW
            for label in sorted(identity["input_identities"])
        }
    return identity


def assert_one_arm(arms: Mapping[str, ArmAtWindow]) -> None:
    """Refuse unless the sets of ``arms`` (by window) are one arm, naming each field that differs."""
    if len(arms) < 2:
        return
    masked = {window: masked_identity(arm) for window, arm in sorted(arms.items())}
    fields = sorted(set().union(*(identity.keys() for identity in masked.values())))
    differ = [
        name
        for name in fields
        if len({repr(identity.get(name)) for identity in masked.values()}) > 1
    ]
    if differ:
        lines = "\n".join(
            f"  {name}: "
            + ", ".join(
                f"{window[:8]}={identity.get(name)!r}" for window, identity in masked.items()
            )
            for name in differ
        )
        raise WindowArmsDiffer(
            f"the sets of windows {sorted(masked)} are not one arm: they differ in {differ}, "
            f"which name the method rather than the window.\n{lines}\n"
            "A threshold pooled over them would be a threshold of no arm. The fields an arm "
            "may vary in across windows are listed in protea.core.operating_point.window_fields."
        )


_PREDICTION_SET_SQL = "SELECT * FROM prediction_set WHERE id = CAST(:id AS uuid)"


def arm_at_window(session: Session, prediction_set_id: str | uuid.UUID) -> ArmAtWindow:
    """Read one set as :class:`ArmAtWindow`: its row, its method's spec, its donor rule, its inputs."""
    from sqlalchemy import text

    row = (
        session.execute(text(_PREDICTION_SET_SQL), {"id": str(prediction_set_id)})
        .mappings()
        .first()
    )
    if row is None:
        raise WindowArmsDiffer(f"prediction set {prediction_set_id} is not in prediction_set")
    row = dict(row)
    meta = row.get("meta") or {}
    policy = meta.get("donor_policy") if isinstance(meta, Mapping) else None
    donor_set = policy.get("donor_set_id") if isinstance(policy, Mapping) else None
    inputs = meta.get("input_sets") if isinstance(meta, Mapping) else None
    return ArmAtWindow(
        row=row,
        spec=_spec_document(session, row.get("method_id")),
        donor_rule=None if donor_set is None else declared_rule(session, donor_set),
        inputs={
            str(label): arm_at_window(session, set_id) for label, set_id in (inputs or {}).items()
        },
    )


def _spec_document(session: Session, method_id: Any) -> Mapping[str, Any] | None:
    if method_id is None:
        return None
    from protea.infrastructure.orm.models.method.method import Method

    method = session.get(Method, uuid.UUID(str(method_id)))
    if method is None:
        raise WindowArmsDiffer(f"method {method_id} is not in method")
    return method.spec().document()


#: The keys of a photo-member set's declaring payload that say which members it holds.
DECLARED_RULE_KEYS: tuple[str, ...] = ("any_of", "all_of", "none_of", "sample")


def declared_rule(session: Session, query_set_id: Any) -> dict[str, Any]:
    """The rule a donor set was declared by: the flags and the sample of its declaring job."""
    from sqlalchemy import text

    payload = session.execute(
        text(
            "SELECT j.payload FROM query_set q JOIN job j ON j.id = q.job_id "
            "WHERE q.id = CAST(:id AS uuid)"
        ),
        {"id": str(query_set_id)},
    ).scalar_one_or_none()
    if payload is None:
        raise WindowArmsDiffer(
            f"donor set {query_set_id} has no declaring job to read its rule from, so whether "
            "two windows' donor sets are one rule cannot be read"
        )
    return {key: payload.get(key) for key in DECLARED_RULE_KEYS}


__all__ = [
    "DECLARED_RULE_KEYS",
    "VARIES_BY_WINDOW",
    "WINDOW_VARYING_COLUMNS",
    "WINDOW_VARYING_META",
    "WINDOW_VARYING_PIN_ROLES",
    "WINDOW_VARYING_SUBFIELDS",
    "ArmAtWindow",
    "WindowArmsDiffer",
    "arm_at_window",
    "assert_one_arm",
    "declared_rule",
    "masked_identity",
]
