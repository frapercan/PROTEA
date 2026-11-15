"""A method, its training windows and its pins (ADR-D60, decision 3).

A ``method`` row is the stored form of a ``MethodSpec``: its family, code,
instance cut, recipe, runtime and whether it explains, with its pins in
``method_pin``. Its id is the uuid5 the spec derives and ``spec_sha256`` the
sha256 of the same canonical JSON (``protea.core.contracts.method``), so the row
is identity and is never updated: a method that changes is another method.

``method_training_window`` records the windows its fitted pins were trained on,
by evaluation set id, with the role ``fit`` or ``holdout``. The windows are not
part of the id; the pins are, and a retrained booster is new bytes under a new
digest, so a method's windows are a fact about its pins rather than an input to
its identity. The leakage guard reads both tables
(``protea.core._evaluation_leakage``).

THE ID IS DERIVED, AND A LISTENER HOLDS IT TO THAT. ``before_insert`` rebuilds
the spec from the row and its pins and refuses a row whose id or digest the spec
does not derive, the lesson ``embedding_config`` paid for with five ids no second
machine could reproduce. Build rows with :meth:`Method.from_spec`, which attaches
the pins and windows through the relationships the listener reads.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import datetime
from typing import Any, cast

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    event,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.core.contracts.method import (
    RUNTIMES,
    WINDOW_ROLES,
    Family,
    InstanceCut,
    MethodSpec,
    Pin,
    Runtime,
    TrainingWindow,
)
from protea.infrastructure.orm.base import Base

_JSONB = JSONB().with_variant(JSON(), "sqlite")

#: The family slugs the ``family`` column accepts, in the enum's order.
FAMILY_VALUES: tuple[str, ...] = tuple(f.value for f in Family)


def closed_over(column: str, values: tuple[str, ...]) -> str:
    """The CHECK expression that closes ``column`` over ``values``."""
    listed = ", ".join(f"'{v}'" for v in values)
    return f"{column} IN ({listed})"


class Method(Base):
    """A method with an identity of its own, as its spec derives it."""

    __tablename__ = "method"
    __table_args__ = (
        UniqueConstraint("spec_sha256", name="uq_method_spec_sha256"),
        CheckConstraint(closed_over("family", FAMILY_VALUES), name="ck_method_family"),
        CheckConstraint(closed_over("runtime", RUNTIMES), name="ck_method_runtime"),
        CheckConstraint("instance_release > 0", name="ck_method_instance_release"),
        CheckConstraint("length(spec_sha256) = 64", name="ck_method_spec_sha256"),
    )

    #: Derived from the spec, never defaulted: see the module docstring.
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    family: Mapped[str] = mapped_column(String(32), nullable=False)
    #: The method's semantic code, a lowercase slug. Not unique on its own: one
    #: code at two instance cuts is two methods.
    code: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    #: The release ordinal of the instance cut (211, 227).
    instance_release: Mapped[int] = mapped_column(Integer, nullable=False)
    ontology_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ontology_snapshot.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    recipe: Mapped[dict[str, Any]] = mapped_column(_JSONB, nullable=False)
    runtime: Mapped[str] = mapped_column(String(16), nullable=False)
    explains: Mapped[bool] = mapped_column(Boolean, nullable=False)
    spec_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    training_windows: Mapped[list[MethodTrainingWindow]] = relationship(
        back_populates="method", cascade="all, delete-orphan"
    )
    pins: Mapped[list[MethodPin]] = relationship(
        back_populates="method", cascade="all, delete-orphan"
    )

    def spec(self) -> MethodSpec:
        """The spec this row and its pins hold, rebuilt and validated."""
        return MethodSpec(
            family=Family(self.family),
            code=self.code,
            instance=InstanceCut(self.instance_release, self.ontology_snapshot_id),
            recipe=self.recipe if self.recipe is not None else {},
            pins=tuple(Pin(p.role, p.sha256, tuple(p.refs or ())) for p in self.pins),
            runtime=cast(Runtime, self.runtime),
            explains=self.explains,
        )

    @classmethod
    def from_spec(cls, spec: MethodSpec, windows: Sequence[TrainingWindow] = ()) -> Method:
        """The rows of ``spec``, with ``windows`` as its training windows.

        Pass ``TrainingIdentity.all_windows()`` for a trained method and nothing
        for one that was never fitted. Only the windows are stored: the rest of
        a training identity has no column yet.
        """
        method = cls(
            id=spec.method_id,
            family=spec.family.value,
            code=spec.code,
            instance_release=spec.instance.release,
            ontology_snapshot_id=spec.instance.ontology_snapshot_id,
            recipe=spec.document()["recipe"],
            runtime=spec.runtime,
            explains=spec.explains,
            spec_sha256=spec.spec_sha256,
        )
        method.pins = [
            MethodPin(role=p.role, sha256=p.sha256, refs=list(p.refs)) for p in spec.pins
        ]
        method.training_windows = [
            MethodTrainingWindow(evaluation_set_id=w.evaluation_set_id, role=w.role)
            for w in windows
        ]
        return method


class MethodTrainingWindow(Base):
    """One window a method's fitted pins were trained on, by evaluation set id."""

    __tablename__ = "method_training_window"
    __table_args__ = (
        CheckConstraint(closed_over("role", WINDOW_ROLES), name="ck_method_training_window_role"),
        # A training identity holds at most one holdout window.
        Index(
            "uq_method_training_window_one_holdout",
            "method_id",
            unique=True,
            postgresql_where=text("role = 'holdout'"),
            sqlite_where=text("role = 'holdout'"),
        ),
    )

    method_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("method.id", ondelete="CASCADE"), primary_key=True
    )
    #: A window a method was trained on cannot be deleted from under it.
    evaluation_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("evaluation_set.id", ondelete="RESTRICT"),
        primary_key=True,
        index=True,
    )
    role: Mapped[str] = mapped_column(String(8), nullable=False)

    method: Mapped[Method] = relationship(back_populates="training_windows")


class MethodPin(Base):
    """Bytes a method depends on, by sha256, with the references that locate them.

    ``refs`` is the pin's references as a JSON array of strings, sorted. A
    reference spelled ``"<table>:<uuid>"`` names a platform row the leakage
    guard can date (``DATED_REFERENCES``); any other is a locator.
    """

    __tablename__ = "method_pin"
    __table_args__ = (CheckConstraint("length(sha256) = 64", name="ck_method_pin_sha256"),)

    method_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("method.id", ondelete="CASCADE"), primary_key=True
    )
    role: Mapped[str] = mapped_column(String(64), primary_key=True)
    sha256: Mapped[str] = mapped_column(String(64), primary_key=True, index=True)
    refs: Mapped[list[str]] = mapped_column(_JSONB, nullable=False)

    method: Mapped[Method] = relationship(back_populates="pins")


@event.listens_for(Method, "before_insert")
def _the_id_derives_from_the_spec(_mapper: Any, _connection: Any, target: Method) -> None:
    """Fill a missing id or digest from the spec, and refuse one it does not derive.

    An omitted recipe is the empty recipe, resolved before the hash so the id
    describes what the INSERT stores.
    """
    if target.recipe is None:
        target.recipe = {}
    spec = target.spec()
    if target.id is None:
        target.id = spec.method_id
    if target.spec_sha256 is None:
        target.spec_sha256 = spec.spec_sha256
    if target.id != spec.method_id or target.spec_sha256 != spec.spec_sha256:
        raise ValueError(
            f"method {target.code!r} carries the id {target.id} and the digest "
            f"{target.spec_sha256}, and its own spec derives {spec.method_id} and "
            f"{spec.spec_sha256}. A method id is derived, never assigned: build the row with "
            "Method.from_spec, which also attaches the pins the derivation reads."
        )
