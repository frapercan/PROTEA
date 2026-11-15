"""The operation descriptor: what every registered operation declares (ADR-D60, decision 6).

``OperationSpec(domain, kind, queue, purity, resource, family, publishes_to, status)``
says where an operation runs and what it needs. Four things derive from it:

- the queue ``POST /jobs`` accepts for an operation (:func:`refuse_job_request`);
- the consumer a worker runs on a queue (:func:`message_queues`, read by
  ``scripts/worker.py``);
- the operations ``POST /jobs`` refuses outright, because they never get a Job row;
- the operations a cluster pilot may run (:func:`pilot_operations`).

The specs sit beside the registration in :mod:`protea.core.operation_catalog`, and
:class:`~protea.core.contracts.registry.OperationRegistry` refuses an operation that
declares none. A spec refuses its own inconsistent fields when it is built.

The fields, in the order the record writes them:

- ``domain``: what the operation is part of. ``universe`` is the data every family
  reads by id (ADR-D60, decision 1); then ``representation``, ``method``,
  ``evaluation`` and ``infrastructure``.
- ``kind``: how it reaches a worker. A ``job`` or a ``coordinator`` is a Job row whose
  id travels on the broker. A ``message_stage``, a ``write`` or a ``file_stage``
  travels as its own payload and never gets a row.
- ``queue``: one of :data:`protea.core.queues.WORK_QUEUES`.
- ``purity``: what it reads besides its message. ``database`` opens a session;
  ``files`` reads or writes the disk of the node it runs on, so it runs only where
  those files are; ``pure`` needs its message and the artifact store and nothing
  else, so any node can run it.
- ``resource``: what bounds it: ``cpu``, ``gpu`` or ``network``.
- ``family``: ``None`` for an operation every family shares, or the one family it
  serves (ADR-D60, decision 2).
- ``publishes_to``: the queues it sends work to.
- ``status``: ``active``; ``review``, kept until the author decides (plan section C);
  or ``mirror``, which measures what a source the recipe does not use would add.

A spec states what the code does today, not what a later phase will make it do:
``compute_embeddings_batch`` reads its configuration from the database, so its purity
is ``database`` until phase 4 makes it pure.

The module imports nothing from the database stack, so a node without it can read it.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Literal, get_args

from protea.core import queues

Domain = Literal["universe", "representation", "method", "evaluation", "infrastructure"]
Kind = Literal["job", "coordinator", "message_stage", "write", "file_stage"]
Purity = Literal["database", "pure", "files"]
Resource = Literal["cpu", "gpu", "network"]
Family = Literal["embedding-reranker", "world-model", "explainable-units"]
Status = Literal["active", "review", "mirror"]

DOMAINS: tuple[str, ...] = get_args(Domain)
KINDS: tuple[str, ...] = get_args(Kind)
PURITIES: tuple[str, ...] = get_args(Purity)
RESOURCES: tuple[str, ...] = get_args(Resource)
FAMILIES: tuple[str, ...] = get_args(Family)
STATUSES: tuple[str, ...] = get_args(Status)

#: Kinds whose broker message is a Job id: a ``QueueConsumer`` claims the row, and
#: ``POST /jobs`` is the door that creates it.
JOB_KINDS: frozenset[str] = frozenset({"job", "coordinator"})
#: Kinds whose broker message is the operation's own payload, with no Job row: an
#: ``OperationConsumer`` runs it, and ``POST /jobs`` refuses it.
MESSAGE_KINDS: frozenset[str] = frozenset({"message_stage", "write", "file_stage"})

#: The domains every family shares, so their operations name no family: one universe
#: and one evaluator are what make the three families comparable (ADR-D60).
SHARED_DOMAINS: frozenset[str] = frozenset({"universe", "evaluation", "infrastructure"})

#: The one queue a cluster pilot consumes (ADR-D60, decision 4). Nothing else may stay
#: on it once phase 4 lands; ``tests/test_the_pilot_queue_carries_one_operation.py``
#: lists what still does, each with the phase that removes it.
PILOT_QUEUE = queues.EMBEDDINGS_BATCH


@dataclass(frozen=True)
class OperationSpec:
    """Where one operation runs and what it needs. See the module docstring."""

    domain: Domain
    kind: Kind
    queue: str
    purity: Purity
    resource: Resource
    family: Family | None = None
    publishes_to: tuple[str, ...] = ()
    status: Status = "active"

    def __post_init__(self) -> None:
        problems = spec_problems(self)
        if problems:
            raise ValueError(f"inconsistent OperationSpec {self!r}: " + "; ".join(problems))

    @property
    def carries_jobs(self) -> bool:
        """True when the broker carries this operation as a Job id."""
        return self.kind in JOB_KINDS


def _enumerated(spec: OperationSpec) -> list[str]:
    """The fields whose value lies outside its closed vocabulary."""
    closed = (
        ("domain", spec.domain, DOMAINS),
        ("kind", spec.kind, KINDS),
        ("purity", spec.purity, PURITIES),
        ("resource", spec.resource, RESOURCES),
        ("status", spec.status, STATUSES),
        ("family", spec.family, (None, *FAMILIES)),
    )
    return [
        f"{name} {value!r} is not one of {allowed}"
        for name, value, allowed in closed
        if value not in allowed
    ]


def _routing(spec: OperationSpec) -> list[str]:
    """The queue and the queues it publishes to must be queues PROTEA declares."""
    problems = []
    if spec.queue not in queues.WORK_QUEUES:
        problems.append(f"queue {spec.queue!r} is not a work queue of protea.core.queues")
    if not isinstance(spec.publishes_to, tuple):
        return [*problems, "publishes_to must be a tuple of queue names"]
    unknown = [q for q in spec.publishes_to if q not in queues.WORK_QUEUES]
    if unknown:
        problems.append(f"publishes_to names undeclared queues: {unknown}")
    if len(set(spec.publishes_to)) != len(spec.publishes_to):
        problems.append("publishes_to names a queue twice")
    return problems


def _coherence(spec: OperationSpec) -> list[str]:
    """The rules that tie one field to another."""
    problems = []
    if spec.kind in (*JOB_KINDS, "write") and spec.purity != "database":
        problems.append(
            f"a {spec.kind} lives in the database (its Job row or its write), "
            "so its purity is database"
        )
    if spec.kind == "file_stage" and spec.purity == "database":
        problems.append("a file stage runs away from the database, so its purity is pure or files")
    if spec.kind == "coordinator" and not spec.publishes_to:
        problems.append("a coordinator fans its work out, so it publishes to at least one queue")
    if spec.domain in SHARED_DOMAINS and spec.family is not None:
        problems.append(f"every family shares the {spec.domain} domain, so it names no family")
    return problems


def spec_problems(spec: OperationSpec) -> list[str]:
    """Everything inconsistent in one spec, empty when it holds together."""
    return [*_enumerated(spec), *_routing(spec), *_coherence(spec)]


def message_queues(specs: Iterable[OperationSpec]) -> frozenset[str]:
    """The queues whose messages are operation payloads: an ``OperationConsumer`` serves them."""
    return frozenset(spec.queue for spec in specs if not spec.carries_jobs)


def job_queues(specs: Iterable[OperationSpec]) -> frozenset[str]:
    """The queues whose messages are Job ids: a ``QueueConsumer`` serves them."""
    return frozenset(spec.queue for spec in specs if spec.carries_jobs)


def pilot_operations(specs: Mapping[str, OperationSpec]) -> frozenset[str]:
    """The operations a pilot may run: the pure ones on :data:`PILOT_QUEUE`.

    Derived, never listed: an operation enters by becoming pure, which is what lets a
    node without the database run it.
    """
    return frozenset(
        name for name, spec in specs.items() if spec.queue == PILOT_QUEUE and spec.purity == "pure"
    )


@dataclass(frozen=True)
class JobRequestRefusal:
    """Why ``POST /jobs`` creates no job for a request, in terms a caller can act on."""

    error: Literal["unknown_operation", "not_a_job", "wrong_queue"]
    message: str
    expected_queue: str | None = None


def is_probe(spec: OperationSpec) -> bool:
    """An infrastructure job exists to prove that a job consumer answers.

    So ``POST /jobs`` accepts it on any queue a job consumer serves, besides its own:
    ``scripts/test_deploy_e2e.sh`` posts ``ping`` to protea.jobs, because its compose
    slice runs no worker on protea.ping.
    """
    return spec.domain == "infrastructure" and spec.kind == "job"


def refuse_job_request(
    specs: Mapping[str, OperationSpec], operation: str, queue: str
) -> JobRequestRefusal | None:
    """Why ``POST /jobs`` must not create this job, or None when the spec admits it."""
    spec = specs.get(operation)
    if spec is None:
        return JobRequestRefusal(
            "unknown_operation", f"{operation!r} is not a registered operation"
        )
    if not spec.carries_jobs:
        return JobRequestRefusal(
            "not_a_job",
            f"{operation} is a {spec.kind}: it travels as its own message on {spec.queue} "
            "and never gets a Job row",
        )
    if queue == spec.queue or (is_probe(spec) and queue in job_queues(specs.values())):
        return None
    return JobRequestRefusal(
        "wrong_queue",
        f"{operation} runs on {spec.queue}, not on {queue!r}",
        expected_queue=spec.queue,
    )
