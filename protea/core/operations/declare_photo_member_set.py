"""Declare a set of one photo's members, chosen by their admission flags.

A breadth level of a donor pool (experimental; experimental or reviewed; plus curated
inference) and the members a head is trained on are sets of one photo's members,
chosen by the sets each member entered by (a
:class:`protea.core.cut_photo.members.FlagRule`: ``any_of``, ``all_of``, ``none_of``),
and optionally a sample of them (``sample``: the first ``size`` in ascending sha256 of
``seed|photo|accession``, :mod:`protea.core.sampling`).

The entries are the chosen members with a sequence at the cut (a ``sequence`` row
under their ``sequence_md5``, as ``member_sequences`` joins them), each with that
sequence's id; a member the rule chooses without one is counted and named, never
entered, and a sample is drawn among the members with a sequence.

One SELECT names the chosen members, and every count, the artifact and the entries
come from it: the dry run counts it, the artifact (the sorted accession list, one per
line, whose sha256 is the population digest of
:func:`protea.core.query_population.population_of`) is aggregated from it by the
database, and the entries are written from it by one ``INSERT ... SELECT`` in the
job's transaction, never one object per entry. The statements a run issues do not
grow with the members it declares.

It refuses before writing anything unless the photo exists, the sample asks for
fewer members than the rule chooses, the rule chooses at least one member with a
sequence, and no set of the same photo holds the same bytes. The rule itself is
checked by the payload: a rule no member can satisfy, and the whole photo without a
sample (the photo already is that set), are refused there. ``dry_run`` returns the
counts, the digest and the members without a sequence, and writes nothing.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import Select, and_, func, insert, literal, not_, or_, select, true
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement

from protea.core.code_revision import code_revision
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.cut_photo.members import (
    ADMISSION_FLAGS,
    ADMISSION_ROUTES,
    FlagRule,
    breadth_level_of,
)
from protea.core.sampling import HASH_ORDER_SEPARATOR
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto, CutPhotoMember
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

__all__ = [
    "AdmissionFlag",
    "DeclarePhotoMemberSetOperation",
    "DeclarePhotoMemberSetPayload",
    "MemberSample",
    "PhotoMemberSetRefused",
    "chosen_members",
    "photo_members_key_for",
]

#: The admission flags a rule names: :data:`protea.core.cut_photo.members.ADMISSION_FLAGS`.
AdmissionFlag = Literal["truth", "superset", "curated", "neighbour"]

#: How many members without a sequence the job result names; the count is always whole.
NAMED_WITHOUT_SEQUENCE = 1000


class PhotoMemberSetRefused(ValueError):
    """A set of photo members the operation will not declare, with the reason."""


class MemberSample(BaseModel):
    """A hash-ordered sample: the first ``size`` members in ascending sha256 of ``seed|photo|accession``."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    size: int = Field(ge=1)
    seed: str = Field(min_length=1)


class DeclarePhotoMemberSetPayload(ProteaPayload, frozen=True):
    """The photo, the flag rule, an optional sample, and what to call the set."""

    cut_photo_id: str
    any_of: list[AdmissionFlag] = Field(default_factory=list)
    all_of: list[AdmissionFlag] = Field(default_factory=list)
    none_of: list[AdmissionFlag] = Field(default_factory=list)
    sample: MemberSample | None = None
    dry_run: bool = False
    output_name: str

    @field_validator("cut_photo_id", mode="before")
    @classmethod
    def must_be_a_uuid(cls, v: Any) -> str:
        return str(uuid.UUID(str(v)))

    @field_validator("output_name")
    @classmethod
    def a_name_a_reader_can_use(cls, value: str) -> str:
        name = value.strip()
        if not name or "/" in name:
            raise ValueError(
                "output_name is a non-empty name without '/': the store key is derived"
            )
        return name

    @model_validator(mode="after")
    def a_rule_that_chooses_a_set(self) -> DeclarePhotoMemberSetPayload:
        rule = self.rule
        if rule.admits_every_member and self.sample is None:
            raise ValueError(
                "the rule chooses every member, which the photo already is: a donor pool of "
                "every member needs no set"
            )
        return self

    @property
    def photo_id(self) -> uuid.UUID:
        return uuid.UUID(self.cut_photo_id)

    @property
    def rule(self) -> FlagRule:
        return FlagRule(
            any_of=tuple(self.any_of), all_of=tuple(self.all_of), none_of=tuple(self.none_of)
        )


@dataclass(frozen=True)
class _Chosen:
    """What the chosen members add up to, read in one statement."""

    entries: int
    by_route: dict[str, int]
    body: bytes

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.body).hexdigest()


@dataclass(frozen=True)
class _Population:
    """The members the rule chooses, before a sample: with a sequence, and without one by name."""

    with_sequence: int
    without_sequence: tuple[str, ...]


def photo_members_key_for(photo_id: uuid.UUID, digest: str) -> str:
    """Where a set's accession list lives in the store: the one spelling."""
    return f"query-sets/photo-members/{photo_id}/{digest}/accessions.txt"


def publish(key: str, blob: bytes) -> str:
    """Put the bytes in the store; return their URI."""
    store = get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))
    return str(store.put(key, blob))


def _rule_clause(rule: FlagRule) -> ColumnElement[bool]:
    """The flag rule over ``cut_photo_member``'s flag columns."""
    flag = {name: getattr(CutPhotoMember, name) for name in ADMISSION_FLAGS}
    clauses: list[ColumnElement[bool]] = []
    if rule.any_of:
        clauses.append(or_(*(flag[name] for name in rule.any_of)))
    clauses.extend(flag[name] for name in rule.all_of)
    clauses.extend(not_(flag[name]) for name in rule.none_of)
    return and_(true(), *clauses)


def chosen_members(photo_id: uuid.UUID, rule: FlagRule, sample: MemberSample | None) -> Select[Any]:
    """The SELECT of the chosen members with a sequence: accession, sequence id and flags.

    A sample takes the first ``size`` in ascending ``sha256(seed|photo|accession)``,
    the accession breaking a tie, computed by the database over the same UTF-8 text
    :func:`protea.core.sampling.hash_order_key` hashes.
    """
    query = (
        select(
            CutPhotoMember.accession.label("accession"),
            Sequence.id.label("sequence_id"),
            *(getattr(CutPhotoMember, name).label(name) for name in ADMISSION_FLAGS),
        )
        .join(Sequence, Sequence.sequence_hash == CutPhotoMember.sequence_md5)
        .where(CutPhotoMember.photo_id == photo_id, _rule_clause(rule))
    )
    if sample is None:
        return query
    prefix = HASH_ORDER_SEPARATOR.join((sample.seed, str(photo_id), ""))
    hashed = func.sha256(func.convert_to(literal(prefix) + CutPhotoMember.accession, "UTF8"))
    return query.order_by(hashed, CutPhotoMember.accession.collate("C")).limit(sample.size)


def _route_counts(chosen: Any) -> list[ColumnElement[int]]:
    """One count per route: a member's first flag in the order of ``ADMISSION_FLAGS``."""
    counts = []
    for position, flag in enumerate(ADMISSION_FLAGS):
        earlier = (not_(chosen.c[name]) for name in ADMISSION_FLAGS[:position])
        counts.append(func.count().filter(and_(*earlier, chosen.c[flag])).label(flag))
    return counts


def _chosen(session: Session, chosen: Any) -> _Chosen:
    """The entries, the counts by route and the artifact's bytes, in one statement."""
    listing = func.string_agg(
        chosen.c.accession, aggregate_order_by(literal("\n"), chosen.c.accession.collate("C"))
    )
    row = session.execute(select(func.count(), *_route_counts(chosen), listing)).one()
    entries, *routes, joined = row
    return _Chosen(
        entries=int(entries),
        by_route={
            ADMISSION_ROUTES[f]: int(n) for f, n in zip(ADMISSION_FLAGS, routes, strict=True)
        },
        body=f"{joined}\n".encode() if joined else b"",
    )


def _population(session: Session, photo_id: uuid.UUID, rule: FlagRule) -> _Population:
    """The members the rule chooses: how many have a sequence, and which do not."""
    without = CutPhotoMember.accession.collate("C")
    row = session.execute(
        select(
            func.count(Sequence.id),
            func.array_agg(aggregate_order_by(CutPhotoMember.accession, without)).filter(
                Sequence.id.is_(None)
            ),
        )
        .select_from(CutPhotoMember)
        .outerjoin(Sequence, Sequence.sequence_hash == CutPhotoMember.sequence_md5)
        .where(CutPhotoMember.photo_id == photo_id, _rule_clause(rule))
    ).one()
    return _Population(with_sequence=int(row[0]), without_sequence=tuple(row[1] or ()))


def _declared_already(session: Session, photo_id: uuid.UUID, digest: str) -> uuid.UUID | None:
    return session.execute(
        select(QuerySet.id).where(
            QuerySet.source_kind == "photo_members",
            QuerySet.cut_photo_id == photo_id,
            QuerySet.content_sha256 == digest,
        )
    ).scalar_one_or_none()


def _sampled_in_words(p: DeclarePhotoMemberSetPayload, population: _Population) -> str:
    if p.sample is None:
        return ""
    return (
        f", sampled: the first {p.sample.size:,} of {population.with_sequence:,} in ascending "
        f"sha256 of 'seed|photo|accession' with seed '{p.sample.seed}'"
    )


def describe(
    photo: CutPhoto, p: DeclarePhotoMemberSetPayload, population: _Population, chosen: _Chosen
) -> str:
    """The set's description, derived from what was counted, never typed."""
    level = breadth_level_of(p.rule)
    named = population.without_sequence
    missing = (
        f"{len(named)} members the rule chooses have no sequence at the cut and are not entered"
        + (f" ({', '.join(named)})" if 0 < len(named) <= 20 else "")
    )
    return (
        f"The {chosen.entries:,} members of photo {photo.short_name} (cut {photo.cut_release}) "
        f"{p.rule.in_words()}"
        + (f", breadth level {level}" if level else "")
        + f"{_sampled_in_words(p, population)}. By route (a member's first flag in the order "
        "truth, superset, curated, neighbour): "
        + ", ".join(f"{n:,} {route}" for route, n in chosen.by_route.items())
        + f". {missing}. The artifact is the sorted accession list, one per line, sha256 "
        f"{chosen.digest}."
    )


def _refuse_what_cannot_be_declared(
    p: DeclarePhotoMemberSetPayload, population: _Population, chosen: _Chosen
) -> None:
    if p.sample is not None and p.sample.size >= population.with_sequence:
        raise PhotoMemberSetRefused(
            f"the sample asks for {p.sample.size:,} of the {population.with_sequence:,} members "
            "the rule chooses with a sequence: that is every one of them, so declare the set "
            "without a sample"
        )
    if chosen.entries == 0:
        raise PhotoMemberSetRefused(
            f"the rule ({p.rule.in_words()}) chooses no member with a sequence in this photo"
        )


def _declare(
    session: Session,
    p: DeclarePhotoMemberSetPayload,
    photo: CutPhoto,
    population: _Population,
    chosen: _Chosen,
    job_id: uuid.UUID | None,
) -> QuerySet:
    """The row, then its entries in one INSERT ... SELECT of the same chosen members."""
    uri = publish(photo_members_key_for(photo.id, chosen.digest), chosen.body)
    row = QuerySet(
        name=f"{p.output_name}-{chosen.digest[:12]}",
        source_kind="photo_members",
        cut_photo_id=photo.id,
        description=describe(photo, p, population, chosen),
        artifact_uri=uri,
        content_sha256=chosen.digest,
        job_id=job_id,
        code_revision=code_revision(),
    )
    session.add(row)
    session.flush()
    members = chosen_members(photo.id, p.rule, p.sample).subquery("chosen_members")
    session.execute(
        insert(QuerySetEntry).from_select(
            ["query_set_id", "sequence_id", "accession"],
            select(
                literal(row.id, PG_UUID(as_uuid=True)), members.c.sequence_id, members.c.accession
            ),
        )
    )
    written = session.execute(
        select(func.count()).where(QuerySetEntry.query_set_id == row.id)
    ).scalar_one()
    if written != chosen.entries:
        raise PhotoMemberSetRefused(
            f"the insert wrote {written} entries where the same SELECT counted "
            f"{chosen.entries}: the photo changed under the job"
        )
    return row


class DeclarePhotoMemberSetOperation:
    name = "declare_photo_member_set"
    description = (
        "Declare a set of one photo's members chosen by their admission flags (any_of, "
        "all_of, none_of), optionally a hash-ordered sample of them: a breadth level of a "
        "donor pool, or the members a head is trained on. The entries are the chosen "
        "members with a sequence at the cut, written by one INSERT ... SELECT; members "
        "without one are counted and named. dry_run writes nothing."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        photo = str(payload.get("cut_photo_id") or "")[:8]
        dry = " (dry run)" if payload.get("dry_run") else ""
        return f"members of photo {photo} by flag{dry}" if photo else ""

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = DeclarePhotoMemberSetPayload.model_validate(contract_payload(payload))
        photo = session.get(CutPhoto, p.photo_id)
        if photo is None:
            raise PhotoMemberSetRefused(f"no cut photo {p.photo_id}")
        population = _population(session, photo.id, p.rule)
        chosen = _chosen(session, chosen_members(photo.id, p.rule, p.sample).subquery("chosen"))
        _refuse_what_cannot_be_declared(p, population, chosen)
        existing = _declared_already(session, photo.id, chosen.digest)
        if existing is not None and not p.dry_run:
            raise PhotoMemberSetRefused(
                f"photo {photo.short_name} already has these members declared as query set "
                f"{existing} (sha256 {chosen.digest}): one photo and one set of bytes are one set"
            )
        result: dict[str, Any] = {
            "cut_photo_id": str(photo.id),
            "cut_release": photo.cut_release,
            "rule": p.rule.declared(),
            "rule_in_words": p.rule.in_words(),
            "breadth_level": breadth_level_of(p.rule),
            "sample": p.sample.model_dump() if p.sample else None,
            "members_with_sequence": population.with_sequence,
            "entries": chosen.entries,
            "by_route": chosen.by_route,
            "without_sequence": len(population.without_sequence),
            "without_sequence_accessions": list(
                population.without_sequence[:NAMED_WITHOUT_SEQUENCE]
            ),
            "content_sha256": chosen.digest,
            "dry_run": p.dry_run,
        }
        if p.dry_run:
            result["already_declared"] = str(existing) if existing else None
            emit(
                "photo_member_set.counted", f"{chosen.entries:,} members (dry run)", result, "info"
            )
            return OperationResult(result=result)
        row = _declare(session, p, photo, population, chosen, job_id_from_payload(payload))
        result.update(query_set_id=str(row.id), name=row.name, artifact_uri=row.artifact_uri)
        emit(
            "photo_member_set.declared", f"{chosen.entries:,} members as {row.name}", result, "info"
        )
        return OperationResult(result=result)
