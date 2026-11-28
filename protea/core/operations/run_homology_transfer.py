"""Score a window's targets from the donors their sequences hit: homology transfer.

Rungs 2a (``top_hit_identity``, the CAFA BLAST baseline) and 2b (the bit score
votes S1 and S2) of the literature ladder, a native method of the
explainable-units family. The rules are :mod:`protea.core.homology.transfer`.
For the window named by ``evaluation_set_id`` (t0 -> t1), which must be a window
of LAFA's frame:

1. The photo whose annotation set starts the window, its homology table
   (ADR-D61) and, for a rule that weighs by bit score, the table's self scores
   (``homology_self_scores``).
2. The targets: the window's declared target set when it has one, otherwise the
   proteins its truth scores; each by its sequence md5 in the photo.
3. Each target sequence's hits, streamed from the store and verified against the
   table's sha256, the first ``hit_cap`` by bit score when it is set; and its
   twins, the members sharing its sequence under another accession, at identity 1.
4. The donors behind each hit: the photo's members with that sequence, admitted
   by the donor policy, as the predict path reads the same contract fields
   (``donor_set_id``, a declared ``photo_members`` set of this photo;
   ``evidence_channels``, the channel of each row at the cut, read with the
   photo's channel table and the member's review state; ``evidence_codes``;
   ``exclude_reference_prefixes``); the target's own accession leaves
   (``self_exclusion`` ``accession``), and its twins with it under ``sequence``.
5. Each donor's state at t0 from its admitted rows, as LAFA's frame states an
   end: named and closed under t0's own ontology, its NOT terms of t0 and their
   descendants taken out first, roots out.
6. The rule's scores, compact (``prediction_score``) with every term id checked
   against the window's t0 snapshot, and the evidence behind each (the best
   donor, its identity and bit score, the vote's numerator and denominator, the
   voters) as the set's ``evidence_artifact``.

The donor policy and the self exclusion are written in the set's meta as a
retrieval's receipt spells them, so a native arm and a predicted arm with the
same donors agree on them; the rule and its variants are the method's recipe. It
reads labels at t0 and nothing later and fits nothing; the same method over the
same window, donors and exclusion again is a no-op.
"""

from __future__ import annotations

import tempfile
import uuid
from collections import ChainMap, Counter
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from protea_contracts.payloads import DonorPolicy, SelfExclusion
from pydantic import Field, field_validator, model_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core._artifact_identity import fetch_verified_to_file
from protea.core.contracts.method import Family, InstanceCut, MethodSpec, Pin
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.homology import transfer
from protea.core.homology.transfer import (
    Denominator,
    Hit,
    Identity,
    Multiplicity,
    Rule,
    TransferRule,
)
from protea.core.method_write import (
    SetContext,
    copy_scores,
    ensure_method,
    new_prediction_set,
    population_spelling,
    store_evidence,
    window_targets,
)
from protea.core.operations._homology_transfer_inputs import (
    DonorRows,
    donor_states,
    donors_by_md5,
    self_bits,
    sequence_lengths,
    target_md5s,
)
from protea.core.operations._homology_transfer_records import evidence_table, rows_of
from protea.core.operations.run_term_frequency import LAFA_STATE, resolve_lafa_window, term_ids
from protea.core.source_guards import SourceGuardError
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.annotation.homology_self_scores import HomologySelfScores
from protea.infrastructure.orm.models.annotation.homology_table import HomologyTable
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet
from protea.infrastructure.orm.models.query.query_set import QuerySet
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

_PROJECT_ROOT = Path(__file__).resolve().parents[3]

#: Each rule's arithmetic, as the recipe states it.
FORMULAS: dict[str, str] = {
    "top_hit_identity": "the largest identity among the voters carrying the term",
    "bitscore_vote": "sum bits x I(t) / sum bits over the voters of the denominator",
    "bitscore_identity_vote": "sum bits x ID x I(t) / sum bits x ID over the voters of the denominator",
}
#: The table's search, as each set records the deviations from the literature's BLAST.
SEARCH = (
    "the homology table's MMseqs2 search at its defaults (e-value 1e-3, at most 300 prefilter "
    "targets) with e-values against the whole photo bank, where the literature runs BLAST+ "
    "(e-value 10, 500 hits) against its training set"
)


class HomologyTransferRefused(SourceGuardError):
    """A homology transfer the operation will not write, with the reason."""


class RunHomologyTransferPayload(ProteaPayload, frozen=True):
    """The window, the rule with its variants, the donors and who leaves with the query.

    ``identity`` goes with the rules that read one, ``denominator`` with the votes,
    ``multiplicity`` ``sequence`` with the votes only. ``hit_cap`` keeps the first
    hits of a target by bit score: the table's search kept at most 300, so a cap of
    300 or more is no cap. The donor policy is the predict payload's: a declared set
    of this photo's members and the channels at the cut say who donates what.
    """

    evaluation_set_id: str
    rule: Rule
    identity: Identity | None = None
    denominator: Denominator | None = None
    multiplicity: Multiplicity = "accession"
    hit_cap: int | None = Field(default=None, ge=1, le=299)
    donor_policy: DonorPolicy = Field(default_factory=DonorPolicy)
    self_exclusion: SelfExclusion = "accession"

    @field_validator("evaluation_set_id", mode="before")
    @classmethod
    def must_be_a_uuid(cls, v: Any) -> str:
        return str(uuid.UUID(str(v)))

    @model_validator(mode="after")
    def the_variants_mean_something(self) -> RunHomologyTransferPayload:
        self.rule_of()
        if self.self_exclusion not in ("accession", "sequence"):
            raise ValueError(
                f"self_exclusion {self.self_exclusion!r}: a transfer reads every hit, so the "
                "query leaves by accession, or with its twins by sequence"
            )
        policy = self.donor_policy
        if policy.reviewed_only or policy.admitted_by_release is not None:
            raise ValueError(
                "reviewed_only reads today's review state and admitted_by_release the first "
                "release that admitted a protein, neither its membership at the cut: name the "
                "donors with donor_set_id"
            )
        if policy.donor_set_id is not None:
            uuid.UUID(policy.donor_set_id)
        return self

    def rule_of(self) -> TransferRule:
        return TransferRule(
            self.rule, self.identity, self.denominator, self.multiplicity, self.hit_cap
        )


@dataclass(frozen=True)
class Sources:
    """The rows a run reads from."""

    eset: EvaluationSet
    photo: CutPhoto
    table: HomologyTable
    scores: HomologySelfScores | None


def resolve(session: Session, p: RunHomologyTransferPayload) -> Sources:
    """The window, its photo, the photo's homology table and its self scores, or a refusal."""
    eset, photo = resolve_lafa_window(session, p.evaluation_set_id, HomologyTransferRefused)
    table = session.execute(
        select(HomologyTable).where(HomologyTable.cut_photo_id == photo.id)
    ).scalar_one_or_none()
    if table is None:
        raise HomologyTransferRefused(f"the photo of cut {photo.cut_release} has no homology table")
    scores = None
    if p.rule in transfer.READS_BITS:
        scores = session.execute(
            select(HomologySelfScores).where(HomologySelfScores.homology_table_id == table.id)
        ).scalar_one_or_none()
        if scores is None:
            raise HomologyTransferRefused(
                f"{p.rule} weighs twins by the target's self score and the homology table of cut "
                f"{photo.cut_release} has none: load them first (load_homology_self_scores)"
            )
    donor_set = p.donor_policy.donor_set_id
    if donor_set is not None:
        declared = session.get(QuerySet, uuid.UUID(donor_set))
        if (
            declared is None
            or declared.source_kind != "photo_members"
            or declared.cut_photo_id != photo.id
        ):
            raise HomologyTransferRefused(
                f"donor_set_id {donor_set} is not a photo_members set of the photo of cut "
                f"{photo.cut_release}"
            )
    if p.identity == "global" and "--seq-id-mode" in [str(x) for x in table.command]:
        raise HomologyTransferRefused(
            "global identity derives nident as round(fident x alnlen), which holds for MMseqs2's "
            "default --seq-id-mode 0; this table's search set another"
        )
    return Sources(eset, photo, table, scores)


def method_code(p: RunHomologyTransferPayload) -> str:
    """The method's code: the rule, then each variant that departs from the rule's reference.

    The references are those of the literature: local identity for the top hit,
    global for S2, the any-aspect denominator, no cap, multiplicity by accession.
    """
    rule = {
        "top_hit_identity": "top-hit",
        "bitscore_vote": "bits",
        "bitscore_identity_vote": "bits-identity",
    }
    reference = "local" if p.rule == "top_hit_identity" else "global"
    parts = ["homology", rule[p.rule]]
    if p.identity is not None and p.identity != reference:
        parts.append(p.identity)
    if p.denominator == "same_aspect":
        parts.append("same-aspect")
    if p.hit_cap is not None:
        parts.append(f"top-{p.hit_cap}")
    if p.multiplicity == "sequence":
        parts.append("by-sequence")
    return "-".join(parts)


def method_spec(src: Sources, p: RunHomologyTransferPayload) -> MethodSpec:
    """The method a run is: the rule and its variants, at the photo's cut, pinned to its tables."""
    recipe: dict[str, Any] = {
        "source": "homology transfer over the cut's homology table (ADR-D61)",
        "rule": p.rule,
        "formula": FORMULAS[p.rule],
        "multiplicity": p.multiplicity,
        "hit_cap": p.hit_cap,
        "donors": "the photo's members behind each hit by sequence md5, admitted by the donor policy",
        "twins": "the members sharing the target's sequence under another accession, at identity 1 "
        "and the target's self-alignment bit score; a twin without one is left out of a vote and "
        "counted",
        "state": f"each donor's admitted rows {LAFA_STATE}, roots out",
        "search": SEARCH,
    }
    if p.identity is not None:
        recipe["identity"] = p.identity
    if p.denominator is not None:
        recipe["denominator"] = p.denominator
    pins = [
        Pin("photo", src.photo.manifest_sha256, (f"cut_photo:{src.photo.id}",)),
        Pin("homology-table", src.table.manifest_sha256, (f"homology_table:{src.table.id}",)),
    ]
    if src.scores is not None:
        pins.append(
            Pin(
                "homology-self-scores",
                src.scores.manifest_sha256,
                (f"homology_self_scores:{src.scores.id}",),
            )
        )
    return MethodSpec(
        family=Family.EXPLAINABLE_UNITS,
        code=method_code(p),
        instance=InstanceCut(src.photo.cut_release, src.photo.ontology_snapshot_id),
        recipe=recipe,
        pins=tuple(pins),
        runtime="native",
        explains=False,
    )


def receipt(p: RunHomologyTransferPayload) -> dict[str, Any]:
    """The donors and the exclusion as a retrieval's receipt spells them (identity)."""
    return {
        "donor_policy": p.donor_policy.model_dump(),
        "exclude_self_neighbour": True,
        "self_exclusion": p.self_exclusion,
    }


def _store() -> Any:
    return get_artifact_store(load_settings(_PROJECT_ROOT))


def _scratch_root() -> Path:
    root = load_settings(_PROJECT_ROOT).artifacts_dir / "homology_transfer"
    root.mkdir(parents=True, exist_ok=True)
    return root


@dataclass(frozen=True)
class Gathered:
    """What the cut says about the targets: sequences, hits, donors, states and self scores."""

    targets: list[str]
    md5_of: dict[str, str]
    hits: dict[str, list[Hit]]
    donors: dict[str, list[str]]
    states: DonorRows
    self_bits: dict[str, float]
    lengths: dict[str, int]


def gather(
    session: Session, src: Sources, p: RunHomologyTransferPayload, targets: list[str]
) -> Gathered:
    """Every input of the scoring, read at t0."""
    md5_of = target_md5s(session, src.photo, targets)
    with tempfile.TemporaryDirectory(dir=_scratch_root()) as scratch:
        path = fetch_verified_to_file(
            _store(),
            src.table.hits_uri,
            src.table.hits_sha256,
            Path(scratch) / "hits.parquet",
            f"the homology hits of cut {src.table.cut_release}",
        )
        hits = transfer.read_hits(path, set(md5_of.values()), p.hit_cap)
    wanted = set(md5_of.values()) | {h.md5 for found in hits.values() for h in found}
    donors = donors_by_md5(session, src.photo, wanted, p.donor_policy.donor_set_id)
    accessions = {a for found in donors.values() for a in found}
    states = donor_states(session, src, accessions, p.donor_policy)
    scores = self_bits(_store(), src.scores, set(md5_of.values())) if src.scores else {}
    lengths = sequence_lengths(session, wanted) if p.identity == "global" else {}
    return Gathered(targets, md5_of, hits, donors, states, scores, lengths)


def target_hits(data: Gathered, md5: str, rule: TransferRule) -> list[Hit]:
    """A target sequence's hits of the table and its twin hit, with global identity when read."""
    found = data.hits.get(md5, [])
    if rule.identity == "global":
        qlen = data.lengths[md5]
        found = [
            h._replace(
                global_identity=transfer.global_identity(
                    h.fident, h.alnlen, qlen, data.lengths[h.md5]
                )
            )
            for h in found
        ]
    return [*found, transfer.twin_hit(md5, data.self_bits.get(md5), data.lengths.get(md5))]


def donors_for(data: Gathered, accession: str, self_exclusion: str) -> Mapping[str, list[str]]:
    """The donors each hit sequence may name for this target: its own accession out, and its
    twins with it under ``sequence`` exclusion."""
    md5 = data.md5_of[accession]
    twins = [a for a in data.donors.get(md5, []) if a != accession]
    return ChainMap({md5: twins if self_exclusion == "accession" else []}, data.donors)


def pivot_ids(session: Session, eset: EvaluationSet, go_ids: list[str]) -> dict[str, int]:
    """Every donor term's ``go_term`` id on the window's pivot, refusing a term it lacks."""
    ids = term_ids(session, eset.pivot_snapshot_id)
    missing = sorted(t for t in go_ids if t not in ids)
    if missing:
        raise HomologyTransferRefused(
            f"{len(missing)} donor terms of t0's ontology are not terms of the window's pivot "
            f"(first {missing[:10]}); the set is written on the pivot"
        )
    return ids


def score_all(
    data: Gathered, rule: TransferRule, self_exclusion: str
) -> Iterator[tuple[str, transfer.Voters, list[transfer.ScoredTerm]]]:
    """Each target with a sequence: its voters and its scored terms."""
    for accession in data.targets:
        if accession not in data.md5_of:
            continue
        hits = target_hits(data, data.md5_of[accession], rule)
        donors = donors_for(data, accession, self_exclusion)
        voters = transfer.voters_of(hits, donors, data.states.terms, rule)
        yield accession, voters, transfer.score_target(voters.voters, data.states.aspects, rule)


class RunHomologyTransferOperation:
    """Score a window's targets from the donors their sequences hit (rungs 2a and 2b)."""

    name = "run_homology_transfer"
    description = (
        "Score the targets of a window of LAFA's frame from the donors their sequences hit in the "
        "cut's homology table, and their twins: the top hit's identity or a bit score vote, with "
        "the donors a policy admits, as a native method's compact prediction set with its "
        "evidence (explainable-units family)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        window = str(payload.get("evaluation_set_id", ""))[:8]
        return f"homology transfer {payload.get('rule', '')} for window {window}"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = RunHomologyTransferPayload.model_validate(contract_payload(payload))
        src = resolve(session, p)
        method = ensure_method(session, method_spec(src, p))
        done = self._already_written(session, method.id, src.eset, p)
        if done is not None:
            emit(
                "run_homology_transfer.already_read",
                None,
                {"prediction_set_id": str(done.id)},
                "info",
            )
            return OperationResult(result={"prediction_set_id": str(done.id), "skipped": True})
        targets, declared = window_targets(session, src.eset)
        data = gather(session, src, p, targets)
        emit(
            "run_homology_transfer.inputs",
            None,
            {
                "targets": len(targets),
                "targets_without_a_sequence": len(targets) - len(data.md5_of),
                "hits": sum(len(h) for h in data.hits.values()),
                "donor_accessions": sum(len(d) for d in data.donors.values()),
                "donors_with_a_state": len(data.states.terms),
            },
            "info",
        )
        return self._write(session, (src, method, data, declared), p, payload, emit)

    @staticmethod
    def _already_written(
        session: Session, method_id: uuid.UUID, eset: EvaluationSet, p: RunHomologyTransferPayload
    ) -> PredictionSet | None:
        """The set this method already wrote on this window with these donors, if any."""
        written = session.execute(
            select(PredictionSet).where(
                PredictionSet.method_id == method_id,
                PredictionSet.meta["evaluation_set_id"].astext == str(eset.id),
            )
        ).scalars()
        mine = receipt(p)
        for pset in written:
            if all(pset.meta.get(key) == value for key, value in mine.items()):
                return pset
        return None

    @staticmethod
    def _write(
        session: Session,
        run: tuple[Any, ...],
        p: RunHomologyTransferPayload,
        payload: dict[str, Any],
        emit: EmitFn,
    ) -> OperationResult:
        src, method, data, declared = run
        ids = pivot_ids(session, src.eset, data.states.go_ids)
        scored, counts = rows_of(score_all(data, p.rule_of(), p.self_exclusion), data)
        evidence = store_evidence(
            _store(),
            evidence_table(scored, data),
            method_name=method.short_name,
            evaluation_set_id=src.eset.id,
        )
        covered = len({row.accession for row in scored})
        per_target = Counter(row.accession for row in scored)
        pset = new_prediction_set(
            session,
            method,
            SetContext(
                annotation_set_id=src.photo.annotation_set_id,
                ontology_snapshot_id=src.eset.pivot_snapshot_id,
                limit_per_entry=max(per_target.values(), default=1),
                description=(
                    f"Homology transfer ({method.short_name}) at cut {src.photo.cut_release} on the "
                    f"{len(data.targets):,} targets of window {src.eset.id}: {covered:,} of them "
                    "carry a term."
                ),
                meta={
                    "evaluation_set_id": str(src.eset.id),
                    **receipt(p),
                    "targets_with_a_term": covered,
                    "targets_without_a_sequence": len(data.targets) - len(data.md5_of),
                    "evidence_artifact": evidence,
                    "written_counts": counts,
                },
                job_id=job_id_from_payload(payload),
                **population_spelling(data.targets, declared),
            ),
        )
        go_ids = data.states.go_ids
        written = copy_scores(
            session, pset, ((r.accession, ids[go_ids[r.term]], r.score) for r in scored)
        )
        result = {
            "prediction_set_id": str(pset.id),
            "method_id": str(method.id),
            "method": method.short_name,
            "rows": written,
            "targets_with_a_term": covered,
            "evidence_artifact": evidence,
            "written_counts": counts,
        }
        emit("run_homology_transfer.done", f"{written} rows", result, "info")
        return OperationResult(result=result)


__all__ = [
    "HomologyTransferRefused",
    "RunHomologyTransferOperation",
    "RunHomologyTransferPayload",
]
