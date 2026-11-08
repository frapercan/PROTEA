# protea/core/operations/export_evaluation_targets.py
"""Write the evaluation delta as the FASTA that both the grid and LAFA consume.

The submission path and the internal path disagree today on the one input that
decides what is being measured. ``apps/method_runtime/protea_predict.py`` requires
``--query_file``, a FASTA naming the targets; ``PredictGOTermsPayload`` leaves
both ``query_set_id`` and ``query_accessions`` optional, and giving neither
selects every protein that has an embedding. So a dispatch that forgets to name
its population does not fail, it silently scores the whole corpus.

That is a comparability problem before it is a cost problem. If the grid scores
one population and the submission scores another, the two sets of numbers are not
about the same thing, and nothing in either artifact records the difference. The
contribution being claimed is an environment where the local measurement and the
submitted one are the same measurement.

So this exports one artifact with two consumers. The file it writes is the LAFA
``--query_file``, and the same bytes are declared here as a ``QuerySet`` that the
grid dispatches against. Both sides quote the same sha256, which proves the
populations are identical rather than intended to be.

THE QUERY SET USED TO BE REGISTERED BY HAND, and that is what changed. The
operation wrote the file and a person then uploaded those bytes through the
FASTA endpoint, typing the provenance into the free-text description: the
window as an id prefix, the job as a word, the sha as a string nothing compares
against. The derivation of the population every number is measured over lived
in prose, which is the one place a gate cannot read.

And the hand route had a trap that had not sprung. The file carries
``canonical_residues``, so re-hashing those bytes at the endpoint hashes the
CANONICALISED sequence; where that differs from the stored one it inserts a
second ``sequence`` row and the entries point at sequences with no embedding.
Measured on 2026-11-08: 0 of 138.297 entries, because the canonicalisation was
a no-op for this population. Declaring here reuses the stored ``sequence_id``,
so the trap is disarmed rather than left waiting.

The population is the delta: proteins that GAINED annotation between the two cuts,
taken from the no-knowledge, limited-knowledge and prior-knowledge maps. Proteins
that only LOST annotation live in ``removed``, which the evaluation reports and
never scores, and they are excluded here by construction. That is the same rule as
building ground truth from additions rather than a net difference, which matters
because the corpus is not monotone and contracts by roughly a third twice.

Header format is a bare accession. LAFA's parser takes the token before the first
whitespace and, if it contains at least two pipes, the field between the first
two; a bare accession round-trips through that unchanged, so no convention has to
be agreed twice.

It writes the artifact and ONE declared ``QuerySet`` with its entries. Writing
the same delta twice finds that row by ``(window, sha256)`` and does not make a
second: one window plus one set of bytes is one population.
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path
from typing import Annotated, Any, NamedTuple

from pydantic import Field, field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.code_revision import code_revision
from protea.core.contracts.operation import (
    EmitFn,
    Operation,
    OperationResult,
    ProteaPayload,
)
from protea.core.evaluation import load_evaluation_data_for_set
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from protea.infrastructure.storage import get_artifact_store

#: The three delta categories the evaluation scores. ``removed`` is deliberately
#: not among them: those proteins lost annotation over the window, the evaluation
#: reports them and never scores them, and including them would put targets in the
#: file that no metric can reward.
SCORED_CATEGORIES = ("nk", "lk", "pk")

#: Residues per line. Sixty is the convention every FASTA reader tolerates, and a
#: fixed width keeps the bytes reproducible rather than dependent on a writer.
WRAP = 60


def canonical_residues(sequence: str) -> str:
    """The sequence as LAFA will actually read it: upper case, no gap characters.

    LAFA's parser does ``line.upper().replace("-", "")`` on every residue line, so
    a stored sequence carrying lower-case masking or an alignment gap reaches the
    model in a different form from the one we published. The sha256 would still
    describe our file faithfully and would still be wrong about what was embedded,
    which is the worst shape a checksum can have: authoritative over the wrong
    object.

    Normalising here rather than at the reader makes the published bytes the same
    object the model sees. The count of altered sequences is reported by the
    caller, so the transformation is visible rather than silent.
    """
    return sequence.upper().replace("-", "")


PositiveInt = Annotated[int, Field(gt=0)]


class ExportEvaluationTargetsPayload(ProteaPayload, frozen=True):
    """Which delta to write, and under what name."""

    evaluation_set_id: str
    categories: list[str] = list(SCORED_CATEGORIES)
    output_name: str = "targets"

    @field_validator("categories")
    @classmethod
    def _only_scored_categories(cls, value: list[str]) -> list[str]:
        """Refuse anything outside the scored three, ``removed`` included.

        Naming ``removed`` here would be a plausible request that silently
        changes the population being measured, which is exactly the class of
        error this operation exists to close.
        """
        if not value:
            raise ValueError("categories cannot be empty; omit it to take all three")
        unknown = sorted(set(value) - set(SCORED_CATEGORIES))
        if unknown:
            raise ValueError(
                f"unknown categories {unknown}; only {list(SCORED_CATEGORIES)} are "
                "scored. Proteins that only lost annotation are reported and never "
                "scored, so they are not targets"
            )
        return value


def format_fasta(records: list[tuple[str, str]]) -> bytes:
    """One record per accession, wrapped, in the order given.

    Ordering is the caller's responsibility and is load-bearing: LAFA's
    ``fasta_accessions`` treats file order as output order, so a stable order here
    makes the predicted TSV stable too.
    """
    lines: list[str] = []
    for accession, sequence in records:
        residues = canonical_residues(sequence)
        lines.append(f">{accession}")
        lines.extend(residues[i : i + WRAP] for i in range(0, len(residues), WRAP))
    return ("\n".join(lines) + "\n").encode() if lines else b""


def delta_accessions(data: Any, categories: list[str]) -> list[str]:
    """Sorted union of the selected categories' proteins.

    Sorted rather than in map order, because a dict's order reflects how the
    delta happened to be built and would make the same evaluation set produce
    different bytes on a rebuild.
    """
    accessions: set[str] = set()
    for category in categories:
        accessions.update(getattr(data, category, {}) or {})
    return sorted(accessions)


def _load_sequences(session: Session, accessions: list[str]) -> dict[str, str]:
    """Accession to residue string, for those that have one."""
    rows = session.execute(
        select(Protein.accession, Sequence.sequence)
        .join(Sequence, Sequence.id == Protein.sequence_id)
        .where(Protein.accession.in_(accessions))
    ).all()
    return {accession: sequence for accession, sequence in rows}


def _sequence_ids(session: Session, accessions: list[str]) -> dict[str, int]:
    """Accession to the id of the sequence row the PROTEIN already points at.

    Taken from the row and never re-hashed from the published bytes. The bytes
    are canonicalised and the stored sequence is not, so hashing them again can
    name a different sequence, which is how the hand-registration route could
    have pointed a query set at sequences with no embedding.
    """
    rows = session.execute(
        select(Protein.accession, Protein.sequence_id).where(Protein.accession.in_(accessions))
    ).all()
    return {accession: sequence_id for accession, sequence_id in rows}


class _Published(NamedTuple):
    """What the artifact write produced, so declaring it re-derives nothing."""

    uri: str
    key: str
    digest: str
    records: list[tuple[str, str]]
    missing: int
    altered: int


class _Declaration(NamedTuple):
    """What the row has to say about itself, built where it is all known."""

    window: uuid.UUID
    name: str
    description: str
    job_id: uuid.UUID | None


def _sequence_ids_or_refuse(session: Session, records: list[tuple[str, str]]) -> dict[str, int]:
    """The sequence id of every published accession, or a refusal naming the gaps.

    IT REFUSES INSTEAD OF SKIPPING. The first version wrote the entries it
    could and dropped the rest, while the description it wrote said
    ``len(records)`` targets: a declared set that does not match the bytes its
    own sha names, which is the defect this whole operation is closing,
    reintroduced one level down. The desktop session caught it.

    Every record reaching here came from a join on ``Protein.sequence_id``, so
    a gap means the population moved between the two queries. The file is
    already in the store with its digest by this point, and that is fine: the
    artifact is true, and only the declaration is refused.
    """
    ids = _sequence_ids(session, [a for a, _ in records])
    sin_fila = sorted(a for a, _ in records if a not in ids)
    if sin_fila:
        raise ValueError(
            f"{len(sin_fila)} of {len(records)} published accessions have no protein "
            f"row to take a sequence_id from: {sin_fila[:5]}. The file is already in "
            "the store with its sha, but declaring a query set with fewer entries "
            "than the bytes it names would make the set disagree with its own digest"
        )
    return ids


def _declaration_for(
    eval_set: EvaluationSet,
    p: ExportEvaluationTargetsPayload,
    pub: _Published,
    job_id: uuid.UUID | None,
) -> _Declaration:
    """What the row says about itself, built from what was actually written.

    DECLARED AFTER EXTRACTION, never before. The name carries the sha of the
    bytes that exist, so it cannot be a scope asserted in a payload and
    contradicted by the file, and the description names the window's own rules
    rather than repeating a label.
    """
    return _Declaration(
        window=eval_set.id,
        name=f"{p.output_name}-{str(eval_set.id)[:8]}-{pub.digest[:12]}",
        description=(
            f"The {len(pub.records)} delta targets of evaluation set {eval_set.id} "
            f"({eval_set.truth_profile}, {eval_set.toi_rule}, not_codes="
            f"{eval_set.not_codes}) over categories {p.categories}, written by "
            f"export_evaluation_targets with sha256 {pub.digest}. "
            f"{pub.missing} delta proteins had no stored sequence and "
            f"{pub.altered} sequences were canonicalised."
        ),
        job_id=job_id,
    )


def _declare(session: Session, dec: _Declaration, pub: _Published, emit: EmitFn) -> uuid.UUID:
    """Register the exported population as a declared ``QuerySet``.

    Idempotent on ``(window, sha256)``, which is the identity of a population:
    the same delta exported twice is the same set, and a second row would make
    two names for one thing, which is the defect this table already had.
    """
    ya = session.execute(
        select(QuerySet).where(
            QuerySet.evaluation_set_id == dec.window,
            QuerySet.content_sha256 == pub.digest,
        )
    ).scalar_one_or_none()
    if ya is not None:
        emit(
            "targets.query_set_reused",
            f"this population is already declared as {ya.id}",
            {"query_set_id": str(ya.id), "name": ya.name, "sha256": pub.digest},
            "info",
        )
        return ya.id

    ids = _sequence_ids_or_refuse(session, pub.records)
    qs = QuerySet(
        name=dec.name,
        description=dec.description,
        source_kind="evaluation_targets",
        evaluation_set_id=dec.window,
        artifact_uri=pub.uri,
        content_sha256=pub.digest,
        job_id=dec.job_id,
        code_revision=code_revision(),
    )
    session.add(qs)
    session.flush()
    # Taxonomy and species are left unset: the headers are bare accessions by
    # design, so there is nothing to parse them from, and inventing them from
    # the protein row would make the entry say something the file does not.
    session.add_all(
        QuerySetEntry(query_set_id=qs.id, sequence_id=ids[a], accession=a) for a, _ in pub.records
    )
    session.flush()
    emit(
        "targets.query_set_declared",
        f"declared as query set {qs.id} with {len(pub.records)} entries",
        {
            "query_set_id": str(qs.id),
            "name": qs.name,
            "evaluation_set_id": str(dec.window),
            "sha256": pub.digest,
            "entries": len(pub.records),
        },
        "info",
    )
    return qs.id


def _report_missing(accessions: list[str], sequences: dict[str, str], emit: EmitFn) -> list[str]:
    """Say which targets have no stored sequence, rather than letting them vanish.

    A target set that quietly shrank is a population nobody can reconstruct
    afterwards, and the sha256 over the smaller file would still look
    authoritative. Examples travel with the count so the gap can be chased
    without a second query.
    """
    missing = [a for a in accessions if a not in sequences]
    if missing:
        emit(
            "targets.missing_sequence",
            f"{len(missing)} delta proteins have no stored sequence and are absent from the file",
            {"missing": len(missing), "examples": missing[:10]},
            "warning",
        )
    return missing


def _report_altered(records: list[tuple[str, str]], emit: EmitFn) -> list[str]:
    """Say which sequences the canonical form changed, rather than changing them quietly.

    Lower case in a protein FASTA often marks a masked or low-complexity region,
    and a dash is an alignment gap. LAFA discards both, so the information is lost
    downstream whatever we do; what must not be lost is the fact that it happened.
    """
    altered = [a for a, seq in records if canonical_residues(seq) != seq]
    if altered:
        emit(
            "targets.canonicalised",
            f"{len(altered)} sequences were upper-cased or had gap characters removed "
            "to match the form LAFA reads",
            {"canonicalised": len(altered), "examples": altered[:10]},
            "warning",
        )
    return altered


class ExportEvaluationTargetsOperation(Operation):
    name = "export_evaluation_targets"
    description = (
        "Write an EvaluationSet's delta proteins as a FASTA and publish it to the "
        "artifact store, so the same file is both the LAFA --query_file and the "
        "QuerySet the internal grid dispatches against. Targets are the proteins "
        "that gained annotation over the window; proteins that only lost "
        "annotation are reported by the evaluation and never scored, so they are "
        "excluded. Declares the exported population as a QuerySet with its "
        "window, its sha256 and the job that built it, idempotent on (window, "
        "sha256), instead of leaving it to be registered by hand."
    )

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ExportEvaluationTargetsPayload.model_validate(contract_payload(payload))
        eval_set = session.get(EvaluationSet, uuid.UUID(p.evaluation_set_id))
        if eval_set is None:
            raise ValueError(f"EvaluationSet {p.evaluation_set_id} not found")

        data, _snapshot_id = load_evaluation_data_for_set(session, eval_set)
        accessions = delta_accessions(data, p.categories)
        emit(
            "targets.delta",
            f"{len(accessions)} delta proteins across {', '.join(p.categories)}",
            {
                "targets": len(accessions),
                "categories": p.categories,
                **{c: len(getattr(data, c, {}) or {}) for c in SCORED_CATEGORIES},
                "removed_excluded": len(getattr(data, "removed", {}) or {}),
            },
            "info",
        )
        if not accessions:
            raise ValueError(
                f"EvaluationSet {p.evaluation_set_id} has no delta proteins in "
                f"{p.categories}; a target file with no targets would score nothing"
            )

        sequences = _load_sequences(session, accessions)
        pub = self._publish(p, accessions, sequences, emit)
        dec = _declaration_for(eval_set, p, pub, job_id_from_payload(payload))
        query_set_id = _declare(session, dec, pub, emit)
        return OperationResult(
            result={
                "uri": pub.uri,
                "key": pub.key,
                "sha256": pub.digest,
                "targets": len(pub.records),
                "missing_sequence": pub.missing,
                "canonicalised": pub.altered,
                "categories": p.categories,
                "query_set_id": str(query_set_id),
                # The sha256 is the point, so it is said rather than left to be
                # noticed: it is what lets the grid and the submission prove
                # they scored one population instead of asserting it. It no
                # longer has to be carried across by hand, which is what the
                # caveat used to ask for.
                "caveat": (
                    "the QuerySet above and a LAFA --query_file of these bytes are "
                    "the same population, and the sha256 is what proves it rather "
                    "than two sides agreeing to intend it"
                ),
            }
        )

    def _publish(
        self,
        p: ExportEvaluationTargetsPayload,
        accessions: list[str],
        sequences: dict[str, str],
        emit: EmitFn,
    ) -> _Published:
        """Write the file, publish it, and say what did not make it in.

        The missing list is emitted rather than inferred because a target set that
        quietly shrank is a population nobody can reconstruct afterwards, and the
        sha256 would still look authoritative.
        """
        missing = _report_missing(accessions, sequences, emit)
        written = [(a, sequences[a]) for a in accessions if a in sequences]
        altered = _report_altered(written, emit)
        raw = format_fasta(written)
        digest = hashlib.sha256(raw).hexdigest()

        from protea.infrastructure.settings import load_settings

        # protea/core/operations/<this>.py -> parents[3] is the repo root, the
        # same walk generate_evaluation_set does for the ground truth beside this.
        project_root = Path(__file__).resolve().parents[3]
        store = get_artifact_store(load_settings(project_root))
        key = f"eval_targets/{p.evaluation_set_id}/{p.output_name}-{digest[:12]}.fasta"
        uri = store.put(key, raw)

        emit(
            "targets.published",
            f"{len(written)} targets, {len(raw) / 1e6:.2f} MB, sha256 {digest[:12]}",
            {"uri": uri, "targets": len(written), "sha256": digest},
            "info",
        )
        return _Published(
            uri=uri,
            key=key,
            digest=digest,
            records=written,
            missing=len(missing),
            altered=len(altered),
        )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return f"write the target FASTA for evaluation set {payload.get('evaluation_set_id')}"
