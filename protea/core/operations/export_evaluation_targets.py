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
``--query_file``; the same bytes register as a ``QuerySet`` through the existing
FASTA endpoint, and the grid dispatches against that. Both sides can quote the
same sha256, which proves the populations are identical rather than intended to
be.

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

It writes no rows. The only side effect is the artifact.
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path
from typing import Annotated, Any

from pydantic import Field, field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import (
    EmitFn,
    Operation,
    OperationResult,
    ProteaPayload,
)
from protea.core.evaluation import load_evaluation_data_for_set
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.annotation.evaluation_set import EvaluationSet
from protea.infrastructure.orm.models.protein.protein import Protein
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


def _report_missing(
    accessions: list[str], sequences: dict[str, str], emit: EmitFn
) -> list[str]:
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
            f"{len(missing)} delta proteins have no stored sequence and are absent "
            "from the file",
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
        "excluded. Writes no rows."
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
        return self._publish(p, accessions, sequences, emit)

    def _publish(
        self,
        p: ExportEvaluationTargetsPayload,
        accessions: list[str],
        sequences: dict[str, str],
        emit: EmitFn,
    ) -> OperationResult:
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
        return OperationResult(
            result={
                "uri": uri,
                "key": key,
                "sha256": digest,
                "targets": len(written),
                "missing_sequence": len(missing),
                "canonicalised": len(altered),
                "categories": p.categories,
                # The sha256 is the point, so it is said rather than left to be
                # noticed: it is what lets the grid and the submission prove they
                # scored one population instead of asserting it.
                "caveat": (
                    "quote this sha256 on both sides. A QuerySet registered from "
                    "these bytes and a LAFA --query_file of these bytes are the "
                    "same population; anything else is two populations that happen "
                    "to agree"
                ),
            }
        )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        return f"write the target FASTA for evaluation set {payload.get('evaluation_set_id')}"
