"""Declare one of ADR-D57's exam populations, from a manifest the server verifies.

WHAT IT IS FOR. The vocabulary exams move inside the platform, and decision 2 of
ADR-D57 says each exam population enters as a declared set: declared, described
and verifiable. There are two, frozen once by the laboratory over Swiss-Prot
2022_03, and they are not the corpus and not a window's delta.

THE PAYLOAD NAMES A MANIFEST AND NOTHING ELSE, which is the whole shape of this
operation. An exam population is several files: the pairs of (accession, md5 of
the exam's sequence) with each protein's half, the split, the concepts, the
instances or the sites, and the laboratory's own manifest carrying the scripts
that built them with their digests, the Swiss-Prot release behind them and the
checks it ran. A ``query_set`` row has one ``artifact_uri`` and one
``content_sha256``, so the row points at a manifest that names every file with
its own digest.

NO FILESYSTEM PATH REACHES THIS OPERATION. The files are in the store before it
runs, under keys derived from their own digests, the way the term text vectors
arrived. A path in a payload would let the API read any file on the server, and
a scope that lives in a payload is a hypothesis dressed as a constant.

AND A DIGEST IS NOT ENOUGH, which is why the row counts are checked too. A
digest proves nobody altered the bytes between there and here. It never proves
they are the bytes that were meant: on 2026-11-08 a vector file arrived whose
command-line label named one ontology snapshot while its contents were another
one's, and every hash matched. So this operation reads each file and checks what
is INSIDE it against what the manifest claims: the parquet row counts, the tsv's
header, its order, its partition, and that every accession is a protein this
platform holds a sequence for.

WHAT IT REPORTS AND DOES NOT STORE. How many of the declared md5 agree with the
sequence this platform holds today, and the accessions where they differ. That
is a MEASURE of (population, platform) at a date and not a property of the set:
the exam derives its own exclusion from the same comparison at the moment it
runs, as the record says, and a count frozen in a row would be stale the next
time a sequence moves.
"""

from __future__ import annotations

import io
import json
import uuid
from pathlib import Path
from typing import Any, NamedTuple

import pyarrow.parquet as pq
from pydantic import field_validator
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from protea.core._artifact_identity import fetch_and_verify
from protea.core.code_revision import code_revision
from protea.core.contracts.operation import (
    EmitFn,
    Operation,
    OperationResult,
    ProteaPayload,
)
from protea.core.operations._exam_population_checks import (
    counted_roles,
    labels_must_fit_where_the_sequence_agrees,
    labels_outside_this_platform,
    labels_stay_inside_the_population,
    positions_count_from_one,
    split_agrees_with_the_population,
    the_sequences_are_the_ones_the_md5_names,
)
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.protein.protein import Protein
from protea.infrastructure.orm.models.query.query_set import QuerySet, QuerySetEntry
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

#: The rule, in one sentence, stored on the row so a reader does not have to
#: open the manifest to know what the set is.
EXAM_RULE = (
    "every protein of one frozen vocabulary exam, paired with the md5 of its "
    "Swiss-Prot 2022_03 sequence and with the half it belongs to"
)

#: The three columns of ``poblacion.tsv``, in order. Checked and not assumed:
#: a file with the same columns in another order would parse into md5 values
#: read as halves and refuse nothing.
COLUMNS = ("accession", "md5", "half")

HALVES = ("selection", "confirmation")


class _Manifest(NamedTuple):
    """The verified manifest, and the file it says holds the population."""

    exam: str
    raw: bytes
    digest: str
    uri: str
    data: dict[str, Any]

    @property
    def files(self) -> dict[str, dict[str, Any]]:
        return dict(self.data["files"])


class _Population(NamedTuple):
    """One row per protein of the exam, as the tsv states them."""

    accessions: list[str]
    md5_of: dict[str, str]
    half_of: dict[str, str]


class _Resolved(NamedTuple):
    """What this platform holds for those accessions."""

    sequence_of: dict[str, int]
    hash_of: dict[str, str]
    #: The length of the sequence stored HERE, which is what a label can fall
    #: outside of. Not the length the exam's own sequence has.
    length_of: dict[str, int]
    missing: list[str]


class DeclareExamPopulationPayload(ProteaPayload, frozen=True):
    """A manifest key and its digest. The exam's name comes from the manifest."""

    manifest_key: str
    manifest_sha256: str

    @field_validator("manifest_key")
    @classmethod
    def _a_store_key_and_not_a_path(cls, value: str) -> str:
        """Refuse an empty key and one that climbs out of the store.

        The key is used verbatim against the artifact store. A ``..`` in it is
        either a mistake or an attempt, and the two are indistinguishable from
        here, so both are refused.
        """
        limpio = value.strip()
        if not limpio:
            raise ValueError("manifest_key cannot be empty")
        if ".." in limpio:
            raise ValueError("manifest_key cannot contain '..': it is a store key, not a path")
        return limpio

    @field_validator("manifest_sha256")
    @classmethod
    def _a_full_digest(cls, value: str) -> str:
        """Refuse a prefix.

        Eight characters of a sha agreed with the truth twice in one hour on
        2026-11-08 while the thing behind them did not, so a short form is not
        accepted where a digest is meant.
        """
        limpio = value.strip().lower()
        if len(limpio) != 64 or any(c not in "0123456789abcdef" for c in limpio):
            raise ValueError(
                f"manifest_sha256 must be 64 hexadecimal characters, got {len(limpio)}: "
                "a prefix is a label and this field is an identity"
            )
        return limpio


def _manifest(p: DeclareExamPopulationPayload, store: Any) -> _Manifest:
    """The manifest, verified against the digest the caller declared."""
    raw = fetch_and_verify(store, p.manifest_key, p.manifest_sha256, f"the manifest at {p.manifest_key}")
    data = json.loads(raw.decode("utf-8"))
    # ``position_conventions`` and ``halves`` are required, which is what makes
    # the two manifests written before this check undeclarable. One of them said
    # positions were indices from 0 and that was false of every label file, so
    # the fix is not to delete those two objects but to refuse any manifest that
    # does not state the convention per file: the next one written by hand
    # cannot repeat it either.
    for campo in (
        "exam",
        "files",
        "partition",
        "concepts",
        "population_file",
        "position_conventions",
        "halves",
    ):
        if campo not in data:
            raise ValueError(f"the manifest at {p.manifest_key} has no '{campo}'")
    if data["population_file"] not in data["files"]:
        raise ValueError(
            f"the manifest names '{data['population_file']}' as its population file and "
            "then does not list it among its files"
        )
    return _Manifest(
        exam=str(data["exam"]),
        raw=raw,
        digest=p.manifest_sha256,
        uri=store.url(p.manifest_key),
        data=data,
    )


def _rows_of(name: str, blob: bytes) -> int | None:
    """The row count a file actually has, or None when the format has none."""
    if name.endswith(".parquet"):
        return int(pq.read_metadata(io.BytesIO(blob)).num_rows)
    if name.endswith(".tsv"):
        return blob.decode("utf-8").count("\n") - 1
    return None


def _verify_every_file(m: _Manifest, store: Any) -> dict[str, bytes]:
    """Fetch and check each file: its digest, and then what is inside it."""
    leidos: dict[str, bytes] = {}
    for name, says in m.files.items():
        blob = fetch_and_verify(store, says["key"], says["sha256"], f"{m.exam}/{name}")
        if len(blob) != int(says["bytes"]):
            raise ValueError(
                f"{m.exam}/{name} is {len(blob)} bytes and the manifest says "
                f"{says['bytes']}, with a matching digest, which cannot both be true"
            )
        claimed = says.get("rows")
        actual = _rows_of(name, blob)
        if claimed is not None and actual is not None and int(claimed) != actual:
            raise ValueError(
                f"{m.exam}/{name} holds {actual} rows and the manifest claims "
                f"{claimed}. The digest matched, so the bytes are intact and the "
                "manifest describes a different file"
            )
        leidos[name] = blob
    return leidos


def _parse_population(blob: bytes, exam: str) -> _Population:
    """The pairs, with the order and the vocabulary checked as they are read."""
    lineas = blob.decode("utf-8").splitlines()
    if not lineas:
        raise ValueError(f"the population file of {exam} is empty")
    header = tuple(lineas[0].split("\t"))
    if header != COLUMNS:
        raise ValueError(f"the population file of {exam} has columns {header}, expected {COLUMNS}")
    accessions, md5_of, half_of = [], {}, {}
    for n, linea in enumerate(lineas[1:], start=2):
        partes = linea.split("\t")
        if len(partes) != 3:
            raise ValueError(f"{exam} line {n} has {len(partes)} fields, expected 3")
        acc, md5, half = partes
        if half not in HALVES:
            raise ValueError(f"{exam} line {n}: half {half!r} is not one of {HALVES}")
        if acc in md5_of:
            raise ValueError(f"{exam} names {acc} twice, so it is not a population")
        accessions.append(acc)
        md5_of[acc] = md5
        half_of[acc] = half
    if accessions != sorted(accessions):
        raise ValueError(
            f"the population file of {exam} is not sorted by accession, and its digest "
            "is of a specific order"
        )
    return _Population(accessions=accessions, md5_of=md5_of, half_of=half_of)


def _resolve(session: Session, accessions: list[str]) -> _Resolved:
    """This platform's sequence for each accession, and the ones it lacks."""
    rows = session.execute(
        select(
            Protein.accession,
            Protein.sequence_id,
            Sequence.sequence_hash,
            func.length(Sequence.sequence),
        )
        .join(Sequence, Sequence.id == Protein.sequence_id)
        .where(Protein.accession.in_(accessions))
    ).all()
    sequence_of = {r[0]: int(r[1]) for r in rows}
    return _Resolved(
        sequence_of=sequence_of,
        hash_of={r[0]: r[2] for r in rows},
        length_of={r[0]: int(r[3]) for r in rows},
        missing=[a for a in accessions if a not in sequence_of],
    )


def _refuse_a_second_row(session: Session, digest: str) -> None:
    """Refuse a duplicate, because the unique constraint cannot catch this one.

    ``uq_query_set_window_content`` is on ``(evaluation_set_id,
    content_sha256)`` and this kind has ``evaluation_set_id`` NULL by its own
    CHECK, so in Postgres two NULLs do not collide and that constraint would
    admit a second row for the same manifest. The same hole the corpus kind has,
    closed the same way and said out loud rather than assumed to be elsewhere.
    """
    existing = session.execute(
        select(QuerySet.id).where(
            QuerySet.source_kind == "exam_population",
            QuerySet.content_sha256 == digest,
        )
    ).first()
    if existing is not None:
        raise ValueError(
            f"this exam population is already declared as QuerySet {existing[0]} with the "
            f"same manifest sha256 {digest}. The unique constraint cannot refuse it "
            "because its evaluation_set_id is NULL and two NULLs do not collide"
        )


def _described(m: _Manifest, pop: _Population) -> str:
    """The description, carrying what a reader should not have to download.

    The partition and the concepts by role ARE the identity of an exam
    population: a set of 3.335 proteins with 1.662 in selection is a different
    thing from the same proteins split another way, and the reserved concepts
    are the ones nobody has looked at. Putting them here means the platform's
    own surface shows them without fetching the manifest.
    """
    part = m.data["partition"]
    conc = m.data["concepts"]
    reparto = ", ".join(f"{k} {v}" for k, v in sorted(part.items()))
    conceptos = ", ".join(f"{k} {v}" for k, v in sorted(conc.items()))
    return (
        f"The frozen '{m.exam}' vocabulary exam population: {len(pop.accessions)} proteins "
        f"over Swiss-Prot 2022_03, partitioned as {reparto}, with concepts {conceptos}. "
        f"The rule is {EXAM_RULE}. It is NOT the corpus and it has NO evaluation window: "
        "these exams sit on a 2022 Swiss-Prot release and not on a transition cut. Its "
        "artifact is a manifest naming every file of the exam with its own digest, "
        f"including the laboratory's own manifest with the scripts that built it "
        f"(lab commit {m.data.get('lab_commit', 'unknown')})."
    )


def _declare(
    session: Session,
    m: _Manifest,
    pop: _Population,
    got: _Resolved,
    job_id: uuid.UUID | None,
) -> QuerySet:
    """The row and its entries, with the job on the row AT its insert.

    ``job_id`` is an argument and not assigned after the flush, which is the
    defect ``ck_query_set_corpus_is_declared`` caught on its first real run:
    the object looked right at the end of the function and the INSERT had
    already gone without it.
    """
    row = QuerySet(
        name=f"examen-{m.exam}-{m.digest[:12]}",
        source_kind="exam_population",
        description=_described(m, pop),
        artifact_uri=m.uri,
        content_sha256=m.digest,
        job_id=job_id,
        code_revision=code_revision(),
    )
    session.add(row)
    session.flush()
    session.add_all(
        [
            QuerySetEntry(query_set_id=row.id, accession=a, sequence_id=got.sequence_of[a])
            for a in pop.accessions
        ]
    )
    return row


def _agreement(pop: _Population, got: _Resolved) -> dict[str, Any]:
    """How many declared md5 match the sequence this platform holds today.

    Emitted, never stored. The exam derives its own exclusion from the same
    comparison when it runs, and this number is stale the moment a sequence
    moves.
    """
    distintas = sorted(a for a in pop.accessions if got.hash_of.get(a) != pop.md5_of[a])
    return {
        "compared": len(pop.accessions),
        "md5_agrees_with_this_platform": len(pop.accessions) - len(distintas),
        "md5_differs": len(distintas),
        "differing_accessions": distintas,
    }


#: The label files of each exam, and the columns whose upper bound is a residue
#: number. Named here rather than inferred from the file list, because a file
#: this map does not know is a file nobody decided how to check.
ETIQUETAS = {
    "sites.parquet": ("position",),
    "instances.parquet": ("start", "end"),
}


def _check_between_files(
    m: _Manifest, pop: _Population, leidos: dict[str, bytes]
) -> dict[str, Any]:
    """Make the files agree with each other, not just with their own digests.

    Each file can be perfectly intact and describe a different population, and
    nothing in a digest notices. So: the split's halves against the tsv's, the
    labels inside the membership, the sequences against the md5 they are paired
    with, the roles counted instead of copied, and the position convention
    verified rather than read.
    """
    medido: dict[str, Any] = {"positions": positions_count_from_one(m.exam, leidos)}
    if "split.parquet" in leidos:
        split_agrees_with_the_population(m.exam, pop.accessions, pop.half_of, leidos["split.parquet"])
        medido["split_agrees"] = True
    for nombre in ETIQUETAS:
        if nombre in leidos:
            medido[f"{nombre}_proteins"] = labels_stay_inside_the_population(
                m.exam, pop.accessions, leidos[nombre], nombre
            )
    if "proteins.parquet" in leidos:
        the_sequences_are_the_ones_the_md5_names(m.exam, pop.md5_of, leidos["proteins.parquet"])
        medido["sequences_match_their_md5"] = True
    contado = counted_roles(m.exam, leidos["concepts.json"])
    afirmado = {str(k): int(v) for k, v in m.data["concepts"].items()}
    if contado != afirmado:
        raise ValueError(
            f"{m.exam}: concepts.json counts {contado} by role and the manifest claims "
            f"{afirmado}. These numbers reach the row's description, which is the "
            "visible identity of this population, so a claim is not enough"
        )
    medido["concepts_counted"] = contado
    return medido


def _outside(leidos: dict[str, bytes], got: _Resolved) -> dict[str, Any]:
    """Labels that fall past the sequence this platform holds, per label file."""
    return {
        nombre: labels_outside_this_platform(leidos[nombre], columnas, got.length_of)
        for nombre, columnas in ETIQUETAS.items()
        if nombre in leidos
    }


def _must_fit(exam: str, leidos: dict[str, bytes], pop: _Population, got: _Resolved) -> int:
    """Require the upper bound where the md5 agrees, which is where it is knowable.

    Separate from ``_outside`` because they are different claims about the same
    column: one refuses and one reports, and which it is depends on whether the
    sequence stored here is the sequence the labels were made against.
    """
    de_acuerdo = {a for a in pop.accessions if got.hash_of.get(a) == pop.md5_of[a]}
    for nombre, columnas in ETIQUETAS.items():
        if nombre in leidos:
            labels_must_fit_where_the_sequence_agrees(
                exam, leidos[nombre], columnas, got.length_of, de_acuerdo
            )
    return len(de_acuerdo)


class DeclareExamPopulationOperation(Operation):
    name = "declare_exam_population"
    description = (
        "Declare one of ADR-D57's frozen vocabulary exam populations as a QuerySet, "
        "from a manifest in the artifact store whose digest the caller declares and "
        "the server verifies. Re-reads every file the manifest names, checks each "
        "digest AND each row count, refuses a population whose accessions this "
        "platform does not hold, and reports how many of the declared md5 still match "
        "the sequence stored here without storing that number."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        clave = payload.get("manifest_key") or "?"
        return f"declare the exam population described by {clave}"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = DeclareExamPopulationPayload.model_validate(contract_payload(payload))
        store = get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))
        m = _manifest(p, store)
        leidos = _verify_every_file(m, store)
        pop = _parse_population(leidos[m.data["population_file"]], m.exam)
        _check_partition(m, pop)
        medido = _check_between_files(m, pop, leidos)
        got = _resolve(session, pop.accessions)
        if got.missing:
            raise ValueError(
                f"{len(got.missing)} of the {len(pop.accessions)} proteins of {m.exam} have no "
                f"sequence in this platform, the first being {got.missing[:5]}. A population "
                "whose members this platform cannot resolve is not a set it can declare"
            )
        _must_fit(m.exam, leidos, pop, got)
        _refuse_a_second_row(session, m.digest)
        row = _declare(session, m, pop, got, job_id_from_payload(payload))
        result = {
            "query_set_id": str(row.id),
            "name": row.name,
            "exam": m.exam,
            "proteins": len(pop.accessions),
            "sequences": len(set(got.sequence_of.values())),
            "partition": m.data["partition"],
            # COUNTED from concepts.json, not copied from the manifest: this
            # number reaches the row's description, which is the visible
            # identity of the population.
            "concepts": medido["concepts_counted"],
            "between_files": medido,
            "artifact_uri": m.uri,
            "content_sha256": m.digest,
            "rule": EXAM_RULE,
            "files": {n: s["sha256"] for n, s in m.files.items()},
            "lab_commit": m.data.get("lab_commit"),
            "sequence_agreement": _agreement(pop, got),
            # What the exclusion protects against, measured: labels that fall
            # past the sequence stored here. Every one belongs to a protein
            # whose md5 differs, and the converse does not hold, so this is a
            # measure and never the exclusion rule.
            "labels_outside_this_platform": _outside(leidos, got),
        }
        emit(
            "exam_population.declared",
            f"{m.exam}: {len(pop.accessions)} proteins, manifest sha {m.digest[:12]}",
            result,
            "info",
        )
        return OperationResult(result=result)


def _check_partition(m: _Manifest, pop: _Population) -> None:
    """The halves counted from the rows must be the halves the manifest claims.

    Separate from the parse so the parse stays about the file's shape and this
    stays about the manifest's claim, which are two different ways to be wrong.
    """
    contado: dict[str, int] = {}
    for half in pop.half_of.values():
        contado[half] = contado.get(half, 0) + 1
    claimed = {str(k): int(v) for k, v in m.data["partition"].items()}
    if contado != claimed:
        raise ValueError(
            f"the population file of {m.exam} splits as {contado} and its manifest "
            f"claims {claimed}"
        )
