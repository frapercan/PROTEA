"""Write a method's own predictions into the platform, with its provenance.

Every prediction set PROTEA holds was produced by PROTEA: ``predict_go_terms``
retrieves neighbours and transfers their terms, and the score is derived at
evaluation time from the retrieval columns. A method trained and run elsewhere
-- the lab's readers, a classifier, anything that is not neighbour transfer --
produces a probability per (protein, term) and nothing for those columns to
hold. Before ``method_score`` existed there was no way to get that number
through the evaluator except by writing it into a column that means something
else, and the one that works is the worst kind: ``distance = 2 * (1 - p)``
comes back out of ``embedding_similarity = 1 - d / 2`` exactly, while
recording a cosine distance to a neighbour that does not exist.

So this is the entry point for a method that scores its own candidates. It
reads the TSV the method wrote, resolves its terms against the snapshot the
window is scored in, and writes ``go_prediction`` rows whose score is the
method's own. The run is then scored by ``run_cafa_evaluation`` like any
other, under a ``ScoringConfig`` with formula ``method_score``.

WHAT IT REFUSES, AND WHY EACH ONE. A method's output arriving from another
machine is the one input nothing else in the platform checks, so the gates are
the operation:

* the file's own fourth column. The lab's dumps carry ``aspect`` beside the
  term, which the scorer does not need and which this reads as a CHECK against
  ``go_term.aspect`` under the snapshot. It is cheap and it is not theatre:
  measured on the long window's two frozen dumps, one disagreement in 893,481
  rows (``GO:2001317``) and none in 5,295,100. The default allowance is zero,
  so the first file that disagrees stops and says which term, and whoever runs
  it decides;
* terms the snapshot does not have. A prediction for a term absent from the
  ontology the window is scored in cannot be scored at all, so it is counted
  and, by default, refused rather than dropped. ``cafaeval`` would drop it in
  silence and the coverage would move with no record of why;
* a cap per protein the payload declares and the file exceeds. When the
  payload declares none, the OBSERVED maximum is written to
  ``prediction_set.limit_per_entry``, which is NOT NULL and would otherwise
  hold a number nobody measured;
* a method with no name. The name identifies the arm and is what
  ``ref_protein_accession`` carries, following
  ``predict_go_terms_from_interpro``, which puts the InterPro accession that
  voted there: the column says what produced the row, not always a donor
  protein.

``distance`` is left NULL. A method with no neighbour has no distance, and
NULL is the only truthful value; the scorer drops the signal out of the
average rather than reading a zero into it.

WHAT IT DOES NOT CLOSE. ``prediction_set`` has no column naming the method,
so two sets ingested here over the same window, population and ontology are
indistinguishable to ``protea.core.method_seal``: ``assert_one_axis``
computes ``differ`` over ``METHOD_IDENTITY_FIELDS``, none of which says
method, and with nothing differing and no axis declared it returns ``()`` and
lets the comparison pass as "the method stayed still". The provenance block
this writes into ``meta`` is a record, not a gate -- ``_CLASSIFIED_META``
rejects unknown meta keys by name, which is why it lands under one key the
seal already tolerates rather than as eleven new ones. Naming the method in
the seal is a separate, reviewed change.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Iterator
from typing import Annotated, Any

from pydantic import Field, field_validator, model_validator
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from protea.core.contracts.operation import (
    EmitFn,
    OperationResult,
    ProteaPayload,
)
from protea.core.operations._encoder_artifact import resolve_encoder_artifact
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.orm.models.embedding.embedding_config import (
    INGESTED_METHOD_BACKEND,
    EmbeddingConfig,
)
from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction
from protea.infrastructure.orm.models.embedding.prediction_set import PredictionSet

#: The columns the file must declare, in order. The fourth is redundant for
#: scoring and load-bearing as a check; see the module docstring.
TSV_COLUMNS: tuple[str, ...] = ("protein", "go_id", "aspect", "score")

#: How many rows ride in one INSERT. Rows are streamed rather than read whole:
#: the long window's dump is 5,295,100 lines and holding it as Python tuples
#: costs about a gigabyte, on a machine whose /tmp is RAM.
#:
#: NOT the number that goes in the statement. The PostgreSQL wire protocol
#: carries at most 65,535 bound parameters per statement, and a chunk costs
#: one parameter PER COLUMN, so 50,000 rows of a six-column row is 300,000 and
#: the driver refuses before the server sees it:
#:
#:     (psycopg.OperationalError) sending query and params failed:
#:     number of parameters must be between 0 and 65535
#:
#: Measured the hard way on the lab's first real file, 12,159,300 rows: the
#: three-row fixtures in the tests never came near it. :func:`_rows_per_insert`
#: derives the real cap from the row itself, so adding a column narrows the
#: chunk instead of breaking the insert.
DEFAULT_CHUNK = 50_000

#: What the protocol allows in one statement.
MAX_BOUND_PARAMS = 65_535

PositiveInt = Annotated[int, Field(gt=0)]


class IngestMethodPredictionsPayload(ProteaPayload, frozen=True):
    """Where the file is, what window it belongs to, and what produced it."""

    #: Exactly one of these two, like ``apply_learned_encoder``: a store key
    #: (what ``export_evaluation_targets`` returns as ``key``) or a local path.
    predictions_uri: str | None = None
    predictions_path: str | None = None
    #: The digest of the file as the producer published it. Verified against
    #: the bytes read, so a truncated or swapped dump is refused rather than
    #: ingested. A content-addressed key is verified anyway by the resolver.
    predictions_sha256: str | None = None

    #: The three axes ``prediction_set`` pins, all NOT NULL with FK RESTRICT.
    #: For a method they are traceability and not a recipe: the embedding
    #: config names the model, the annotation set the corpus it saw at t0, and
    #: the snapshot the ontology its terms are read in.
    embedding_config_id: str
    annotation_set_id: str
    ontology_snapshot_id: str
    query_set_id: str | None = None

    #: What the method capped at per protein. Omitted means "measure it".
    limit_per_entry: PositiveInt | None = None

    #: The producer's own record: name, page, shas, commit, band, models.
    #: Stored verbatim under ``meta["method"]``; ``name`` is required because
    #: it identifies the arm and is written on every row.
    method: dict[str, Any] = Field(default_factory=dict)

    #: Share of rows whose term is absent from the snapshot that may pass.
    max_unknown_terms_pct: float = 0.0
    #: Rows whose declared aspect disagrees with the snapshot that may pass.
    max_aspect_disagreements: int = 0
    chunk_size: PositiveInt = DEFAULT_CHUNK

    @model_validator(mode="after")
    def _exactly_one_source(self) -> IngestMethodPredictionsPayload:
        if bool(self.predictions_uri) == bool(self.predictions_path):
            raise ValueError(
                "give exactly one of predictions_uri (a store key) and "
                "predictions_path (a local file)"
            )
        return self

    @field_validator("method")
    @classmethod
    def _must_name_itself(cls, value: dict[str, Any]) -> dict[str, Any]:
        name = str(value.get("name", "")).strip()
        if not name:
            raise ValueError(
                "method.name is required: it identifies the arm, it is written "
                "on every row, and a prediction set that cannot say what "
                "produced it is the thing this operation exists to prevent"
            )
        return value


def _term_ids(session: Session, snapshot_id: uuid.UUID) -> dict[str, tuple[int, str]]:
    """``{go_id: (go_term_id, aspect)}`` for one snapshot.

    Both halves in one pass: the id is what the row needs and the aspect is
    what the file's fourth column is checked against, and asking twice would
    let the two answers come from different queries.
    """
    rows = session.execute(
        select(GOTerm.go_id, GOTerm.id, GOTerm.aspect).where(
            GOTerm.ontology_snapshot_id == snapshot_id
        )
    ).all()
    return {go_id: (term_id, aspect) for go_id, term_id, aspect in rows}


class _Counters:
    """What the file turned out to contain, as it is read."""

    def __init__(self) -> None:
        self.rows = 0
        self.unknown_terms = 0
        self.unknown_examples: list[str] = []
        self.aspect_disagreements = 0
        self.aspect_examples: list[str] = []
        self.terms_per_protein: dict[str, int] = {}

    @property
    def proteins(self) -> int:
        return len(self.terms_per_protein)

    @property
    def max_terms_per_protein(self) -> int:
        return max(self.terms_per_protein.values(), default=0)


def _lines(path: str) -> Iterator[list[str]]:
    """The file's rows, header checked.

    The header is required rather than sniffed: a file whose first line is a
    prediction would otherwise lose that prediction to a skip, and a file
    whose columns moved would be read in the wrong order with no complaint.
    """
    with open(path, encoding="utf-8") as handle:
        header = handle.readline().rstrip("\n").split("\t")
        if tuple(header) != TSV_COLUMNS:
            raise ValueError(
                f"{path} declares columns {header}; this reads "
                f"{list(TSV_COLUMNS)}, in that order"
            )
        for line in handle:
            yield line.rstrip("\n").split("\t")


def scan(path: str, terms: dict[str, tuple[int, str]]) -> _Counters:
    """Read the file once and report what it contains, writing nothing.

    TWO PASSES, ON PURPOSE. The second one needs a ``prediction_set`` row to
    point at, and that row's ``limit_per_entry`` is NOT NULL and is a
    method-identity field in :mod:`protea.core.method_seal`: when the payload
    does not declare a cap, the only honest value is the one the file turns
    out to have, which is not known until it has been read. Filling it
    provisionally and updating it afterwards would publish an identity field
    nobody measured, and the first pass costs a local read.

    It also means a file that fails a gate leaves NO rows behind.
    """
    counters = _Counters()
    for protein, go_id, aspect, _score in _lines(path):
        counters.rows += 1
        known = terms.get(go_id)
        if known is None:
            counters.unknown_terms += 1
            if len(counters.unknown_examples) < 10:
                counters.unknown_examples.append(go_id)
            continue
        _term_id, snapshot_aspect = known
        if aspect != snapshot_aspect:
            counters.aspect_disagreements += 1
            if len(counters.aspect_examples) < 10:
                counters.aspect_examples.append(f"{go_id} file={aspect} db={snapshot_aspect}")
        counters.terms_per_protein[protein] = counters.terms_per_protein.get(protein, 0) + 1
    return counters


def _read_rows(
    path: str,
    terms: dict[str, tuple[int, str]],
    set_id: uuid.UUID,
    method_name: str,
) -> Iterator[dict[str, Any]]:
    """Stream the file again, now as insert rows for a set that exists."""
    for protein, go_id, _aspect, score in _lines(path):
        known = terms.get(go_id)
        if known is None:
            continue
        term_id, _snapshot_aspect = known
        yield {
            "prediction_set_id": set_id,
            "protein_accession": protein,
            "go_term_id": term_id,
            # What produced the row, as predict_go_terms_from_interpro writes
            # the IPR accession that voted. Not a donor protein.
            "ref_protein_accession": method_name,
            # NULL: a method with no neighbour has no distance.
            "distance": None,
            "method_score": float(score),
        }


class IngestMethodPredictionsOperation:
    """Read a method's TSV and write its prediction set."""

    name = "ingest_method_predictions"
    description = (
        "Ingest a method's own (protein, go_id, score) predictions as a "
        "PredictionSet whose score column is the method's, with its provenance."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        method = (payload.get("method") or {}).get("name", "an unnamed method")
        return f"ingest the predictions of {method}"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = IngestMethodPredictionsPayload.model_validate(contract_payload(payload))
        config, corpus, snapshot = self._resolve(session, p)
        path = resolve_encoder_artifact(p.predictions_path, p.predictions_uri)
        digest = self._verify(path, p.predictions_sha256, emit)

        terms = _term_ids(session, snapshot.id)
        counters = scan(path, terms)
        self._gate(p, counters)

        set_id = uuid.uuid4()
        session.add(
            PredictionSet(
                id=set_id,
                embedding_config_id=config.id,
                annotation_set_id=corpus.id,
                ontology_snapshot_id=snapshot.id,
                query_set_id=uuid.UUID(p.query_set_id) if p.query_set_id else None,
                limit_per_entry=p.limit_per_entry or counters.max_terms_per_protein,
                meta={
                    "algorithm": "ingested_method",
                    "method": p.method,
                    "source": {
                        "uri": p.predictions_uri,
                        "path": p.predictions_path,
                        "sha256": digest,
                        "rows": counters.rows,
                        "unknown_terms": counters.unknown_terms,
                        "aspect_disagreements": counters.aspect_disagreements,
                    },
                },
            )
        )
        # Flushed before the rows: go_prediction.prediction_set_id is a
        # foreign key, so the parent has to exist for the first chunk.
        session.flush()
        rows = _read_rows(path, terms, set_id, str(p.method["name"]).strip())
        inserted = self._insert(session, rows, p.chunk_size)
        return self._report(p, counters, inserted, set_id, digest, emit)

    @staticmethod
    def _resolve(
        session: Session, p: IngestMethodPredictionsPayload
    ) -> tuple[EmbeddingConfig, AnnotationSet, OntologySnapshot]:
        """The three rows the set points at, or a refusal naming the missing one."""
        found = []
        for model, raw in (
            (EmbeddingConfig, p.embedding_config_id),
            (AnnotationSet, p.annotation_set_id),
            (OntologySnapshot, p.ontology_snapshot_id),
        ):
            row = session.get(model, uuid.UUID(raw))
            if row is None:
                raise ValueError(f"{model.__name__} {raw} not found")
            found.append(row)
        config = found[0]
        # The backend tag has to be the one this operation writes under, and
        # the refusal is what makes it mean anything: ``_evaluation_leakage``
        # treats ``ingested-method`` as evidence that the rows came from a
        # model fitted elsewhere, and therefore demands a training cut. If any
        # config could be named here, that evidence would be a convention.
        if config.model_backend != INGESTED_METHOD_BACKEND:
            raise ValueError(
                f"EmbeddingConfig {config.id} has backend "
                f"{config.model_backend!r}; an ingested method's set must point "
                f"at a config whose backend is {INGESTED_METHOD_BACKEND!r}, "
                "which is what tells the leakage guard this arm was fitted and "
                "owes a cut"
            )
        return config, found[1], found[2]

    @staticmethod
    def _verify(path: str, expected: str | None, emit: EmitFn) -> str:
        """The digest of the bytes read, refused if it is not the one declared."""
        sha = hashlib.sha256()
        with open(path, "rb") as handle:
            for block in iter(lambda: handle.read(1 << 20), b""):
                sha.update(block)
        digest = sha.hexdigest()
        if expected and digest != expected:
            raise ValueError(
                f"{path} hashes to {digest} and the payload declares {expected}; "
                "the file is not the one the job was asked to ingest"
            )
        emit(
            "ingest_method_predictions.file",
            f"sha256 {digest[:12]}",
            {"sha256": digest, "declared": bool(expected)},
            "info",
        )
        return digest

    @staticmethod
    def _insert(session: Session, rows: Iterator[dict[str, Any]], chunk_size: int) -> int:
        """Insert in chunks, letting the unique constraint reject a repeat.

        ``on_conflict_do_nothing`` on ``uq_go_prediction_set_protein_term``, as
        ``predict_go_terms_from_interpro`` does, so a retry on the same set is
        safe. The count of rows that did NOT land is reported rather than
        swallowed: a dump with the same pair twice is a producer bug, and the
        difference is the only place it shows.
        """
        inserted = 0
        limit = chunk_size
        chunk: list[dict[str, Any]] = []
        for row in rows:
            if not chunk:
                # Resolved from the first row and then fixed: every row the
                # builder makes has the same columns.
                limit = _rows_per_insert(row, chunk_size)
            chunk.append(row)
            if len(chunk) >= limit:
                inserted += _flush(session, chunk)
                chunk = []
        if chunk:
            inserted += _flush(session, chunk)
        return inserted

    @staticmethod
    def _gate(p: IngestMethodPredictionsPayload, counters: _Counters) -> None:
        """Refuse on what the file said about itself, after reading it all.

        After, not during: a file that fails on its last line should still
        report how many rows disagreed, and a gate that raises on the first
        one turns a measurement into an anecdote.
        """
        if not counters.rows:
            raise ValueError("the file carries no prediction rows")
        drift = 100.0 * counters.unknown_terms / counters.rows
        if drift > p.max_unknown_terms_pct:
            raise ValueError(
                f"{counters.unknown_terms} of {counters.rows} rows name a term the "
                f"snapshot does not have ({drift:.4f}% over the allowed "
                f"{p.max_unknown_terms_pct}%), e.g. {counters.unknown_examples}. "
                "Those rows cannot be scored at all"
            )
        if counters.aspect_disagreements > p.max_aspect_disagreements:
            raise ValueError(
                f"{counters.aspect_disagreements} rows declare an aspect the "
                f"snapshot disagrees with (allowed {p.max_aspect_disagreements}), "
                f"e.g. {counters.aspect_examples}"
            )
        if p.limit_per_entry and counters.max_terms_per_protein > p.limit_per_entry:
            raise ValueError(
                f"the file carries up to {counters.max_terms_per_protein} terms for a "
                f"protein and the payload declares a cap of {p.limit_per_entry}"
            )

    @staticmethod
    def _report(
        p: IngestMethodPredictionsPayload,
        counters: _Counters,
        inserted: int,
        set_id: uuid.UUID,
        digest: str,
        emit: EmitFn,
    ) -> OperationResult:
        scored = counters.rows - counters.unknown_terms
        result = {
            "prediction_set_id": str(set_id),
            "method": p.method.get("name"),
            "sha256": digest,
            "rows": counters.rows,
            "inserted": inserted,
            "repeated_pairs": scored - inserted,
            "proteins": counters.proteins,
            "unknown_terms": counters.unknown_terms,
            "aspect_disagreements": counters.aspect_disagreements,
            "max_terms_per_protein": counters.max_terms_per_protein,
            "limit_per_entry": p.limit_per_entry or counters.max_terms_per_protein,
            "caveat": (
                "score this set with a ScoringConfig whose formula is "
                "method_score. Under any other formula a method row has no "
                "signal with a weight and the cell comes out zero"
            ),
        }
        emit(
            "ingest_method_predictions.stored",
            f"{inserted} rows over {counters.proteins} proteins",
            result,
            "info",
        )
        return OperationResult(result=result)


def _rows_per_insert(row: dict[str, Any], asked: int) -> int:
    """How many of these rows fit in one statement, at most ``asked``.

    Derived from the row, not configured: a column added to the row builder
    narrows this automatically, which is the difference between a cap that
    stays true and a number somebody remembered.
    """
    return max(1, min(asked, MAX_BOUND_PARAMS // max(1, len(row))))


def _flush(session: Session, chunk: list[dict[str, Any]]) -> int:
    """One INSERT, returning how many rows actually landed."""
    stmt = (
        pg_insert(GOPrediction.__table__)
        .values(chunk)
        .on_conflict_do_nothing(constraint="uq_go_prediction_set_protein_term")
        .returning(GOPrediction.__table__.c.id)
    )
    return len(session.execute(stmt).fetchall())
