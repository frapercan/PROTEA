from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from protea.core.explorations import check_in
from protea.infrastructure.orm.base import Base

#: How a query set's population was chosen. A CLOSED vocabulary, because the
#: kind is what says which of the other columns must be present: a derived set
#: has a window, a sha and a job, and an uploaded one cannot have a window.
#:
#: ``legacy_undeclared`` is a named value and NOT a NULL on purpose. NULL
#: cannot tell "nobody has written this yet" from "we looked and it cannot be
#: established", and those are different facts about a set.
QUERY_SET_SOURCES: tuple[str, ...] = (
    "evaluation_targets",
    "fasta_upload",
    "quick_annotation",
    #: The corpus's own proteins: the one population everything else is
    #: measured over. It is not an upload, not a window's delta, and
    #: ``legacy_undeclared`` would say "we looked and it cannot be
    #: established" about the only population that CAN be established
    #: exactly. A vector bank is a RECIPE applied to a POPULATION, and until
    #: this value existed only the recipe had an identity.
    "corpus_population",
    #: One of the two frozen exam populations of ADR-D57, over Swiss-Prot
    #: 2022_03: a sample with a partition into a selection and a confirmation
    #: half, the second of which is spent once. It is NOT the corpus and it
    #: has no evaluation window at all, which is also why these exams do not
    #: fit ADR-D55's exploration unit. Its artifact is a MANIFEST, because an
    #: exam population is several files and a row has one uri.
    "exam_population",
    "legacy_undeclared",
)

if TYPE_CHECKING:
    from protea.infrastructure.orm.models.sequence.sequence import Sequence


class QuerySet(Base):
    """A frozen POPULATION of proteins to ask about, and where it came from.

    Entries preserve the original accession strings and link to the
    deduplicated ``Sequence`` rows, so the same physical sequence can appear in
    many query sets without duplication.

    WHAT THIS DOCSTRING USED TO SAY, because the correction is the point. It
    said "a named collection of sequences uploaded by the user", and that was
    true of the endpoint and false of every row that matters: the four real
    query sets here are the DELTA OF A WINDOW, written by
    ``export_evaluation_targets`` and then registered by a person who typed the
    provenance into ``description`` by hand. So the derivation of the
    population everything is measured over lived in prose:

        "Las 40.531 dianas de la ventana ea0f4334 (projected,
        live_in_both_ends), exportadas por el job a9b1c60e con sha256
        c4f15a254272..."

    Every fact needed to attribute a number, in a sentence. The window is a
    prefix a reader has to resolve, the sha is a string nothing compares
    against, and two of those sets are named almost identically
    (``dianas-valid-211-227`` and ``dianas-valid-211-227-titular``) while
    coming from DIFFERENT windows, ``ea94e57e`` and ``ea0f4334``, with 63.734
    and 40.531 targets. A name is not a declaration.

    THE LATENT HAZARD IN THAT ROUTE, measured and not realised. The exported
    FASTA carries ``canonical_residues`` (upper case, gaps removed), so
    registering those bytes through the upload endpoint re-hashes the
    CANONICALISED sequence. Where that differs from the stored one the endpoint
    inserts a second ``sequence`` row, and the entries then point at sequences
    with no embedding. On 2026-11-08 this had not happened: all 138.297 entries
    point at their protein's own sequence, 0 mismatches, because the
    canonicalisation was a no-op for this population. It is a trap that has not
    sprung, which is the worst kind to leave armed.

    So the operation that knows the derivation writes it, and these columns are
    where it goes. ``source_kind`` says which of them must be present, and the
    CHECK constraints make an undeclared derived set unexpressible rather than
    discouraged.
    """

    __tablename__ = "query_set"
    __table_args__ = (
        # T3.5: list endpoints order by ``created_at DESC``.
        Index("ix_query_set_created_at", "created_at"),
        CheckConstraint(
            check_in("source_kind", QUERY_SET_SOURCES), name="ck_query_set_source_kind"
        ),
        # The kind decides what must be there. A derived set with no window is
        # the defect this table had, so it cannot be written any more.
        CheckConstraint(
            "source_kind <> 'evaluation_targets' OR ("
            "evaluation_set_id IS NOT NULL AND content_sha256 IS NOT NULL "
            "AND job_id IS NOT NULL AND description IS NOT NULL)",
            name="ck_query_set_targets_are_declared",
        ),
        # A locator without an identity is what made the ground truth readable
        # without being verifiable, so a uri REQUIRES its sha. Not the
        # converse: an uploaded FASTA is hashed and not kept, so its bytes have
        # an identity and no location, and that is a set that can still prove
        # which population it is.
        CheckConstraint(
            "artifact_uri IS NULL OR content_sha256 IS NOT NULL",
            name="ck_query_set_uri_with_its_sha",
        ),
        # Idempotence: one window plus one set of bytes is ONE population. A
        # re-export of the same delta must find this row, not make a second.
        #
        # IT DOES NOT COVER A CORPUS POPULATION, and that is worth knowing
        # rather than discovering: that kind has ``evaluation_set_id`` NULL by
        # its own CHECK, and in Postgres two NULLs do not collide, so this
        # constraint would admit a second row with the SAME sha. The operation
        # that writes one therefore looks for an existing row by (kind, sha)
        # and refuses, because the constraint cannot.
        UniqueConstraint("evaluation_set_id", "content_sha256", name="uq_query_set_window_content"),
        # The four criteria of the set guard as a constraint. The last clause
        # is the one that carries work: a corpus population is not a window's
        # delta, so naming an evaluation set here would be a category error
        # with a foreign key behind it.
        CheckConstraint(
            "source_kind <> 'corpus_population' OR ("
            "artifact_uri IS NOT NULL AND content_sha256 IS NOT NULL "
            "AND description IS NOT NULL AND job_id IS NOT NULL "
            "AND evaluation_set_id IS NULL)",
            name="ck_query_set_corpus_is_declared",
        ),
        # The same four criteria for the exam populations, and the comment
        # above about two NULLs not colliding applies to them too: the
        # operation that writes one refuses a duplicate itself.
        CheckConstraint(
            "source_kind <> 'exam_population' OR ("
            "artifact_uri IS NOT NULL AND content_sha256 IS NOT NULL "
            "AND description IS NOT NULL AND job_id IS NOT NULL "
            "AND evaluation_set_id IS NULL)",
            name="ck_query_set_exam_is_declared",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    #: How the population was chosen, from :data:`QUERY_SET_SOURCES`. It is
    #: NOT NULL because every set was chosen somehow; the rows that predate
    #: the column say ``legacy_undeclared``, which is a statement and not a
    #: gap.
    source_kind: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default="legacy_undeclared"
    )
    #: The window this population is the delta OF, for ``evaluation_targets``.
    #: RESTRICT and not SET NULL: a window that something asks about is not a
    #: window to delete quietly, and nulling this would turn a declared set
    #: into an undeclared one without anybody choosing that.
    evaluation_set_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("evaluation_set.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )
    #: Where the bytes this set was registered from live.
    artifact_uri: Mapped[str | None] = mapped_column(String(512), nullable=True)
    #: The sha256 of those bytes. This is the number both sides quote: a LAFA
    #: ``--query_file`` of these bytes and this set are the same population,
    #: and anything else is two populations that happen to agree.
    content_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    #: The job that built it. RESTRICT and not SET NULL, which is the desktop
    #: session's catch: the CHECK below requires this for a derived set, so a
    #: SET NULL would make deleting that job fail with a CHECK violation that
    #: names a constraint and explains nothing. RESTRICT says what happened.
    #: The job IS the provenance of a declared set, so it is not a row to
    #: delete under one.
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("job.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )
    #: The revision of the code that built it. The declaration says what the
    #: set IS and the revision says what was RUN, and those are different
    #: quantities: on 2026-11-07 a fix to the synonym map moved the delta of a
    #: whole class of windows with every identity field untouched.
    #:
    #: 64 and not 40, matching ``evaluation_set.code_revision``:
    #: ``code_revision()`` returns ``"<40 hex>+dirty"`` when the tree it ran
    #: from is dirty, which is 46 characters. A 40-char column would refuse
    #: the INSERT in exactly the case the column exists to record.
    code_revision: Mapped[str | None] = mapped_column(String(64), nullable=True)

    entries: Mapped[list[QuerySetEntry]] = relationship(
        "QuerySetEntry", back_populates="query_set", cascade="all, delete-orphan"
    )


class QuerySetEntry(Base):
    """One sequence within a QuerySet, preserving the original FASTA accession.

    ``accession`` is the raw identifier from the FASTA header (may not exist in
    the ``protein`` table). ``sequence_id`` links to the deduplicated ``Sequence``
    row used for embedding computation and similarity search.
    """

    __tablename__ = "query_set_entry"

    __table_args__ = (
        UniqueConstraint("query_set_id", "accession", name="uq_query_set_entry_set_accession"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    query_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("query_set.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    sequence_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("sequence.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    accession: Mapped[str] = mapped_column(String, nullable=False, index=True)
    taxonomy_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    species: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    query_set: Mapped[QuerySet] = relationship("QuerySet", back_populates="entries")
    sequence: Mapped[Sequence] = relationship("Sequence")
