"""The evidence channel table of ADR-D59 (decision 2), declared in the database.

A photo's member rows are stored without their channel: the channel is a pure
function of a row's evidence code, its reference class and whether the protein is
reviewed at the cut, and that function is a declared table, keyed by what it
depends on (ADR-D56). Who assigned the row decides no channel, so it is not a key;
it stays in the refusal inventory (``unknown_evidence.tsv``), where it helps to
place a new key.

``evidence_channel_table`` is one row per version of the table, identified by the
digest every photo records (``protea.core.cut_photo.build.channel_table_digest``)
and dated by the day GO's ``GO_REF`` list was read. ``evidence_channel`` holds its
keys and channels (``protea.core.cut_photo.channels.channel_rows``), and
``evidence_channel_empty_go_ref`` how it reads a ``GO_REF`` with no number.

Two SQL functions let any reader compute a channel without Python.
``evidence_reference_class(table, db_reference, with_from)`` is
``protea.core.cut_photo.formats.reference_class`` read with that table, and
``evidence_channel_of(table, code, db_reference, with_from, reviewed)`` is the
channel, or NULL for a key the table does not list, which is a key the builder
refuses. The postgres tests check both against the Python on every key.

A version is written once, by the migration that introduced the tables or by the
first ``load_cut_photo`` that classified with it, and every later load verifies it
instead of writing it again.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    String,
    Text,
    event,
    func,
    inspect,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from protea.infrastructure.orm.base import Base

#: ``protea.core.cut_photo.channels.CHANNELS``, restated so the schema does not
#: import the builder (a test pins the two equal).
CHANNEL_CHECK = (
    "channel IN ('curated', 'no_data', 'orthology_electronic', 'phylogenetic', "
    "'reviewed_field_electronic', 'signature_electronic', 'truth')"
)
#: ADR-D58, decision 2: a short descriptor is a lowercase slug of at most 64 characters.
SHORT_NAME_CHECK = "short_name ~ '^[a-z0-9][a-z0-9-]*$' AND length(short_name) <= 64"
TABLE_CHECK = "length(sha256) = 64 AND length(description) > 0"

#: ``formats.reference_class`` in SQL, reading an empty ``GO_REF`` with the table's
#: own rule. A numbered ``GO_REF`` anywhere in the field wins, then a publication,
#: then an empty ``GO_REF`` read through its with/from when every item is in one
#: vocabulary the table lists (``empty_go_ref`` otherwise), and anything else is
#: ``other``. A NULL field reads as an empty one.
REFERENCE_CLASS_FUNCTION = r"""
CREATE OR REPLACE FUNCTION evidence_reference_class(
    p_table text, p_reference text, p_with_from text
) RETURNS text
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN p_reference ~ 'GO_REF:[0-9]{7}'
            THEN substring(p_reference FROM 'GO_REF:[0-9]{7}')
        WHEN p_reference ~ '(^|\|)(PMID|DOI):' THEN 'publication'
        WHEN p_reference ~ '(^|\|)GO_REF:(\||$)' THEN coalesce((
            SELECT min(m.reads_as)
            FROM unnest(string_to_array(p_with_from, '|')) AS item
            LEFT JOIN evidence_channel_empty_go_ref m
                ON m.table_sha256 = p_table AND m.with_from_prefix = split_part(item, ':', 1)
            WHERE item <> ''
            HAVING count(*) > 0
                AND count(*) = count(m.reads_as)
                AND count(DISTINCT m.reads_as) = 1
        ), 'empty_go_ref')
        ELSE 'other'
    END
$$
"""

#: The channel of one row: a code that decides its channel alone matches its ``*``
#: row; any other is looked up by its reference class. NULL for an unlisted key.
CHANNEL_FUNCTION = r"""
CREATE OR REPLACE FUNCTION evidence_channel_of(
    p_table text, p_code text, p_reference text, p_with_from text, p_reviewed boolean
) RETURNS text
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT coalesce(
        (SELECT c.channel FROM evidence_channel c
          WHERE c.table_sha256 = p_table AND c.evidence_code = p_code
            AND c.reference_class = '*' AND c.reviewed = p_reviewed),
        (SELECT c.channel FROM evidence_channel c
          WHERE c.table_sha256 = p_table AND c.evidence_code = p_code
            AND c.reference_class = evidence_reference_class(p_table, p_reference, p_with_from)
            AND c.reviewed = p_reviewed)
    )
$$
"""


class EvidenceChannelTable(Base):
    """One version of the channel table, named by the digest a photo records."""

    __tablename__ = "evidence_channel_table"
    __table_args__ = (
        CheckConstraint(SHORT_NAME_CHECK, name="ck_evidence_channel_table_short_name"),
        CheckConstraint(TABLE_CHECK, name="ck_evidence_channel_table_identity"),
    )

    sha256: Mapped[str] = mapped_column(String(64), primary_key=True)
    #: The day GO's GO_REF list was read to classify the electronic references.
    goref_list_read_on: Mapped[date] = mapped_column(Date, nullable=False)
    #: ADR-D58, derived from the code that declared the table, never typed.
    short_name: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    #: The revision and job that wrote it; both NULL when the migration seeded it.
    code_revision: Mapped[str | None] = mapped_column(String(64), nullable=True)
    job_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("job.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class EvidenceChannel(Base):
    """One key of a channel table and its channel."""

    __tablename__ = "evidence_channel"
    __table_args__ = (CheckConstraint(CHANNEL_CHECK, name="ck_evidence_channel_channel"),)

    table_sha256: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("evidence_channel_table.sha256", ondelete="RESTRICT"),
        primary_key=True,
    )
    evidence_code: Mapped[str] = mapped_column(String(8), primary_key=True)
    #: A ``GO_REF`` for ``IEA``; ``*`` for a code whose channel ignores the reference.
    reference_class: Mapped[str] = mapped_column(String(32), primary_key=True)
    reviewed: Mapped[bool] = mapped_column(Boolean, primary_key=True)
    channel: Mapped[str] = mapped_column(String(32), nullable=False)
    #: The reference's title in GO's list, word for word; NULL under ``*``.
    reference_title: Mapped[str | None] = mapped_column(Text, nullable=True)


class EvidenceChannelEmptyGoRef(Base):
    """How a channel table reads a ``GO_REF`` with no number: by its with/from."""

    __tablename__ = "evidence_channel_empty_go_ref"

    table_sha256: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("evidence_channel_table.sha256", ondelete="RESTRICT"),
        primary_key=True,
    )
    with_from_prefix: Mapped[str] = mapped_column(String(64), primary_key=True)
    reads_as: Mapped[str] = mapped_column(String(32), nullable=False)


@event.listens_for(Base.metadata, "after_create")
def _create_the_functions(_metadata: Any, connection: Any, **_kw: Any) -> None:
    """Create the two functions once ``create_all`` has made their tables, on Postgres.

    A SQL function's body is checked against the tables it reads when it is
    created, so they come after every table, and only when both tables were made:
    some tests create a few tables of the schema and never these. Through ``text``
    and not ``sqlalchemy.DDL``, which would read the ``%`` of a pattern as a
    substitution (see ``exploration_event``).
    """
    if connection.dialect.name != "postgresql":
        return
    present = inspect(connection)
    if not all(
        present.has_table(name) for name in ("evidence_channel", "evidence_channel_empty_go_ref")
    ):
        return
    connection.execute(text(REFERENCE_CLASS_FUNCTION))
    connection.execute(text(CHANNEL_FUNCTION))
