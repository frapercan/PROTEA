"""The evidence channel table a photo is loaded with (ADR-D59, decision 2).

Written once, under the digest every photo records, and verified by every later
load: ``load_cut_photo`` calls ``check_stored_channel_table`` before it writes
anything and ``declare_channel_table`` in its transaction.
"""

from __future__ import annotations

import uuid
from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.code_revision import code_revision
from protea.core.cut_photo.build import channel_table_digest
from protea.core.cut_photo.channels import (
    GOREF_LIST_READ_ON,
    channel_rows,
    channel_table_descriptors,
)
from protea.core.cut_photo.formats import EMPTY_GO_REF_BY_WITH_FROM
from protea.core.operations._cut_photo_errors import PhotoRefused
from protea.infrastructure.orm.models.annotation.evidence_channel import (
    EvidenceChannel,
    EvidenceChannelEmptyGoRef,
    EvidenceChannelTable,
)


def _declared_channel_keys() -> dict[tuple[str, str, bool], tuple[str, str | None]]:
    return {
        (row.evidence_code, row.reference_class, row.reviewed): (row.channel, row.reference_title)
        for row in channel_rows()
    }


def check_stored_channel_table(session: Session) -> None:
    """Refuse a stored channel table under this code's digest that is not this code's.

    The table is written once, under the digest the photos record, and every reader
    classifies with what is stored. A stored version that differs from the code's
    (another day's GO_REF list, another key or channel, another reading of an empty
    ``GO_REF``) would make the readers disagree with the photo, so the load stops
    before writing anything. Nothing stored yet is not a refusal: the load writes it.
    """
    sha = channel_table_digest()
    version = session.get(EvidenceChannelTable, sha)
    if version is None:
        return
    stored = {
        (r.evidence_code, r.reference_class, r.reviewed): (r.channel, r.reference_title)
        for r in session.execute(
            select(EvidenceChannel).where(EvidenceChannel.table_sha256 == sha)
        ).scalars()
    }
    empty = dict(
        session.execute(
            select(
                EvidenceChannelEmptyGoRef.with_from_prefix, EvidenceChannelEmptyGoRef.reads_as
            ).where(EvidenceChannelEmptyGoRef.table_sha256 == sha)
        )
        .tuples()
        .all()
    )
    named = []
    if stored != _declared_channel_keys():
        named.append("its keys or channels")
    if empty != EMPTY_GO_REF_BY_WITH_FROM:
        named.append("its reading of an empty GO_REF")
    if version.goref_list_read_on.isoformat() != GOREF_LIST_READ_ON:
        named.append("the day GO's list was read")
    if named:
        raise PhotoRefused(
            f"channel table {sha} is stored with other contents than this code declares "
            f"({', '.join(named)}): a table is written once and never changed"
        )


def declare_channel_table(session: Session, job_id: uuid.UUID | None) -> bool:
    """Write the channel table this code declares unless it is stored; True if written.

    ``check_stored_channel_table`` has already refused a stored version that differs,
    so a stored one is this code's and is left as it is.
    """
    sha = channel_table_digest()
    if session.get(EvidenceChannelTable, sha) is not None:
        return False
    session.add(
        EvidenceChannelTable(
            sha256=sha,
            goref_list_read_on=date.fromisoformat(GOREF_LIST_READ_ON),
            code_revision=code_revision() or None,
            job_id=job_id,
            **channel_table_descriptors(sha),
        )
    )
    session.flush()
    session.add_all(EvidenceChannel(table_sha256=sha, **row._asdict()) for row in channel_rows())
    session.add_all(
        EvidenceChannelEmptyGoRef(table_sha256=sha, with_from_prefix=prefix, reads_as=reads_as)
        for prefix, reads_as in sorted(EMPTY_GO_REF_BY_WITH_FROM.items())
    )
    session.flush()
    return True
