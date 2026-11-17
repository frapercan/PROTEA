"""Give the members of a loaded photo their own taxon, from the photo's own rows.

``load_cut_photo`` writes each member's taxon as it loads a photo
(``cut_photo_member.taxon_id``). The photos loaded before that column existed get
it here, from the same file and by the same rule (:func:`member_taxa`).

The operation refuses before writing anything unless the photo it names is
loaded (found by its manifest's sha256) and ``rows.parquet`` still hashes to what
that manifest recorded. Then, in the job's transaction, it sets the taxon of
every member the rows name, and counts the members left without one. A rerun
writes the same values again, so it is a no-op in effect.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from pydantic import field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.cut_photo.taxa import member_taxa
from protea.core.source_guards import SourceGuardError
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto


class MemberTaxaRefused(SourceGuardError):
    """A photo whose members' taxa the operation will not write, with the reason."""


class DeclareMemberTaxaPayload(ProteaPayload, frozen=True):
    """The loaded photo by its manifest's sha256, and where its files are."""

    photo_dir: str
    manifest_sha256: str

    @field_validator("manifest_sha256", mode="before")
    @classmethod
    def digest_must_be_hex64(cls, v: Any) -> str:
        text = str(v).strip().lower()
        if len(text) != 64 or any(c not in "0123456789abcdef" for c in text):
            raise ValueError("manifest_sha256 must be 64 hexadecimal characters")
        return text


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 22), b""):
            digest.update(block)
    return digest.hexdigest()


def checked_rows(session: Session, photo_dir: Path, manifest_sha256: str) -> CutPhoto:
    """The loaded photo, refused unless ``rows.parquet`` is still the file it recorded."""
    photo = session.execute(
        select(CutPhoto).where(CutPhoto.manifest_sha256 == manifest_sha256)
    ).scalar_one_or_none()
    if photo is None:
        raise MemberTaxaRefused(f"no loaded photo has manifest {manifest_sha256}")
    recorded = (photo.manifest or {}).get("outputs", {}).get("rows.parquet")
    path = photo_dir / "rows.parquet"
    if recorded is None or not path.is_file() or _file_sha256(path) != recorded:
        raise MemberTaxaRefused(
            f"{path} is not the rows.parquet the photo of cut {photo.cut_release} recorded"
        )
    return photo


_STAGE = "CREATE TEMP TABLE member_taxon (accession varchar(32), taxon int4) ON COMMIT DROP"
_UPDATE = (
    "UPDATE cut_photo_member m SET taxon_id = t.taxon FROM member_taxon t "
    "WHERE m.photo_id = %(photo)s AND m.accession = t.accession"
)
_COUNT = "SELECT count(*), count(taxon_id) FROM cut_photo_member WHERE photo_id = %(photo)s"


def write_taxa(session: Session, photo: CutPhoto, taxa: dict[str, int]) -> dict[str, int]:
    """Set the taxa in one statement through a temporary table; count the members."""
    raw = session.connection().connection.driver_connection
    with raw.cursor() as cur:
        cur.execute(_STAGE)
        with cur.copy("COPY member_taxon (accession, taxon) FROM STDIN") as copy:
            for accession, taxon in taxa.items():
                copy.write_row((accession, taxon))
        cur.execute(_UPDATE, {"photo": photo.id})
        updated = int(cur.rowcount)
        cur.execute(_COUNT, {"photo": photo.id})
        members, with_taxon = cur.fetchone()
    return {
        "members": int(members),
        "members_given_a_taxon": updated,
        "members_with_taxon": int(with_taxon),
        "members_without_taxon": int(members) - int(with_taxon),
    }


class DeclareMemberTaxaOperation:
    """Fill ``cut_photo_member.taxon_id`` for a photo loaded before the column existed."""

    name = "declare_member_taxa"
    description = (
        "Give every member of a loaded photo its own NCBI taxon, read from the photo's "
        "own GOA rows after checking them against its manifest."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        digest = str(payload.get("manifest_sha256", ""))[:12]
        return f"member taxa of photo {digest}" if digest else ""

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = DeclareMemberTaxaPayload.model_validate(contract_payload(payload))
        photo_dir = Path(p.photo_dir)
        photo = checked_rows(session, photo_dir, p.manifest_sha256)
        taxa, read = member_taxa(photo_dir / "rows.parquet")
        emit("declare_member_taxa.read", None, {"cut": photo.cut_release, **read}, "info")
        counts = write_taxa(session, photo, taxa)
        result = {"cut": photo.cut_release, **counts, **read}
        emit("declare_member_taxa.done", None, result, "info")
        return OperationResult(result=result)


__all__ = [
    "DeclareMemberTaxaOperation",
    "DeclareMemberTaxaPayload",
    "MemberTaxaRefused",
]
