"""Load Swiss-Prot membership for one archived UniProt release.

The campaign's evaluation window is GOA 227 to 230, and on that stretch
contemporaneous Swiss-Prot membership cannot be read off the GAF: GOA replaced
the UniProtKB entry name in DB Object Synonym with the gene symbol, measured at
100% of rows on releases 164..178 and 0% on 179 and 180 (5% on 231). So the
window's membership comes from UniProt's own archived release, and this is the
operation that puts it in the database with its provenance.

Both routes reduce to the same bytes
------------------------------------

Whether the rows come from a streamed ``uniprot_sprot-only<REL>.tar.gz`` or
from a table extracted earlier, the operation canonicalises them to one
``"<accession>\\t<md5>\\n"`` line per entry sorted by accession, digests that,
and refuses the load unless the digest matches ``expected_sha256``. A download
and a handed-over table are therefore interchangeable and each is evidence
about the other: the same release loaded by either route produces the same row,
or one of them is wrong and the job says so instead of writing.
"""

from __future__ import annotations

import gzip
import hashlib
import tarfile
import time
from datetime import date
from typing import Any

import requests
from pydantic import field_validator
from sqlalchemy import insert, select
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.protein.swissprot_release import (
    SwissProtMembership,
    SwissProtRelease,
)

#: UniProt's archived release layout. The Swiss-Prot-only bundle is the small
#: one; the FASTA member inside it holds the canonical sequences.
_ARCHIVE_URL = (
    "https://ftp.uniprot.org/pub/databases/uniprot/previous_releases/"
    "release-{release}/knowledgebase/uniprot_sprot-only{release}.tar.gz"
)

#: The member that carries canonical sequences. ``varsplic`` carries the
#: isoforms and is deliberately NOT read: an isoform is not a Swiss-Prot entry
#: and counting it would inflate membership.
_FASTA_MEMBER = "uniprot_sprot.fasta.gz"

_INSERT_CHUNK = 10_000


class LoadSwissProtReleasePayload(ProteaPayload, frozen=True):
    """Which release to load, from where, and what it must hash to."""

    release: str
    #: ISO date, as a string: the payload is strict and a job's payload
    #: arrives as JSON, which has no date type. Read it as a ``date`` through
    #: :attr:`published_date`.
    published_at: str
    expected_sha256: str
    #: ``uniprot_archive`` streams the release from UniProt. ``local_table``
    #: reads an already-extracted ``accession<TAB>md5`` file.
    source: str = "uniprot_archive"
    local_path: str | None = None
    source_url: str | None = None
    #: The accession count the provenance claims, checked against the rows
    #: actually parsed when given.
    expected_accessions: int | None = None
    timeout_seconds: int = 900

    @field_validator("release", mode="before")
    @classmethod
    def release_must_look_like_a_uniprot_release(cls, v: Any) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("release must be a non-empty string, e.g. '2025_03'")
        text = v.strip()
        year, _, number = text.partition("_")
        if not (year.isdigit() and len(year) == 4 and number.isdigit()):
            raise ValueError(f"release must look like '2025_03', got {text!r}")
        return text

    @field_validator("expected_sha256", mode="before")
    @classmethod
    def digest_must_be_hex64(cls, v: Any) -> str:
        if not isinstance(v, str):
            raise ValueError("expected_sha256 must be a string")
        text = v.strip().lower()
        if len(text) != 64 or any(c not in "0123456789abcdef" for c in text):
            raise ValueError("expected_sha256 must be 64 hex characters")
        return text

    @field_validator("published_at", mode="before")
    @classmethod
    def published_at_must_be_an_iso_date(cls, v: Any) -> str:
        if not isinstance(v, str):
            raise ValueError("published_at must be an ISO date string, e.g. '2025-06-18'")
        try:
            parsed = date.fromisoformat(v.strip())
        except ValueError as exc:
            raise ValueError(f"published_at must be an ISO date, got {v!r}") from exc
        return parsed.isoformat()

    @property
    def published_date(self) -> date:
        """The publication date this release claims."""
        return date.fromisoformat(self.published_at)

    @field_validator("source", mode="before")
    @classmethod
    def source_must_be_known(cls, v: Any) -> str:
        allowed = {"uniprot_archive", "local_table"}
        if v not in allowed:
            raise ValueError(f"source must be one of {sorted(allowed)}, got {v!r}")
        return str(v)


def canonical_table(members: dict[str, str]) -> bytes:
    """The bytes a release resolves to, whatever route produced it.

    One ``"<accession>\\t<md5>\\n"`` line per entry, sorted by accession. The
    sort is what makes the digest a property of the release rather than of the
    order a file happened to be written in.
    """
    return "".join(f"{acc}\t{members[acc]}\n" for acc in sorted(members)).encode()


def parse_swissprot_fasta(lines: Any) -> dict[str, str]:
    """Accession to md5 of its sequence, from a Swiss-Prot FASTA stream.

    ``lines`` yields ``bytes``. The header is ``>sp|<accession>|<ENTRY_NAME>``
    and the md5 is taken over the sequence lines joined with nothing, each
    stripped, encoded UTF-8 -- the same convention the reference tables use, so
    the digests are comparable.

    :raises ValueError: on a record with no sequence, or a duplicated
        accession. Both would otherwise make the count silently wrong.
    """
    members: dict[str, str] = {}
    accession: str | None = None
    chunks: list[str] = []

    def flush() -> None:
        if accession is None:
            return
        if not chunks:
            raise ValueError(f"{accession} has a header but no sequence")
        digest = hashlib.md5("".join(chunks).encode()).hexdigest()  # noqa: S324
        if accession in members:
            raise ValueError(f"{accession} appears twice in the FASTA")
        members[accession] = digest

    for raw in lines:
        line = raw.decode("utf-8", "strict").strip()
        if not line:
            continue
        if line.startswith(">"):
            flush()
            parts = line.split("|")
            if len(parts) < 3:
                raise ValueError(f"unparseable FASTA header: {line[:80]!r}")
            accession = parts[1]
            chunks = []
        else:
            chunks.append(line)
    flush()
    return members


def read_local_table(path: str) -> dict[str, str]:
    """Accession to md5 from an already-extracted two-column table."""
    members: dict[str, str] = {}
    with open(path, encoding="utf-8") as handle:
        for number, raw in enumerate(handle, start=1):
            line = raw.rstrip("\n")
            if not line:
                continue
            accession, _, digest = line.partition("\t")
            if not accession or len(digest) != 32:
                raise ValueError(f"{path}:{number}: expected 'accession<TAB>md5', got {line[:80]!r}")
            if accession in members:
                raise ValueError(f"{path}:{number}: {accession} appears twice")
            members[accession] = digest
    return members


class LoadSwissProtReleaseOperation:
    """Persist one archived release's Swiss-Prot membership and sequence md5s.

    Idempotent on ``release``: a second run whose canonical digest matches the
    stored one is a no-op. A second run whose digest DIFFERS is refused rather
    than merged, because that is a changed definition of the release and not a
    retry -- silently replacing the rows would move every window that had
    already been scored against them.
    """

    name = "load_swissprot_release"
    description = (
        "Load Swiss-Prot membership and sequence md5 for one archived UniProt "
        "release, from the UniProt archive or a verified table; gated on the "
        "canonical sha256 and idempotent on the release name."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        data = payload or {}
        release = data.get("release", "")
        source = data.get("source", "uniprot_archive")
        if not release:
            return ""
        return f"Swiss-Prot {release} <- {source}"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = LoadSwissProtReleasePayload.model_validate(contract_payload(payload))
        started = time.perf_counter()

        url = p.source_url or _ARCHIVE_URL.format(release=p.release)
        emit(
            "load_swissprot_release.start",
            None,
            {"release": p.release, "source": p.source, "url": url},
            "info",
        )

        members = self._read(p, url, emit)
        blob = canonical_table(members)
        digest = hashlib.sha256(blob).hexdigest()

        emit(
            "load_swissprot_release.canonicalised",
            None,
            {"accessions": len(members), "sha256": digest, "bytes": len(blob)},
            "info",
        )

        if digest != p.expected_sha256:
            raise ValueError(
                f"release {p.release} canonical digest {digest} does not match the "
                f"declared {p.expected_sha256}; refusing to load. The canonical form "
                "is one 'accession<TAB>md5' line per entry sorted by accession."
            )
        if p.expected_accessions is not None and len(members) != p.expected_accessions:
            raise ValueError(
                f"release {p.release} parsed {len(members)} accessions but the "
                f"provenance declares {p.expected_accessions}"
            )

        existing = session.execute(
            select(SwissProtRelease).where(SwissProtRelease.release == p.release)
        ).scalar_one_or_none()
        if existing is not None:
            return self._already_loaded(existing, digest, len(members), emit)

        row = SwissProtRelease(
            release=p.release,
            published_at=p.published_date,
            source_url=url,
            accession_count=len(members),
            content_sha256=digest,
            job_id=job_id_from_payload(payload),
            meta={
                "source": p.source,
                "local_path": p.local_path,
                "canonical_bytes": len(blob),
            },
        )
        session.add(row)
        session.flush()

        self._insert_members(session, row.id, members, emit)

        elapsed = time.perf_counter() - started
        emit(
            "load_swissprot_release.done",
            f"{p.release}: {len(members)} accessions in {elapsed:.1f}s",
            {"release_id": str(row.id), "accessions": len(members)},
            "info",
        )
        return OperationResult(
            result={
                "release_id": str(row.id),
                "release": p.release,
                "accessions": len(members),
                "content_sha256": digest,
                "elapsed_seconds": round(elapsed, 3),
            }
        )

    def _already_loaded(
        self,
        existing: SwissProtRelease,
        digest: str,
        parsed: int,
        emit: EmitFn,
    ) -> OperationResult:
        if existing.content_sha256 != digest:
            raise ValueError(
                f"release {existing.release} is already loaded with digest "
                f"{existing.content_sha256} but this run resolves to {digest}; "
                "refusing to replace it. Rows already scored against the stored "
                "membership would change meaning."
            )
        emit(
            "load_swissprot_release.noop",
            f"{existing.release} already loaded, digest matches",
            {"release_id": str(existing.id), "accessions": existing.accession_count},
            "info",
        )
        return OperationResult(
            result={
                "release_id": str(existing.id),
                "release": existing.release,
                "accessions": existing.accession_count,
                "content_sha256": existing.content_sha256,
                "already_loaded": True,
                "parsed": parsed,
            }
        )

    def _read(
        self, p: LoadSwissProtReleasePayload, url: str, emit: EmitFn
    ) -> dict[str, str]:
        if p.source == "local_table":
            if not p.local_path:
                raise ValueError("source 'local_table' requires local_path")
            members = read_local_table(p.local_path)
            emit(
                "load_swissprot_release.read_local",
                None,
                {"path": p.local_path, "accessions": len(members)},
                "info",
            )
            return members
        return self._stream_archive(url, p.timeout_seconds, emit)

    def _stream_archive(self, url: str, timeout: int, emit: EmitFn) -> dict[str, str]:
        """Stream the release tarball and parse only its canonical FASTA member.

        The bundle is read with ``r|gz`` so it is never held on disk: members
        arrive in order and only the current one is readable, which is enough
        because exactly one member is wanted.
        """
        emit("load_swissprot_release.download_start", None, {"url": url}, "info")
        with requests.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()
            with tarfile.open(fileobj=response.raw, mode="r|gz") as archive:
                for member in archive:
                    if not member.isfile():
                        continue
                    if member.name.rsplit("/", 1)[-1] != _FASTA_MEMBER:
                        continue
                    handle = archive.extractfile(member)
                    if handle is None:  # pragma: no cover - tar says file, gives none
                        raise ValueError(f"{member.name} is not readable in {url}")
                    emit(
                        "load_swissprot_release.member",
                        None,
                        {"member": member.name, "size": member.size},
                        "info",
                    )
                    with gzip.GzipFile(fileobj=handle) as fasta:
                        return parse_swissprot_fasta(fasta)
        raise ValueError(f"{_FASTA_MEMBER} not found in {url}")

    def _insert_members(
        self,
        session: Session,
        release_id: Any,
        members: dict[str, str],
        emit: EmitFn,
    ) -> None:
        rows = [
            {"release_id": release_id, "accession": acc, "sequence_md5": members[acc]}
            for acc in sorted(members)
        ]
        table = SwissProtMembership.__table__
        for start in range(0, len(rows), _INSERT_CHUNK):
            session.execute(insert(table), rows[start : start + _INSERT_CHUNK])
            emit(
                "load_swissprot_release.progress",
                None,
                {"written": min(start + _INSERT_CHUNK, len(rows)), "total": len(rows)},
                "debug",
            )
