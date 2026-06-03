"""Scrape GOA release publication dates from the EBI FTP listing and
upsert them onto ``annotation_set.source_published_at`` rows where the
source is ``goa`` and the integer ``source_version`` matches.

This is a small HTTP+SQL operation: ``requests.get`` the listing,
regex out ``(version_int, datetime_utc)`` pairs, then ``UPDATE`` each
matching AnnotationSet. New AnnotationSet rows are never created; the
operation is fully idempotent (overwrites are accepted by design so
re-runs eventually correct any upstream timestamp revision).
"""

from __future__ import annotations

import re
import time
from datetime import UTC, datetime
from typing import Annotated, Any

import requests
from pydantic import Field, field_validator
from sqlalchemy import update
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet

DEFAULT_LISTING_URL = (
    "https://ftp.ebi.ac.uk/pub/databases/GO/goa/old/UNIPROT/"
)

# Match the file column and the date column in the same row.  The EBI
# Apache index uses one ``<a>`` per file followed by an aligned ``<td>``
# for the timestamp.  We match a relaxed window between them so trivial
# markup tweaks do not silently break parsing.
_LISTING_RE = re.compile(
    r'href="goa_uniprot_all\.gaf\.(?P<version>\d+)\.gz"'
    r".*?(?P<date>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})",
    re.DOTALL,
)

PositiveInt = Annotated[int, Field(gt=0)]


class RefreshGoaReleaseDatesPayload(ProteaPayload, frozen=True):
    listing_url: str = DEFAULT_LISTING_URL
    timeout_seconds: PositiveInt = 60

    @field_validator("listing_url", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("listing_url must be a non-empty string")
        return v.strip()


def _parse_listing(html: str) -> list[tuple[int, datetime]]:
    """Return ``[(version_int, datetime_utc), ...]`` from the EBI FTP HTML.

    Lines that fail to parse a valid date are silently dropped: the EBI
    server occasionally emits truncated rows on partial sync and we
    prefer a partial update to a hard failure.
    """
    out: list[tuple[int, datetime]] = []
    seen: set[int] = set()
    for m in _LISTING_RE.finditer(html):
        try:
            version = int(m.group("version"))
        except (TypeError, ValueError):
            continue
        if version in seen:
            continue
        try:
            dt = datetime.strptime(m.group("date"), "%Y-%m-%d %H:%M").replace(
                tzinfo=UTC
            )
        except ValueError:
            continue
        seen.add(version)
        out.append((version, dt))
    return out


class RefreshGoaReleaseDatesOperation:
    """Refresh ``source_published_at`` on existing ``goa`` AnnotationSets.

    Reads the EBI old-UNIPROT listing, extracts ``(version, published_at)``
    for every ``goa_uniprot_all.gaf.<N>.gz`` row, and ``UPDATE``s the
    matching AnnotationSet rows. Rows whose integer version is absent
    from the listing are left untouched (still NULL or whatever the
    prior value was). Rows present in the listing but not in the DB
    are silently skipped: this operation never inserts.
    """

    name = "refresh_goa_release_dates"
    description = (
        "Scrape EBI FTP for GOA release publication dates and upsert "
        "``source_published_at`` onto matching ``goa`` annotation sets."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        url = (payload or {}).get("listing_url", DEFAULT_LISTING_URL)
        host = url.split("/")[2] if "//" in url else url
        return f"listing={host}"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = RefreshGoaReleaseDatesPayload.model_validate(payload or {})
        t0 = time.perf_counter()

        emit(
            "refresh_goa_release_dates.start",
            None,
            {"listing_url": p.listing_url},
            "info",
        )

        html = self._fetch(p, emit)
        releases = _parse_listing(html)
        emit(
            "refresh_goa_release_dates.parsed",
            None,
            {"parsed_releases": len(releases)},
            "info",
        )

        matched, updated = self._apply(session, releases, emit)

        result = {
            "parsed_releases": len(releases),
            "matched_annotation_sets": matched,
            "updated": updated,
            "elapsed_seconds": time.perf_counter() - t0,
        }
        emit("refresh_goa_release_dates.done", None, result, "info")
        return OperationResult(result=result)

    def _fetch(
        self, p: RefreshGoaReleaseDatesPayload, emit: EmitFn
    ) -> str:
        emit(
            "refresh_goa_release_dates.fetch_start",
            None,
            {"listing_url": p.listing_url},
            "info",
        )
        resp = requests.get(p.listing_url, timeout=p.timeout_seconds)
        resp.raise_for_status()
        html = resp.text
        emit(
            "refresh_goa_release_dates.fetch_done",
            None,
            {"bytes": len(html)},
            "info",
        )
        return html

    def _apply(
        self,
        session: Session,
        releases: list[tuple[int, datetime]],
        emit: EmitFn,
    ) -> tuple[int, int]:
        """UPDATE matching ``goa`` AnnotationSets, return (matched, updated).

        ``matched`` counts AnnotationSet rows whose integer version
        appeared in the listing (i.e. an UPDATE candidate, regardless
        of whether the value changed).  ``updated`` counts rows whose
        ``source_published_at`` actually moved (NULL or stale → new).
        """
        if not releases:
            return (0, 0)
        rows = (
            session.query(AnnotationSet)
            .filter(AnnotationSet.source == "goa")
            .all()
        )
        by_version: dict[int, datetime] = {v: dt for v, dt in releases}
        matched = 0
        updated = 0
        for row in rows:
            version_int = _coerce_int(row.source_version)
            if version_int is None:
                continue
            published = by_version.get(version_int)
            if published is None:
                continue
            matched += 1
            if row.source_published_at != published:
                session.execute(
                    update(AnnotationSet)
                    .where(AnnotationSet.id == row.id)
                    .values(source_published_at=published)
                )
                updated += 1
        emit(
            "refresh_goa_release_dates.applied",
            None,
            {"matched_annotation_sets": matched, "updated": updated},
            "info",
        )
        return (matched, updated)


def _coerce_int(raw: str | None) -> int | None:
    if raw is None:
        return None
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return None
