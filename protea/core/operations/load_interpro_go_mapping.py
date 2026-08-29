"""IP.4a — InterPro2GO mapping loader.

Streams the EBI ``interpro2go`` flat file (one line per InterPro entry
to GO term mapping) and upserts rows into ``interpro_go_mapping``.
The file is small enough (few thousand lines, ~1 MB) to hold in
memory, but the operation still parses lazily so a partial read is
graceful.

Idempotent: the unique key
``(ipr_accession, go_id, source_version)`` is enforced by an
``ON CONFLICT DO NOTHING`` upsert, so re-running the operation for
the same release is a no-op.

InterPro2GO line format (text, comment-prefixed with ``!``):

    InterPro:IPR000001 Kringle > GO:protein binding ; GO:0005515

The pattern is:

    InterPro:<IPR_ACCESSION> <human_label> > GO:<label> ; <GO_ID>

Evidence is implicit (curator-asserted IEA in the consuming pipeline)
so the ``evidence`` column captures the literal upstream label
(``"IEA"`` by default) without inventing extra metadata.
"""

from __future__ import annotations

import re
import time
from collections.abc import Iterator
from typing import Annotated, Any

import requests
from pydantic import Field, field_validator
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.utils import contract_payload
from protea.infrastructure.orm.models.annotation.interpro_go_mapping import (
    InterProGoMapping,
)

PositiveInt = Annotated[int, Field(gt=0)]

_DEFAULT_MAPPING_URL = "https://ftp.ebi.ac.uk/pub/databases/GO/goa/external2go/interpro2go"
_DEFAULT_EVIDENCE = "IEA"

# InterPro2GO line:  InterPro:IPR000001 <label> > GO:<label> ; GO:0005515
_LINE_RE = re.compile(r"^InterPro:(?P<ipr>IPR\d{6,7})\s+.*>\s*GO:.*;\s*(?P<go>GO:\d{7})\s*$")


class LoadInterProGoMappingPayload(ProteaPayload, frozen=True):
    """Payload for ``load_interpro_go_mapping``.

    ``mapping_url`` defaults to the canonical EBI endpoint so callers
    can dispatch the op without a payload body in the common case.
    ``source_version`` is required so the snapshot is queryable later;
    pass the InterPro release tag (e.g. ``"InterPro-104.0"``) here.
    """

    source_version: str
    mapping_url: str = _DEFAULT_MAPPING_URL
    evidence: str = _DEFAULT_EVIDENCE
    timeout_seconds: PositiveInt = 120
    chunk_size: PositiveInt = 1000

    @field_validator("source_version", "mapping_url", "evidence", mode="before")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


class LoadInterProGoMappingOperation:
    """Stream the EBI InterPro2GO file and upsert mapping rows."""

    name = "load_interpro_go_mapping"
    description = (
        "Download the EBI InterPro2GO flat file and upsert (ipr_accession, "
        "go_id) rows into interpro_go_mapping; idempotent per source_version."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        p = payload or {}
        version = p.get("source_version")
        url = p.get("mapping_url", "")
        filename = url.rsplit("/", 1)[-1] if url else ""
        bits = []
        if version:
            bits.append(f"release={version}")
        if filename:
            bits.append(filename)
        return " · ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = LoadInterProGoMappingPayload.model_validate(contract_payload(payload))

        t0 = time.perf_counter()
        emit(
            "load_interpro_go_mapping.start",
            None,
            {"mapping_url": p.mapping_url, "source_version": p.source_version},
            "info",
        )

        text = self._download(p, emit)
        rows = list(self._parse(text, p))

        if not rows:
            emit("load_interpro_go_mapping.no_rows_parsed", None, {}, "warning")
            return OperationResult(
                result={
                    "rows_parsed": 0,
                    "rows_inserted": 0,
                    "source_version": p.source_version,
                    "elapsed_seconds": time.perf_counter() - t0,
                }
            )

        inserted = self._upsert(session, rows, p.chunk_size)
        result: dict[str, Any] = {
            "source_version": p.source_version,
            "rows_parsed": len(rows),
            "rows_inserted": inserted,
            "rows_skipped_duplicate": len(rows) - inserted,
            "elapsed_seconds": time.perf_counter() - t0,
        }
        emit("load_interpro_go_mapping.done", None, result, "info")
        return OperationResult(result=result)

    def _download(self, p: LoadInterProGoMappingPayload, emit: EmitFn) -> str:
        emit(
            "load_interpro_go_mapping.download_start",
            None,
            {"url": p.mapping_url},
            "info",
        )
        resp = requests.get(p.mapping_url, timeout=p.timeout_seconds)
        resp.raise_for_status()
        text = resp.text
        emit(
            "load_interpro_go_mapping.download_done",
            None,
            {"bytes": len(text)},
            "info",
        )
        return text

    def _parse(self, text: str, p: LoadInterProGoMappingPayload) -> Iterator[dict[str, Any]]:
        """Yield one row dict per InterPro2GO line; deduplicate within file.

        Lines starting with ``!`` are comments. Malformed lines are
        silently skipped. Dedup is on ``(ipr, go_id)`` within this call
        because the source occasionally repeats the same pair.
        """
        seen: set[tuple[str, str]] = set()
        for raw in text.splitlines():
            line = raw.strip()
            if not line or line.startswith("!"):
                continue
            m = _LINE_RE.match(line)
            if m is None:
                continue
            ipr = m.group("ipr")
            go = m.group("go")
            key = (ipr, go)
            if key in seen:
                continue
            seen.add(key)
            yield {
                "ipr_accession": ipr,
                "go_id": go,
                "evidence": p.evidence,
                "source_version": p.source_version,
            }

    def _upsert(self, session: Session, rows: list[dict[str, Any]], chunk_size: int) -> int:
        """Bulk-upsert via ON CONFLICT DO NOTHING; returns inserted count.

        Postgres' ``RETURNING`` on a do-nothing upsert returns only the
        actually-inserted rows, so we get an accurate inserted total
        even when re-running the loader for an already-loaded release.
        """
        inserted_total = 0
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            stmt = (
                pg_insert(InterProGoMapping.__table__)
                .values(chunk)
                .on_conflict_do_nothing(constraint="uq_interpro_go_mapping_pair")
                .returning(InterProGoMapping.__table__.c.id)
            )
            result = session.execute(stmt)
            inserted_total += len(result.fetchall())
        return inserted_total
