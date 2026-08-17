"""Archive the raw OBO behind an OntologySnapshot into the artifact store.

``load_ontology_snapshot`` downloads an OBO, parses it into ``GOTerm`` and
``GOTermRelationship`` rows, and keeps only ``obo_url``. The bytes are never
written anywhere. ``run_cafa_evaluation`` therefore re-fetches the file from the
upstream URL on every run, which means the ontology behind a published score is
a link to a third party rather than a record we hold.

That matters beyond tidiness. The term universe, the True Path Rule edges, the
propagation, the Information Accretion table and every metric derived from them
are all downstream of these bytes. If the upstream file is revised or withdrawn,
past scores become unreproducible and nothing in the system says so.

Archiving after the fact cannot be done blindly: the file at ``obo_url`` today
is not necessarily the file that was parsed when the snapshot was loaded. So the
fetched bytes are gated against the term set already in the database, and a
mismatch raises rather than overwriting the record with a newer ontology under
an older snapshot's identity.
"""

from __future__ import annotations

import gzip
import hashlib
import tempfile
import uuid
from pathlib import Path
from typing import Any, NamedTuple

import requests
from pydantic import field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store


def obo_key_for(snapshot_id: Any) -> str:
    """Storage key under which a snapshot's archived OBO lives."""
    return f"ontology_snapshot/{snapshot_id}/go.obo.gz"


class ArchiveOntologySnapshotPayload(ProteaPayload, frozen=True):
    ontology_snapshot_id: str
    #: Re-archive even when ``obo_uri`` is already set.
    force: bool = False
    #: Share of the database term set that may be missing from the fetched file
    #: before the congruence gate fails. The default demands an exact match.
    max_term_drift_pct: float = 0.0
    timeout_seconds: int = 300

    @field_validator("ontology_snapshot_id", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()


class OntologyDriftError(RuntimeError):
    """The bytes at ``obo_url`` no longer match the loaded snapshot."""


class _Fetched(NamedTuple):
    """What the downloaded OBO says about itself.

    The ids and the version are read in one pass and judged together: a file
    whose version matches but whose terms do not is the same failure as the
    reverse, so passing them separately invites a caller to check one and
    forget the other.
    """

    ids: set[str]
    version: str | None


class ArchiveOntologySnapshotOperation:
    name = "archive_ontology_snapshot"
    description = (
        "Fetch the raw OBO for an OntologySnapshot, verify it still matches the "
        "term set loaded in the database, and archive it in the artifact store "
        "with its sha256."
    )

    @staticmethod
    def _parse_ids_and_version(obo_text: str) -> _Fetched:
        """Extract the live term ids and the data-version from OBO text.

        Deliberately minimal and independent of
        ``LoadOntologySnapshotOperation._parse_terms``: this is a congruence
        check, so it should not inherit that parser's interpretation of the
        file. Obsolete terms are excluded to match the database side of the
        comparison.
        """
        ids: set[str] = set()
        version: str | None = None
        current: str | None = None
        obsolete = False
        in_term = False
        for raw in obo_text.splitlines():
            line = raw.strip()
            if version is None and line.startswith("data-version:"):
                version = line.split(":", 1)[1].strip()
            if line == "[Term]":
                if in_term and current and not obsolete:
                    ids.add(current)
                in_term, current, obsolete = True, None, False
                continue
            if line.startswith("[") and line.endswith("]"):
                if in_term and current and not obsolete:
                    ids.add(current)
                in_term, current, obsolete = False, None, False
                continue
            if not in_term:
                continue
            if line.startswith("id:"):
                current = line.split(":", 1)[1].strip()
            elif line.startswith("is_obsolete:"):
                obsolete = line.split(":", 1)[1].strip() == "true"
        if in_term and current and not obsolete:
            ids.add(current)
        return ids, version

    def _gate_congruence(
        self,
        session: Session,
        snapshot: OntologySnapshot,
        fetched: _Fetched,
        max_drift_pct: float,
        emit: EmitFn,
    ) -> dict[str, Any]:
        if fetched.version is not None and fetched.version != snapshot.obo_version:
            raise OntologyDriftError(
                f"fetched OBO declares data-version {fetched.version!r} but the "
                f"snapshot was loaded as {snapshot.obo_version!r}; the upstream "
                f"URL now serves a different release"
            )

        db_ids = {
            r[0]
            for r in session.execute(
                text(
                    "select go_id from go_term "
                    "where ontology_snapshot_id = :s and is_obsolete = false"
                ),
                {"s": str(snapshot.id)},
            )
        }
        missing = db_ids - fetched.ids
        added = fetched.ids - db_ids
        drift_pct = 100.0 * len(missing) / len(db_ids) if db_ids else 0.0
        stats = {
            "db_terms": len(db_ids),
            "fetched_terms": len(fetched.ids),
            "missing_from_fetch": len(missing),
            "added_by_fetch": len(added),
            "drift_pct": drift_pct,
        }
        if drift_pct > max_drift_pct:
            raise OntologyDriftError(
                f"{len(missing):,} of {len(db_ids):,} loaded terms "
                f"({drift_pct:.4f} percent) are absent from the file now served "
                f"at {snapshot.obo_url}; archiving it would record a different "
                f"ontology under this snapshot's identity. Sample: "
                f"{sorted(missing)[:5]}"
            )
        emit("archive_ontology_snapshot.congruence_ok", None, stats, "info")
        return stats

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        """One line naming the snapshot being archived.

        The drift tolerance is included when it is not the default, since a
        run that accepted drift and one that refused it are different acts on
        the same snapshot and the history should not show them alike.
        """
        p = payload or {}
        snapshot = str(p.get("ontology_snapshot_id") or "")[:8]
        bits = [f"snapshot={snapshot}"] if snapshot else []
        drift = p.get("max_term_drift_pct")
        if drift not in (None, 0.0):
            bits.append(f"drift<={drift}%")
        if p.get("force"):
            bits.append("force")
        return " ".join(bits)

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = ArchiveOntologySnapshotPayload.model_validate(payload)
        snapshot = session.get(OntologySnapshot, uuid.UUID(p.ontology_snapshot_id))
        if snapshot is None:
            raise ValueError(f"OntologySnapshot {p.ontology_snapshot_id} not found")

        if snapshot.obo_uri and not p.force:
            emit(
                "archive_ontology_snapshot.reused",
                "already archived",
                {"obo_uri": snapshot.obo_uri, "obo_sha256": snapshot.obo_sha256},
                "info",
            )
            return OperationResult(
                result={
                    "ontology_snapshot_id": str(snapshot.id),
                    "obo_uri": snapshot.obo_uri,
                    "obo_sha256": snapshot.obo_sha256,
                    "reused": True,
                }
            )

        emit(
            "archive_ontology_snapshot.download_start",
            None,
            {"url": snapshot.obo_url, "obo_version": snapshot.obo_version},
            "info",
        )
        resp = requests.get(snapshot.obo_url, timeout=p.timeout_seconds)
        resp.raise_for_status()
        obo_bytes = resp.content
        digest = hashlib.sha256(obo_bytes).hexdigest()
        emit(
            "archive_ontology_snapshot.download_done",
            None,
            {"bytes": len(obo_bytes), "sha256": digest},
            "info",
        )

        fetched = self._parse_ids_and_version(
            obo_bytes.decode("utf-8", errors="replace")
        )
        stats = self._gate_congruence(
            session, snapshot, fetched, p.max_term_drift_pct, emit
        )

        project_root = Path(__file__).resolve().parents[3]
        store = get_artifact_store(load_settings(project_root))
        key = obo_key_for(snapshot.id)
        with tempfile.TemporaryDirectory(prefix="protea_obo_") as tmp:
            local = Path(tmp) / "go.obo.gz"
            with gzip.open(local, "wb") as fh:
                fh.write(obo_bytes)
            uri = store.put(key, local)
            compressed = local.stat().st_size

        snapshot.obo_uri = uri
        snapshot.obo_sha256 = digest
        session.flush()

        emit(
            "archive_ontology_snapshot.persisted",
            None,
            {"obo_uri": uri, "key": key, "sha256": digest,
             "raw_bytes": len(obo_bytes), "stored_bytes": compressed, **stats},
            "info",
        )
        return OperationResult(
            result={
                "ontology_snapshot_id": str(snapshot.id),
                "obo_version": snapshot.obo_version,
                "obo_uri": uri,
                "obo_sha256": digest,
                "raw_bytes": len(obo_bytes),
                "stored_bytes": compressed,
                "reused": False,
                **stats,
            }
        )
