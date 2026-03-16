from __future__ import annotations

import re
import time
from typing import Any

import requests
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.infrastructure.orm.models.annotation.go_term import GOTerm
from protea.infrastructure.orm.models.annotation.go_term_relationship import GOTermRelationship
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot


class LoadOntologySnapshotPayload(ProteaPayload, frozen=True):
    obo_url: str
    timeout_seconds: int = 120
    force_relationships: bool = False

    from pydantic import field_validator

    @field_validator("obo_url", mode="before")
    @classmethod
    def must_be_non_empty(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("obo_url must be a non-empty string")
        return v.strip()


class LoadOntologySnapshotOperation:
    """Downloads a go.obo file and upserts an OntologySnapshot + GOTerm rows.

    The ``data-version:`` header of the OBO file is used as the canonical
    version identifier (e.g. ``releases/2024-01-17``). If a snapshot with that
    version already exists, the operation is a no-op and returns the existing
    snapshot id — making it safe to re-run.

    GO term aspect is mapped from the OBO ``namespace`` field:
    ``biological_process`` → P, ``molecular_function`` → F,
    ``cellular_component`` → C.
    """

    name = "load_ontology_snapshot"

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = LoadOntologySnapshotPayload.model_validate(payload)

        emit("load_ontology_snapshot.start", None, {"obo_url": p.obo_url}, "info")
        t0 = time.perf_counter()

        obo_text = self._download(p, emit)
        obo_version = self._extract_version(obo_text)

        emit("load_ontology_snapshot.version", None, {"obo_version": obo_version}, "info")

        # Idempotency: if snapshot already loaded, check if relationships are missing
        existing = session.query(OntologySnapshot).filter_by(obo_version=obo_version).first()
        if existing is not None:
            from sqlalchemy import func as _func

            rel_count = (
                session.query(_func.count(GOTermRelationship.id))
                .filter(GOTermRelationship.ontology_snapshot_id == existing.id)
                .scalar()
                or 0
            )

            if rel_count > 0:
                emit(
                    "load_ontology_snapshot.already_exists",
                    None,
                    {"ontology_snapshot_id": str(existing.id), "obo_version": obo_version},
                    "info",
                )
                return OperationResult(
                    result={
                        "ontology_snapshot_id": str(existing.id),
                        "obo_version": obo_version,
                        "skipped": True,
                    }
                )

            # Snapshot exists but has no relationships — backfill them
            emit(
                "load_ontology_snapshot.backfill_relationships",
                None,
                {"ontology_snapshot_id": str(existing.id)},
                "info",
            )
            terms = self._parse_terms(obo_text)
            go_id_to_db_id = {
                go_id: db_id
                for go_id, db_id in session.query(GOTerm.go_id, GOTerm.id)
                .filter(GOTerm.ontology_snapshot_id == existing.id)
                .all()
            }
            relationships: list[GOTermRelationship] = []
            for t in terms:
                child_db_id = go_id_to_db_id.get(t["go_id"])
                if child_db_id is None:
                    continue
                for rel_type, parent_go_id in t.get("relationships", []):
                    parent_db_id = go_id_to_db_id.get(parent_go_id)
                    if parent_db_id is None:
                        continue
                    relationships.append(
                        GOTermRelationship(
                            child_go_term_id=child_db_id,
                            parent_go_term_id=parent_db_id,
                            relation_type=rel_type,
                            ontology_snapshot_id=existing.id,
                        )
                    )
            session.add_all(relationships)
            session.flush()
            emit(
                "load_ontology_snapshot.backfill_done",
                None,
                {"relationships_inserted": len(relationships)},
                "info",
            )
            return OperationResult(
                result={
                    "ontology_snapshot_id": str(existing.id),
                    "obo_version": obo_version,
                    "skipped": False,
                    "relationships_inserted": len(relationships),
                }
            )

        snapshot = OntologySnapshot(obo_url=p.obo_url, obo_version=obo_version)
        session.add(snapshot)
        session.flush()

        terms = self._parse_terms(obo_text)
        emit("load_ontology_snapshot.parsed", None, {"term_count": len(terms)}, "info")

        go_terms = [
            GOTerm(
                go_id=t["go_id"],
                name=t["name"],
                aspect=t["aspect"],
                definition=t["definition"],
                is_obsolete=t["is_obsolete"],
                ontology_snapshot_id=snapshot.id,
            )
            for t in terms
        ]
        session.add_all(go_terms)
        session.flush()

        # Build go_id → db id map for relationship insertion
        go_id_to_db_id = {gt.go_id: gt.id for gt in go_terms}

        relationships: list[GOTermRelationship] = []
        for t in terms:
            child_db_id = go_id_to_db_id.get(t["go_id"])
            if child_db_id is None:
                continue
            for rel_type, parent_go_id in t.get("relationships", []):
                parent_db_id = go_id_to_db_id.get(parent_go_id)
                if parent_db_id is None:
                    continue
                relationships.append(
                    GOTermRelationship(
                        child_go_term_id=child_db_id,
                        parent_go_term_id=parent_db_id,
                        relation_type=rel_type,
                        ontology_snapshot_id=snapshot.id,
                    )
                )
        session.add_all(relationships)
        session.flush()

        elapsed = time.perf_counter() - t0
        emit(
            "load_ontology_snapshot.done",
            None,
            {
                "ontology_snapshot_id": str(snapshot.id),
                "obo_version": obo_version,
                "terms_inserted": len(go_terms),
                "relationships_inserted": len(relationships),
                "elapsed_seconds": elapsed,
            },
            "info",
        )
        return OperationResult(
            result={
                "ontology_snapshot_id": str(snapshot.id),
                "obo_version": obo_version,
                "terms_inserted": len(go_terms),
                "relationships_inserted": len(relationships),
                "elapsed_seconds": elapsed,
            }
        )

    def _download(self, p: LoadOntologySnapshotPayload, emit: EmitFn) -> str:
        emit("load_ontology_snapshot.download_start", None, {"url": p.obo_url}, "info")
        resp = requests.get(p.obo_url, timeout=p.timeout_seconds, stream=True)
        resp.raise_for_status()
        text = resp.text
        emit("load_ontology_snapshot.download_done", None, {"bytes": len(text)}, "info")
        return text

    def _extract_version(self, obo_text: str) -> str:
        for line in obo_text.splitlines():
            if line.startswith("data-version:"):
                return line.split(":", 1)[1].strip()
        raise ValueError("go.obo has no data-version header")

    _ASPECT_MAP = {
        "biological_process": "P",
        "molecular_function": "F",
        "cellular_component": "C",
    }

    # Relationship types to capture from OBO `relationship:` lines
    _RELATION_TYPES = {
        "part_of",
        "regulates",
        "negatively_regulates",
        "positively_regulates",
        "occurs_in",
        "capable_of",
        "capable_of_part_of",
    }

    def _parse_terms(self, obo_text: str) -> list[dict[str, Any]]:
        terms: list[dict[str, Any]] = []
        current: dict[str, Any] = {}

        def flush() -> None:
            if "go_id" in current:
                terms.append(
                    {
                        "go_id": current["go_id"],
                        "name": current.get("name"),
                        "aspect": self._ASPECT_MAP.get(current.get("namespace", ""), None),
                        "definition": current.get("definition"),
                        "is_obsolete": current.get("is_obsolete", False),
                        "relationships": current.get("relationships", []),
                    }
                )
            current.clear()

        in_term = False
        for raw in obo_text.splitlines():
            line = raw.strip()
            if line == "[Term]":
                flush()
                in_term = True
                continue
            if line.startswith("[") and line != "[Term]":
                flush()
                in_term = False
                continue
            if not in_term or not line or line.startswith("!"):
                continue

            if line.startswith("id: GO:"):
                current["go_id"] = line.split(None, 1)[1].strip()
            elif line.startswith("name:"):
                current["name"] = line[5:].strip()
            elif line.startswith("namespace:"):
                current["namespace"] = line.split(None, 1)[1].strip()
            elif line.startswith("def:"):
                m = re.match(r'def:\s*"(.*?)"', line)
                current["definition"] = m.group(1) if m else None
            elif line == "is_obsolete: true":
                current["is_obsolete"] = True
            elif line.startswith("is_a: GO:"):
                # is_a: GO:XXXXXXX ! label
                parent_go_id = line.split(None, 1)[1].split("!")[0].strip()
                current.setdefault("relationships", []).append(("is_a", parent_go_id))
            elif line.startswith("relationship:"):
                # relationship: part_of GO:XXXXXXX ! label
                parts = line[len("relationship:") :].strip().split()
                if (
                    len(parts) >= 2
                    and parts[0] in self._RELATION_TYPES
                    and parts[1].startswith("GO:")
                ):
                    current.setdefault("relationships", []).append((parts[0], parts[1]))

        flush()
        return terms
