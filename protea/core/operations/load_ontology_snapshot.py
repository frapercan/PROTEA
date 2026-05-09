from __future__ import annotations

import re
import time
from typing import Any

import requests
from pydantic import field_validator
from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.operations._load_ontology_helpers import (
    handle_existing_snapshot,
    insert_new_snapshot,
)
from protea.infrastructure.orm.models.annotation.ontology_snapshot import OntologySnapshot


def _flush_term(
    current: dict[str, Any],
    terms: list[dict[str, Any]],
    aspect_map: dict[str, str],
) -> None:
    """Append the in-progress OBO term to ``terms`` and reset ``current``.

    Pulled out of ``LoadOntologySnapshotOperation._parse_terms`` to keep
    that method under the §3 60-LOC ceiling. Caller passes the class's
    ``_ASPECT_MAP`` explicitly so this helper has no implicit ``self``
    dependency.
    """
    if "go_id" in current:
        terms.append(
            {
                "go_id": current["go_id"],
                "name": current.get("name"),
                "aspect": aspect_map.get(current.get("namespace", ""), None),
                "definition": current.get("definition"),
                "is_obsolete": current.get("is_obsolete", False),
                "relationships": current.get("relationships", []),
            }
        )
    current.clear()


class LoadOntologySnapshotPayload(ProteaPayload, frozen=True):
    obo_url: str
    timeout_seconds: int = 120
    force_relationships: bool = False

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
    description = (
        "Download a GO OBO file and persist it as an OntologySnapshot with its "
        "GOTerm + GOTermRelationship rows; idempotent on the OBO data-version."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        url = (payload or {}).get("obo_url", "")
        if not url:
            return ""
        # The URL usually looks like .../<YYYY-MM-DD>/ontology/<file>.obo
        marker = url.rsplit("/", 4)
        if len(marker) >= 4 and marker[-1].endswith(".obo"):
            return f"{marker[-3]} · {marker[-1]}"
        return url

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = LoadOntologySnapshotPayload.model_validate(payload)

        emit("load_ontology_snapshot.start", None, {"obo_url": p.obo_url}, "info")
        t0 = time.perf_counter()

        obo_text = self._download(p, emit)
        obo_version = self._extract_version(obo_text)
        emit("load_ontology_snapshot.version", None, {"obo_version": obo_version}, "info")

        terms = self._parse_terms(obo_text)
        emit("load_ontology_snapshot.parsed", None, {"term_count": len(terms)}, "info")

        existing = session.query(OntologySnapshot).filter_by(obo_version=obo_version).first()
        if existing is not None:
            return handle_existing_snapshot(session, existing, terms, obo_version, emit)

        return insert_new_snapshot(session, p.obo_url, obo_version, terms, t0, emit)

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
        in_term = False
        for raw in obo_text.splitlines():
            line = raw.strip()
            if line == "[Term]":
                _flush_term(current, terms, self._ASPECT_MAP)
                in_term = True
                continue
            if line.startswith("[") and line != "[Term]":
                _flush_term(current, terms, self._ASPECT_MAP)
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

        _flush_term(current, terms, self._ASPECT_MAP)
        return terms
