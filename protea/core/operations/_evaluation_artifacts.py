"""Resolving an evaluation's input artifacts to files on disk.

Three resolvers, lifted out of ``RunCafaEvaluationOperation`` unchanged.
They were static methods on a class they did not use, called from two
operations, and they are the reason that file sat 101 lines over its
budget and its class 127 over: a resolver is not part of running an
evaluation, it is what has to exist before one can run.

This is a pure move. The bodies are byte-identical apart from losing one
level of indentation and their decorators, and the only reason it happens
in this branch rather than its own is that the smell ratchet refuses a
file already over budget, correctly, and the change it was refusing was
two lines long.
"""

from __future__ import annotations

import hashlib
import os
import uuid

from sqlalchemy.orm import Session

from protea.core.contracts.operation import EmitFn
from protea.core.operations import _run_cafa_artifacts as _artifacts
from protea.infrastructure.orm.models.annotation.ontology_snapshot import (
    OntologySnapshot,
)


def resolve_obo(tmpdir: str, snapshot: OntologySnapshot, emit: EmitFn) -> str:
    """Resolve the OBO, preferring the archived copy over the upstream URL.

    ``obo_url`` points at a third party that can revise or withdraw the file,
    so a run that refetches it is only reproducible for as long as EBI keeps
    serving the same bytes. When ``archive_ontology_snapshot`` has run, the
    bytes come from the artifact store instead and the recorded sha256 is
    verified. The fallback is kept because the snapshots loaded before
    ADR-D47 have no archive.
    """
    obo_path = os.path.join(tmpdir, "go.obo")
    if snapshot.obo_uri:
        emit(
            "run_cafa_evaluation.downloading_obo",
            None,
            {"uri": snapshot.obo_uri, "source": "artifact_store"},
            "info",
        )
        _artifacts.download_tsv(snapshot.obo_uri, obo_path)
        if snapshot.obo_sha256:
            with open(obo_path, "rb") as fh:
                digest = hashlib.sha256(fh.read()).hexdigest()
            if digest != snapshot.obo_sha256:
                raise ValueError(
                    f"archived OBO for snapshot {snapshot.id} hashes to "
                    f"{digest}, but the row records {snapshot.obo_sha256}; "
                    f"the stored artifact has drifted"
                )
        return obo_path
    emit(
        "run_cafa_evaluation.downloading_obo",
        None,
        {"url": snapshot.obo_url, "source": "upstream",
         "warning": "snapshot has no archived OBO; this run depends on the "
                    "upstream URL still serving the same bytes"},
        "warning",
    )
    _artifacts.download_obo(snapshot.obo_url, obo_path)
    return obo_path


def resolve_ia_from_set(
    tmpdir: str,
    snapshot: OntologySnapshot,
    session: Session | None,
    ia_set_id: str,
    emit: EmitFn,
) -> str:
    """Fetch an ``InformationAccretionSet`` artifact and verify its hash.

    This is the traceable route. The row pins the three axes IA depends on
    (snapshot, corpus, evidence regime), the bytes come from the shared
    object store so both machines resolve the same file, and the recorded
    sha256 is checked after download. The path-based routes record none of
    that, which is the failure ``docs/IA_PROVENANCE_v227.md`` documents.
    """
    from protea.infrastructure.orm.models.annotation.information_accretion_set import (  # noqa: E501
        InformationAccretionSet,
    )

    if session is None:
        raise ValueError("information_accretion_set_id requires a database session")
    ia_set = session.get(InformationAccretionSet, uuid.UUID(ia_set_id))
    if ia_set is None:
        raise ValueError(f"InformationAccretionSet {ia_set_id} not found")
    if ia_set.ontology_snapshot_id != snapshot.id:
        raise ValueError(
            f"InformationAccretionSet {ia_set_id} was computed on ontology "
            f"snapshot {ia_set.ontology_snapshot_id}, but this evaluation "
            f"propagates under {snapshot.id}; the term universes differ"
        )
    ia_path = os.path.join(tmpdir, "ia.tsv")
    emit(
        "run_cafa_evaluation.downloading_ia",
        None,
        {"information_accretion_set_id": ia_set_id, "uri": ia_set.artifact_uri},
        "info",
    )
    _artifacts.download_tsv(ia_set.artifact_uri, ia_path)
    with open(ia_path, "rb") as fh:
        digest = hashlib.sha256(fh.read()).hexdigest()
    if digest != ia_set.content_sha256:
        raise ValueError(
            f"InformationAccretionSet {ia_set_id} content hash mismatch: "
            f"row records {ia_set.content_sha256}, fetched bytes hash to "
            f"{digest}; the stored artifact has drifted"
        )
    emit(
        "run_cafa_evaluation.ia_resolved",
        None,
        {
            "ia_path": ia_path,
            "information_accretion_set_id": ia_set_id,
            "evidence_regime": ia_set.evidence_regime,
            "annotation_set_id": str(ia_set.annotation_set_id),
            "content_sha256": digest,
        },
        "info",
    )
    return ia_path


def resolve_ia_file(
    tmpdir: str,
    snapshot: OntologySnapshot,
    payload_ia_file: str | None,
    emit: EmitFn,
    session: Session | None = None,
    ia_set_id: str | None = None,
) -> str | None:
    """Resolve the Information Accretion file for cafaeval.

    Priority: ``information_accretion_set_id`` > explicit ``payload.ia_file``
    > snapshot ``ia_url`` (downloaded once) > ``None`` (cafaeval falls back
    to uniform IC=1, with a warning).
    """
    if ia_set_id and payload_ia_file:
        raise ValueError(
            "pass either information_accretion_set_id or ia_file, not both: "
            "they would silently disagree about which table was scored"
        )
    if ia_set_id:
        return resolve_ia_from_set(
            tmpdir, snapshot, session, ia_set_id, emit
        )

    ia_path: str | None = payload_ia_file
    if ia_path is None and snapshot.ia_url:
        ia_path = os.path.join(tmpdir, "ia.tsv")
        emit("run_cafa_evaluation.downloading_ia", None, {"url": snapshot.ia_url}, "info")
        _artifacts.download_tsv(snapshot.ia_url, ia_path)
    if ia_path:
        emit("run_cafa_evaluation.ia_resolved", None, {"ia_path": ia_path}, "info")
        return ia_path
    emit(
        "run_cafa_evaluation.ia_missing",
        None,
        {
            "warning": "No IA file available; cafaeval will use uniform IC=1 for all "
            "GO terms. Set ia_url on the OntologySnapshot or pass ia_file "
            "in the payload for information-content-weighted metrics.",
        },
        "warning",
    )
    return None



__all__ = ["resolve_obo", "resolve_ia_file", "resolve_ia_from_set"]
