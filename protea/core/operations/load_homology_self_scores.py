"""Load the self-alignment bit scores of a homology table's queries, built on the cluster.

The cluster half is :mod:`protea.core.homology.self_scores`. This operation refuses
before writing anything unless:

- ``self-scores.json`` hashes to the sha256 the payload names, and its status is
  ``complete`` (a sampled ``measurement`` is never loaded);
- ``self-scores.parquet`` still hashes to what the manifest recorded, holds one row
  per query it scored with a positive finite bit score, and counts as recorded;
- the photo it names is the loaded full photo of that cut, and that photo has a
  homology table whose queries are the ones these scores were computed for: the
  sha256 of the query bytes the table's prepare pass wrote, and their count;
- every scored sequence is a query of that table: the sequence of a superset member.

Then it puts the scores and the manifest in the artifact store and writes one
``homology_self_scores`` row, derived from its table by a foreign key, with its
ADR-D58 descriptors. A table has one row: the same manifest again is a no-op, and
another one is refused.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from pydantic import field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.code_revision import code_revision
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.homology.self_scores import MANIFEST, OUTPUTS
from protea.core.source_guards import SourceGuardError
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto, CutPhotoMember
from protea.infrastructure.orm.models.annotation.homology_self_scores import HomologySelfScores
from protea.infrastructure.orm.models.annotation.homology_table import HomologyTable
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store


class SelfScoresRefused(SourceGuardError):
    """Self scores the loader will not write, with the reason."""


class LoadHomologySelfScoresPayload(ProteaPayload, frozen=True):
    """Where one cut's scores are and what their manifest hashes to; the rest is read from it."""

    scores_dir: str
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


def read_scores_manifest(scores_dir: Path, expected_sha256: str) -> dict[str, Any]:
    """The verified ``self-scores.json``, refused unless it hashes as named and is complete."""
    path = scores_dir / MANIFEST
    if not path.is_file():
        raise SelfScoresRefused(f"{path} does not exist")
    got = _file_sha256(path)
    if got != expected_sha256:
        raise SelfScoresRefused(f"{MANIFEST} hashes to {got}, not to {expected_sha256}")
    manifest: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("status") != "complete":
        raise SelfScoresRefused(
            f"the scores' status is {manifest.get('status')!r}: only a complete run loads"
        )
    for name in OUTPUTS:
        recorded = manifest.get("outputs", {}).get(name)
        if recorded is None or _file_sha256(scores_dir / name) != recorded:
            raise SelfScoresRefused(f"{name} does not hash to what the manifest recorded")
    return manifest


def table_of(session: Session, manifest: dict[str, Any]) -> tuple[CutPhoto, HomologyTable]:
    """The loaded photo the scores name and its homology table, refused unless the queries agree."""
    named = manifest["photo"]
    photo = session.execute(
        select(CutPhoto).where(CutPhoto.manifest_sha256 == named["manifest_sha256"])
    ).scalar_one_or_none()
    if photo is None or photo.cut_release != int(named["cut_release"]) or photo.kind != "full":
        raise SelfScoresRefused(
            f"no loaded full photo of cut {named['cut_release']} has manifest "
            f"{named['manifest_sha256']}: load the photo first"
        )
    table = session.execute(
        select(HomologyTable).where(HomologyTable.cut_photo_id == photo.id)
    ).scalar_one_or_none()
    if table is None:
        raise SelfScoresRefused(
            f"the photo of cut {photo.cut_release} has no homology table: load it first"
        )
    prepared = (table.manifest or {}).get("counts", {}).get("prepare", {})
    if (
        prepared.get("queries_fasta_sha256") != manifest["queries_fasta_sha256"]
        or int(manifest["queries"]) != table.queries
    ):
        raise SelfScoresRefused(
            f"the scores were computed for {manifest['queries']} queries whose bytes hash to "
            f"{manifest['queries_fasta_sha256']}, and the homology table of cut "
            f"{photo.cut_release} has {table.queries} queries hashing to "
            f"{prepared.get('queries_fasta_sha256')}: they are not this table's queries"
        )
    return photo, table


def check_scores(
    session: Session, scores_dir: Path, manifest: dict[str, Any], photo: CutPhoto
) -> int:
    """One positive finite bit score per scored query, each a superset sequence; the row count."""
    table = pq.read_table(scores_dir / "self-scores.parquet", columns=["query_md5", "bits"])
    md5s = table.column("query_md5").to_pylist()
    if len(set(md5s)) != len(md5s):
        raise SelfScoresRefused("a query is scored twice")
    if not all(math.isfinite(b) and b > 0 for b in table.column("bits").to_pylist()):
        raise SelfScoresRefused("a self score is not a positive finite bit score")
    without = int(manifest["queries_without_self_score"])
    if len(md5s) != int(manifest["queries"]) - without:
        raise SelfScoresRefused(
            f"the scores hold {len(md5s)} rows and the manifest counts {manifest['queries']} "
            f"queries, {without} of them without a self score"
        )
    superset = set(
        session.execute(
            select(CutPhotoMember.sequence_md5).where(
                CutPhotoMember.photo_id == photo.id,
                CutPhotoMember.superset.is_(True),
                CutPhotoMember.sequence_md5.is_not(None),
            )
        ).scalars()
    )
    strange = set(md5s) - superset
    if strange:
        raise SelfScoresRefused(
            f"{len(strange)} scored sequences are no superset sequence of the photo of cut "
            f"{photo.cut_release}"
        )
    return len(md5s)


def scores_keys_for(cut: int, manifest_sha256: str) -> tuple[str, str]:
    """Where a table's self scores and their manifest live in the artifact store."""
    prefix = f"homology-self-scores/{cut}/{manifest_sha256}"
    return f"{prefix}/self-scores.parquet", f"{prefix}/{MANIFEST}"


def store_scores(scores_dir: Path, cut: int, manifest_sha256: str) -> tuple[str, str]:
    """Put the verified scores and manifest in the artifact store; return their URIs."""
    store = get_artifact_store(load_settings(Path(__file__).resolve().parents[3]))
    scores_key, manifest_key = scores_keys_for(cut, manifest_sha256)
    scores_uri = str(store.put(scores_key, scores_dir / "self-scores.parquet"))
    return scores_uri, str(store.put(manifest_key, scores_dir / MANIFEST))


def descriptors(manifest: dict[str, Any]) -> tuple[str, str]:
    """ADR-D58's short and long descriptors, derived from the manifest, never typed."""
    cut = int(manifest["cut_release"])
    queries, without = int(manifest["queries"]), int(manifest["queries_without_self_score"])
    version = str(manifest["mmseqs"]["version"])[:12]
    return f"homology-self-scores-{cut}", (
        f"The self-alignment bit scores of the {queries:,} queries of the homology table of cut "
        f"{cut}: each sequence aligned with itself by MMseqs2 {version} at the table's scoring "
        f"and no e-value cut; {queries - without:,} scored, {without:,} that the alignment "
        "could not score (a degenerate sequence)."
    )


class LoadHomologySelfScoresOperation:
    """Write the verified self scores of one homology table's queries."""

    name = "load_homology_self_scores"
    description = (
        "Verify the self-alignment bit scores of a homology table's queries built away from the "
        "server (their manifest, their photo, the table's queries) and declare them."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        digest = str(payload.get("manifest_sha256", ""))[:12]
        return f"self scores {digest} <- {payload.get('scores_dir', '')}" if digest else ""

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = LoadHomologySelfScoresPayload.model_validate(contract_payload(payload))
        scores_dir = Path(p.scores_dir)
        manifest = read_scores_manifest(scores_dir, p.manifest_sha256)
        photo, table = table_of(session, manifest)
        existing = session.execute(
            select(HomologySelfScores).where(HomologySelfScores.homology_table_id == table.id)
        ).scalar_one_or_none()
        if existing is not None:
            if existing.manifest_sha256 == p.manifest_sha256:
                emit(
                    "load_homology_self_scores.already_loaded",
                    None,
                    {"cut": photo.cut_release},
                    "info",
                )
                return OperationResult(result={"self_scores_id": str(existing.id), "skipped": True})
            raise SelfScoresRefused(
                f"the homology table of cut {photo.cut_release} already has self scores, with "
                f"manifest {existing.manifest_sha256}"
            )
        scored = check_scores(session, scores_dir, manifest, photo)
        scores_uri, manifest_uri = store_scores(scores_dir, photo.cut_release, p.manifest_sha256)
        short_name, description = descriptors(manifest)
        row = HomologySelfScores(
            homology_table_id=table.id,
            mmseqs_version=str(manifest["mmseqs"]["version"]),
            mmseqs_binary_sha256=str(manifest["mmseqs"]["binary_sha256"]),
            command=list(manifest["command"]),
            queries=int(manifest["queries"]),
            queries_without_self_score=int(manifest["queries_without_self_score"]),
            scores_uri=scores_uri,
            scores_sha256=str(manifest["outputs"]["self-scores.parquet"]),
            manifest=manifest,
            manifest_uri=manifest_uri,
            manifest_sha256=p.manifest_sha256,
            short_name=short_name,
            description=description,
            code_revision=code_revision(),
            builder_revision=manifest.get("revision"),
            job_id=job_id_from_payload(payload),
        )
        session.add(row)
        session.flush()
        result = {
            "self_scores_id": str(row.id),
            "homology_table_id": str(table.id),
            "cut": photo.cut_release,
            "queries": row.queries,
            "scored": scored,
            "queries_without_self_score": row.queries_without_self_score,
            "scores_uri": scores_uri,
            "short_name": short_name,
        }
        emit("load_homology_self_scores.done", None, result, "info")
        return OperationResult(result=result)


__all__ = [
    "LoadHomologySelfScoresOperation",
    "LoadHomologySelfScoresPayload",
    "SelfScoresRefused",
]
