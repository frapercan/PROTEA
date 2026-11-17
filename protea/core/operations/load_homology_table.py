"""Load the homology table of a cut built away from the server (ADR-D61).

The cluster half is :mod:`protea.core.homology.build`. This operation refuses
before writing anything unless:

- the manifest hashes to the sha256 the payload names, and its status is
  ``complete`` (a sampled ``measurement`` is never loaded);
- the photo it names is the loaded photo of the same cut, found by the photo
  manifest's sha256;
- ``hits.parquet`` still hashes to what the manifest recorded, and holds the
  hits the manifest counted;
- every query is the sequence of a superset member of that photo, and every
  target the sequence of one of its members.

Then it puts the hits and the manifest in the artifact store and writes one
``homology_table`` row with its ADR-D58 descriptors. A cut has one table: the
same manifest again is a no-op, and another one is refused.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow.compute as pc
import pyarrow.parquet as pq
from pydantic import field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.code_revision import code_revision
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.homology.build import OUTPUTS
from protea.core.source_guards import SourceGuardError
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto, CutPhotoMember
from protea.infrastructure.orm.models.annotation.homology_table import HomologyTable
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store


class HomologyTableRefused(SourceGuardError):
    """A homology table the loader will not write, with the reason."""


class LoadHomologyTablePayload(ProteaPayload, frozen=True):
    """Where the table's files are and what its manifest hashes to; the rest is read from it."""

    table_dir: str
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


def read_table_manifest(table_dir: Path, expected_sha256: str) -> dict[str, Any]:
    """The verified ``homology.json``, refused unless it hashes as named and is complete."""
    path = table_dir / "homology.json"
    if not path.is_file():
        raise HomologyTableRefused(f"{path} does not exist")
    got = _file_sha256(path)
    if got != expected_sha256:
        raise HomologyTableRefused(f"homology.json hashes to {got}, not to {expected_sha256}")
    manifest: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("status") != "complete":
        raise HomologyTableRefused(
            f"the table's status is {manifest.get('status')!r}: only a complete table loads"
        )
    for name in OUTPUTS:
        recorded = manifest.get("outputs", {}).get(name)
        if recorded is None or _file_sha256(table_dir / name) != recorded:
            raise HomologyTableRefused(f"{name} does not hash to what the manifest recorded")
    return manifest


def photo_of(session: Session, manifest: dict[str, Any]) -> CutPhoto:
    """The loaded photo the table was built from, by its manifest's sha256 and cut."""
    named = manifest["photo"]
    photo = session.execute(
        select(CutPhoto).where(CutPhoto.manifest_sha256 == named["manifest_sha256"])
    ).scalar_one_or_none()
    if photo is None:
        raise HomologyTableRefused(
            f"no loaded photo has manifest {named['manifest_sha256']}: load the photo first"
        )
    if photo.cut_release != int(named["cut_release"]) or photo.kind != "full":
        raise HomologyTableRefused(
            f"the photo with that manifest is cut {photo.cut_release} ({photo.kind}), "
            f"not the full photo of cut {named['cut_release']}"
        )
    return photo


def check_hits(
    session: Session, table_dir: Path, manifest: dict[str, Any], photo: CutPhoto
) -> None:
    """Every query a superset member's sequence, every target a member's; counts as recorded."""
    hits = pq.read_table(table_dir / "hits.parquet", columns=["query_md5", "target_md5"])
    search = manifest["counts"]["search"]
    if hits.num_rows != int(search["hits"]):
        raise HomologyTableRefused(
            f"hits.parquet holds {hits.num_rows} hits and the manifest counted {search['hits']}"
        )
    queries = set(pc.unique(hits.column("query_md5")).to_pylist())
    targets = set(pc.unique(hits.column("target_md5")).to_pylist())
    if len(queries) != int(search["queries_with_hits"]):
        raise HomologyTableRefused("the queries with hits are not the ones the manifest counted")
    members = session.execute(
        select(CutPhotoMember.sequence_md5, CutPhotoMember.superset).where(
            CutPhotoMember.photo_id == photo.id, CutPhotoMember.sequence_md5.is_not(None)
        )
    ).all()
    superset = {md5 for md5, in_superset in members if in_superset}
    bank = {md5 for md5, _ in members}
    strange_queries = queries - superset
    strange_targets = targets - bank
    if strange_queries or strange_targets:
        raise HomologyTableRefused(
            f"{len(strange_queries)} queries are no superset sequence and "
            f"{len(strange_targets)} targets no member sequence of the photo of cut "
            f"{photo.cut_release}"
        )


def table_keys_for(cut: int, manifest_sha256: str) -> tuple[str, str]:
    """Where a table's hits and manifest live in the artifact store: the one spelling."""
    prefix = f"homology/{cut}/{manifest_sha256}"
    return f"{prefix}/hits.parquet", f"{prefix}/homology.json"


def store_table(table_dir: Path, cut: int, manifest_sha256: str) -> tuple[str, str]:
    """Put the verified hits and manifest in the artifact store; return their URIs."""
    project_root = Path(__file__).resolve().parents[3]
    store = get_artifact_store(load_settings(project_root))
    hits_key, manifest_key = table_keys_for(cut, manifest_sha256)
    hits_uri = str(store.put(hits_key, table_dir / "hits.parquet"))
    return hits_uri, str(store.put(manifest_key, table_dir / "homology.json"))


def descriptors(manifest: dict[str, Any]) -> tuple[str, str]:
    """ADR-D58's short and long descriptors, derived from the manifest, never typed."""
    cut = int(manifest["cut_release"])
    prepare, search = manifest["counts"]["prepare"], manifest["counts"]["search"]
    version = str(manifest["mmseqs"]["version"])[:12]
    return f"homology-{cut}", (
        f"The homology table of cut {cut} (ADR-D61): the {prepare['queries']:,} unique "
        f"sequences of the photo's superset searched against the {prepare['bank']:,} "
        f"unique sequences of its members with MMseqs2 {version} at its defaults; "
        f"{search['hits']:,} hits for {search['queries_with_hits']:,} queries, self hits "
        f"dropped ({search['self_hits_dropped']:,}), no threshold applied."
    )


class LoadHomologyTableOperation:
    """Write one verified homology table of a cut (ADR-D61)."""

    name = "load_homology_table"
    description = (
        "Verify the homology table of a cut built away from the server (its manifest, "
        "its photo, its hits against the photo's members) and declare it (ADR-D61)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        digest = str(payload.get("manifest_sha256", ""))[:12]
        return f"homology {digest} <- {payload.get('table_dir', '')}" if digest else ""

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = LoadHomologyTablePayload.model_validate(contract_payload(payload))
        table_dir = Path(p.table_dir)
        manifest = read_table_manifest(table_dir, p.manifest_sha256)
        photo = photo_of(session, manifest)
        existing = session.execute(
            select(HomologyTable).where(HomologyTable.cut_photo_id == photo.id)
        ).scalar_one_or_none()
        if existing is not None:
            if existing.manifest_sha256 == p.manifest_sha256:
                emit("load_homology_table.already_loaded", None, {"cut": photo.cut_release}, "info")
                return OperationResult(result={"table_id": str(existing.id), "skipped": True})
            raise HomologyTableRefused(
                f"cut {photo.cut_release} already has a homology table, with manifest "
                f"{existing.manifest_sha256}"
            )
        check_hits(session, table_dir, manifest, photo)
        hits_uri, manifest_uri = store_table(table_dir, photo.cut_release, p.manifest_sha256)
        short_name, description = descriptors(manifest)
        prepare, search = manifest["counts"]["prepare"], manifest["counts"]["search"]
        row = HomologyTable(
            cut_photo_id=photo.id,
            cut_release=photo.cut_release,
            mmseqs_version=str(manifest["mmseqs"]["version"]),
            mmseqs_binary_sha256=str(manifest["mmseqs"]["binary_sha256"]),
            command=list(manifest["command"]),
            queries=int(prepare["queries"]),
            bank=int(prepare["bank"]),
            hits=int(search["hits"]),
            self_hits_dropped=int(search["self_hits_dropped"]),
            queries_with_hits=int(search["queries_with_hits"]),
            members_without_sequence=int(prepare["members_without_sequence"]),
            hits_uri=hits_uri,
            hits_sha256=str(manifest["outputs"]["hits.parquet"]),
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
            "table_id": str(row.id),
            "cut": photo.cut_release,
            "hits": row.hits,
            "queries": row.queries,
            "bank": row.bank,
            "hits_uri": hits_uri,
            "short_name": short_name,
        }
        emit("load_homology_table.done", None, result, "info")
        return OperationResult(result=result)


__all__ = [
    "HomologyTableRefused",
    "LoadHomologyTableOperation",
    "LoadHomologyTablePayload",
]
