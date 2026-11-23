"""Load the segment families of a cut built away from the server (ADR-D65, decision 2).

The cluster half is :mod:`protea.core.segment_families.build`. This operation
refuses before writing anything unless:

- ``segment-families.json`` hashes to the sha256 the payload names, is
  ``complete`` and declares the ``segment_families`` table;
- the photo it names is the loaded full photo of its cut (by manifest sha256);
- the lexicon bank it names is loaded (by manifest sha256 and name), was
  decoded with the lexicon it names and covers that photo;
- every output this builder writes (the segments and the families of each
  scope) is present, hashes as recorded, and they all hold one row per segment;
- every donor md5 is the sequence of a ``truth`` member of the photo and every
  target md5 that of another superset member.

Then it puts the manifest and every output in the artifact store and writes one
``segment_families`` row. A photo and a lexicon bank have one: the same
manifest again is a no-op and another one is refused.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pyarrow.compute as pc
import pyarrow.parquet as pq
from pydantic import field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.code_revision import code_revision
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.file_stage import file_sha256
from protea.core.operations._bank_population import loaded_photos
from protea.core.segment_families.build import SCOPES
from protea.core.source_guards import SourceGuardError
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.embedding.lexicon_bank import LexiconBank
from protea.infrastructure.orm.models.embedding.model_weights import ModelWeights
from protea.infrastructure.orm.models.embedding.segment_families import SegmentFamilies
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

MANIFEST = "segment-families.json"
OUTPUTS = ("segments.parquet", *(f"families-{s}.parquet" for s in SCOPES))


class SegmentFamiliesRefused(SourceGuardError):
    """Segment families the loader will not write, with the reason."""


class LoadSegmentFamiliesPayload(ProteaPayload, frozen=True):
    """Where the families' files are and their manifest's sha256."""

    families_dir: str
    manifest_sha256: str

    @field_validator("manifest_sha256", mode="before")
    @classmethod
    def digest_must_be_hex64(cls, v: Any) -> str:
        text = str(v).strip().lower()
        if len(text) != 64 or any(c not in "0123456789abcdef" for c in text):
            raise ValueError("manifest_sha256 must be 64 hexadecimal characters")
        return text


def read_families_manifest(root: Path, expected_sha256: str) -> dict[str, Any]:
    """The verified manifest, refused unless it hashes as named, is complete and is a families table."""
    path = root / MANIFEST
    if not path.is_file():
        raise SegmentFamiliesRefused(f"{path} does not exist")
    got = file_sha256(path)
    if got != expected_sha256:
        raise SegmentFamiliesRefused(f"{path.name} hashes to {got}, not to {expected_sha256}")
    manifest: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("status") != "complete":
        raise SegmentFamiliesRefused(
            f"the families' status is {manifest.get('status')!r}: only complete families load"
        )
    if manifest.get("table") != "segment_families":
        raise SegmentFamiliesRefused(f"the manifest declares {manifest.get('table')!r}")
    return manifest


def checked_bank(session: Session, manifest: dict[str, Any], photo_sha256: str) -> LexiconBank:
    """The loaded lexicon bank the families were cut from, of the lexicon named, covering the photo."""
    named = manifest["lexicon"]
    bank = session.execute(
        select(LexiconBank).where(LexiconBank.manifest_sha256 == named["manifest_sha256"])
    ).scalar_one_or_none()
    if bank is None or bank.name != named["name"]:
        raise SegmentFamiliesRefused(
            f"no loaded lexicon bank of {named['name']!r} has manifest {named['manifest_sha256']}: "
            "load the bank first"
        )
    weights = session.get(ModelWeights, bank.model_weights_id)
    if weights is None or weights.sha256 != named["sha256"]:
        raise SegmentFamiliesRefused("the bank was not decoded with the lexicon the families name")
    if photo_sha256 not in {x["manifest_sha256"] for x in bank.photos}:
        raise SegmentFamiliesRefused(f"the lexicon bank {bank.short_name} does not cover the photo")
    return bank


def check_outputs(root: Path, manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """Every output as recorded and of one row per segment; the list the row keeps."""
    recorded = manifest.get("outputs") or {}
    if sorted(recorded) != sorted(OUTPUTS):
        raise SegmentFamiliesRefused(
            f"the manifest lists {sorted(recorded)}, and this builder writes {sorted(OUTPUTS)}"
        )
    out = []
    for name in OUTPUTS:
        path = root / name
        if not path.is_file() or file_sha256(path) != recorded[name]:
            raise SegmentFamiliesRefused(f"{name} is missing or does not hash as recorded")
        out.append(
            {"name": name, "rows": pq.ParquetFile(path).metadata.num_rows, "sha256": recorded[name]}
        )
    if len({o["rows"] for o in out}) != 1:
        raise SegmentFamiliesRefused(
            f"the outputs hold {[o['rows'] for o in out]} rows, not one per segment"
        )
    return out


_GAPS = {
    "donor md5s that are no truth member's": (
        "SELECT count(*) FROM family_md5 f WHERE f.donor AND NOT EXISTS (SELECT 1 FROM "
        "cut_photo_member m WHERE m.photo_id = %(photo)s AND m.truth AND m.sequence_md5 = f.md5)"
    ),
    "target md5s that are no superset member's": (
        "SELECT count(*) FROM family_md5 f WHERE NOT f.donor AND NOT EXISTS (SELECT 1 FROM "
        "cut_photo_member m WHERE m.photo_id = %(photo)s AND m.superset AND m.sequence_md5 = f.md5)"
    ),
}


def population_gaps(session: Session, segments: Path, photo_id: uuid.UUID) -> dict[str, int]:
    """How far the segments' md5s are from the photo's donors and targets; ``both`` when one is either."""
    table = (
        pq.read_table(segments, columns=["md5", "donor"]).group_by(["md5", "donor"]).aggregate([])
    )
    both = len(table) - len(pc.unique(table["md5"]))
    raw = session.connection().connection.driver_connection
    with raw.cursor() as cur:
        cur.execute("CREATE TEMP TABLE family_md5 (md5 varchar(32), donor boolean) ON COMMIT DROP")
        with cur.copy("COPY family_md5 (md5, donor) FROM STDIN") as copy:
            for md5, donor in zip(
                table["md5"].to_pylist(), table["donor"].to_pylist(), strict=True
            ):
                copy.write_row((md5, donor))
        gaps = {"md5s both donor and target": both}
        for name, sql in _GAPS.items():
            cur.execute(sql, {"photo": photo_id})
            gaps[name] = int(cur.fetchone()[0])
    return {name: count for name, count in gaps.items() if count}


def store_families(root: Path, prefix: str, outputs: list[dict[str, Any]]) -> str:
    """Put the manifest and every output in the artifact store; fill each output's URI; return the manifest's."""
    project_root = Path(__file__).resolve().parents[3]
    store = get_artifact_store(load_settings(project_root))
    for output in outputs:
        output["uri"] = str(store.put(f"{prefix}/{output['name']}", root / output["name"]))
    return str(store.put(f"{prefix}/{MANIFEST}", root / MANIFEST))


def descriptors(manifest: dict[str, Any]) -> tuple[str, str]:
    """ADR-D58's short and long descriptors, derived from the manifest, never typed."""
    name, cut = manifest["lexicon"]["name"], int(manifest["cut_release"])
    prepare = manifest["counts"]["prepare"]
    scopes = manifest["counts"]["families"]
    per_scope = "; ".join(
        f"{s}: {scopes[s]['families']:,} families, {scopes[s]['families_of_two_or_more']:,} "
        "of two or more cores"
        for s in SCOPES
    )
    return f"families-{name}-{cut}"[:64], (
        f"The segment families of cut {cut} under {name} (ADR-D65): the cores of the lexicon's "
        f"segments of the photo's {prepare['donor_sequences']:,} donor sequences (truth members) "
        f"and {prepare['target_sequences']:,} target sequences (other superset members), "
        f"linked by MMseqs2 {manifest['mmseqs']['version'][:7]}. {per_scope}."
    )


def new_families(
    manifest: dict[str, Any],
    stored: tuple[str, str],
    outputs: list[dict[str, Any]],
    sources: tuple[uuid.UUID, uuid.UUID],
) -> SegmentFamilies:
    """The row a verified manifest declares; ``stored`` is its sha256 and its URI in the store."""
    short_name, description = descriptors(manifest)
    return SegmentFamilies(
        cut_photo_id=sources[0],
        lexicon_bank_id=sources[1],
        cut_release=int(manifest["cut_release"]),
        scopes=list(SCOPES),
        segments=int(outputs[0]["rows"]),
        outputs=outputs,
        counts=manifest["counts"],
        mmseqs={**manifest["mmseqs"], "commands": manifest.get("commands")},
        manifest=manifest,
        manifest_uri=stored[1],
        manifest_sha256=stored[0],
        short_name=short_name,
        description=description,
        code_revision=code_revision(),
        builder_revision=manifest.get("revision"),
    )


class LoadSegmentFamiliesOperation:
    """Write the verified segment families of one cut under one lexicon (ADR-D65)."""

    name = "load_segment_families"
    description = (
        "Verify the segment families of a cut built away from the server (their manifest, photo, "
        "lexicon bank, outputs and population) and declare them (ADR-D65)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        digest = str(payload.get("manifest_sha256", ""))[:12]
        return f"segment families {digest}" if digest else ""

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = LoadSegmentFamiliesPayload.model_validate(contract_payload(payload))
        root = Path(p.families_dir)
        manifest = read_families_manifest(root, p.manifest_sha256)
        existing = session.execute(
            select(SegmentFamilies).where(SegmentFamilies.manifest_sha256 == p.manifest_sha256)
        ).scalar_one_or_none()
        if existing is not None:
            emit(
                "load_segment_families.already_loaded",
                None,
                {"families": existing.short_name},
                "info",
            )
            return OperationResult(
                result={"segment_families_id": str(existing.id), "skipped": True}
            )
        named = {**manifest["photo"], "kind": "full"}
        (photo,) = loaded_photos(session, [named], SegmentFamiliesRefused)
        bank = checked_bank(session, manifest, photo.manifest_sha256)
        taken = session.execute(
            select(SegmentFamilies).where(
                SegmentFamilies.cut_photo_id == photo.id,
                SegmentFamilies.lexicon_bank_id == bank.id,
            )
        ).scalar_one_or_none()
        if taken is not None:
            raise SegmentFamiliesRefused(
                f"the photo of cut {photo.cut_release} already has families under this bank "
                f"({taken.short_name}, manifest {taken.manifest_sha256})"
            )
        outputs = check_outputs(root, manifest)
        gaps = population_gaps(session, root / "segments.parquet", photo.id)
        if gaps:
            raise SegmentFamiliesRefused(
                f"the segments are not the photo's donors and targets: {gaps}"
            )
        prefix = f"segment-families/{bank.name}/{photo.cut_release}/{p.manifest_sha256}"
        manifest_uri = store_families(root, prefix, outputs)
        row = new_families(
            manifest, (p.manifest_sha256, manifest_uri), outputs, (photo.id, bank.id)
        )
        row.job_id = job_id_from_payload(payload)
        session.add(row)
        session.flush()
        result = {
            "segment_families_id": str(row.id),
            "short_name": row.short_name,
            "segments": row.segments,
            "manifest_uri": manifest_uri,
        }
        emit("load_segment_families.done", None, result, "info")
        return OperationResult(result=result)


__all__ = [
    "LoadSegmentFamiliesOperation",
    "LoadSegmentFamiliesPayload",
    "SegmentFamiliesRefused",
]
