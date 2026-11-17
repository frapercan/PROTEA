"""Load the lexicon bank of a set of photos decoded away from the server (ADR-D63).

The cluster half is :mod:`protea.core.lexicon_bank.build`. This operation refuses
before writing anything unless:

- ``lexicon/<name>/lexicon-bank.json`` hashes to the sha256 the payload names, is
  ``complete`` and is the bank of that name;
- the weights the payload names are a registered lexicon (kind ``lexicon``) whose
  sha256 is the one the bank was decoded with, and so is its parity record's;
- every photo it names is loaded (by manifest sha256, cut and kind);
- every shard of both passes still hashes to what the manifest recorded and
  holds the rows it counted;
- the cut's md5s are unique, each is a ``sequence`` row, and together they are
  exactly the md5s of the photos' members.

Then it puts the manifest and every shard in the artifact store and writes one
``lexicon_bank`` row. The same manifest again is a no-op.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from pydantic import field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.code_revision import code_revision
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.file_stage import file_sha256
from protea.core.operations._bank_population import loaded_photos, population_gaps
from protea.core.source_guards import SourceGuardError
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.embedding.lexicon_bank import LexiconBank
from protea.infrastructure.orm.models.embedding.model_weights import ModelWeights
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

PASSES = ("cut", "mass")


class LexiconBankRefused(SourceGuardError):
    """A lexicon bank the loader will not write, with the reason."""


class LoadLexiconBankPayload(ProteaPayload, frozen=True):
    """Where the bank's files are, its lexicon's name and weights, and its manifest's sha256."""

    bank_dir: str
    name: str
    model_weights_id: str
    manifest_sha256: str

    @field_validator("model_weights_id", mode="before")
    @classmethod
    def weights_must_be_a_uuid(cls, v: Any) -> str:
        return str(uuid.UUID(str(v)))

    @field_validator("manifest_sha256", mode="before")
    @classmethod
    def digest_must_be_hex64(cls, v: Any) -> str:
        text = str(v).strip().lower()
        if len(text) != 64 or any(c not in "0123456789abcdef" for c in text):
            raise ValueError("manifest_sha256 must be 64 hexadecimal characters")
        return text


def root_of(bank_dir: Path, name: str) -> Path:
    """Where the builder writes a lexicon's bank: the one spelling."""
    return bank_dir / "lexicon" / name


def read_bank_manifest(root: Path, name: str, expected_sha256: str) -> dict[str, Any]:
    """The verified manifest, refused unless it hashes as named, is complete and is this lexicon's."""
    path = root / "lexicon-bank.json"
    if not path.is_file():
        raise LexiconBankRefused(f"{path} does not exist")
    got = file_sha256(path)
    if got != expected_sha256:
        raise LexiconBankRefused(f"{path.name} hashes to {got}, not to {expected_sha256}")
    manifest: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("status") != "complete":
        raise LexiconBankRefused(
            f"the bank's status is {manifest.get('status')!r}: only a complete bank loads"
        )
    if manifest.get("name") != name:
        raise LexiconBankRefused(
            f"the manifest is the bank of {manifest.get('name')!r}, not of {name!r}"
        )
    return manifest


def checked_weights(session: Session, weights_id: str, manifest: dict[str, Any]) -> ModelWeights:
    """The registered lexicon, refused unless its sha256 is the one the bank and its parity used."""
    weights = session.get(ModelWeights, uuid.UUID(weights_id))
    if weights is None or weights.kind != "lexicon":
        raise LexiconBankRefused(f"{weights_id} is not a registered lexicon")
    if weights.sha256 != manifest["lexicon_sha256"]:
        raise LexiconBankRefused(
            f"the bank was decoded with {manifest['lexicon_sha256']}, not with {weights.sha256}"
        )
    parity = manifest.get("parity") or {}
    if parity.get("lexicon_sha256") != weights.sha256:
        raise LexiconBankRefused("the parity record is not about this lexicon")
    return weights


def check_shards(root: Path, manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """Every shard of both passes as recorded; the list the row keeps (pass, shard, rows, sha256)."""
    listed = {name: manifest["passes"][name]["shards"] for name in PASSES}
    if [s["shard"] for s in listed["cut"]] != [s["shard"] for s in listed["mass"]]:
        raise LexiconBankRefused("the two passes do not cover the same shards")
    out = []
    for name in PASSES:
        for shard in listed[name]:
            path = root / name / f"shard-{int(shard['shard']):05d}.parquet"
            if not path.is_file() or file_sha256(path) != shard["sha256"]:
                raise LexiconBankRefused(
                    f"{name}/{path.name} is missing or does not hash as recorded"
                )
            if pq.ParquetFile(path).metadata.num_rows != int(shard["rows"]):
                raise LexiconBankRefused(f"{name}/{path.name} does not hold the rows it counted")
            out.append({"pass": name, **shard})
    if sum(s["rows"] for s in listed["cut"]) != int(manifest["sequences"]):
        raise LexiconBankRefused("the shards do not hold every sequence of the bank")
    return out


def store_bank(root: Path, name: str, manifest_sha256: str, shards: list[dict[str, Any]]) -> str:
    """Put the manifest and every shard in the artifact store; fill each shard's URI; return the manifest's."""
    project_root = Path(__file__).resolve().parents[3]
    store = get_artifact_store(load_settings(project_root))
    prefix = f"lexicon-banks/{name}/{manifest_sha256}"
    for shard in shards:
        stem = f"{shard['pass']}/shard-{int(shard['shard']):05d}.parquet"
        shard["uri"] = str(store.put(f"{prefix}/{stem}", root / stem))
    return str(store.put(f"{prefix}/lexicon-bank.json", root / "lexicon-bank.json"))


def descriptors(manifest: dict[str, Any]) -> tuple[str, str]:
    """ADR-D58's short and long descriptors, derived from the manifest, never typed."""
    cuts = sorted(int(p["cut_release"]) for p in manifest["photos"])
    short = f"lexicon-{manifest['name']}-cuts-{'-'.join(map(str, cuts))}"[:64]
    cut = manifest["parity"].get("cut", {})
    return short, (
        f"The lexicon bank of {manifest['name']} over the {manifest['sequences']:,} unique member "
        f"sequences of the photos of cuts {', '.join(map(str, cuts))} (ADR-D63): every sequence's "
        f"best-path segments and columns, and its theme mass, decoded at full length by the "
        f"platform's decoder on {manifest['passes']['cut']['card']}. Against the laboratory's code "
        f"the cut was identical in {cut.get('labels', 0)} of {cut.get('proteins', 0)} proteins."
    )


def new_bank(
    manifest: dict[str, Any],
    stored: tuple[str, str],
    shards: list[dict[str, Any]],
    weights_id: uuid.UUID,
) -> LexiconBank:
    """The row a verified manifest declares; ``stored`` is its sha256 and its URI in the store."""
    short_name, description = descriptors(manifest)
    return LexiconBank(
        model_weights_id=weights_id,
        name=manifest["name"],
        photos=[
            {k: x[k] for k in ("cut_release", "kind", "manifest_sha256")}
            for x in manifest["photos"]
        ],
        sequences=int(manifest["sequences"]),
        residues=int(manifest["residues"]),
        sequences_sha256=str(manifest["sequences_parquet_sha256"]),
        shards=shards,
        passes={
            k: {f: v[f] for f in ("runs", "card", "software")}
            for k, v in manifest["passes"].items()
        },
        parity=manifest["parity"],
        manifest=manifest,
        manifest_uri=stored[1],
        manifest_sha256=stored[0],
        short_name=short_name,
        description=description,
        code_revision=code_revision(),
        builder_revision=manifest.get("revision"),
    )


class LoadLexiconBankOperation:
    """Write one verified lexicon bank of a set of photos (ADR-D63)."""

    name = "load_lexicon_bank"
    description = (
        "Verify the lexicon bank of a set of photos decoded away from the server (its manifest, "
        "its lexicon, its parity, its photos, its shards, its population) and declare it (ADR-D63)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        digest = str(payload.get("manifest_sha256", ""))[:12]
        return f"lexicon bank {payload.get('name', '')} {digest}" if digest else ""

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = LoadLexiconBankPayload.model_validate(contract_payload(payload))
        root = root_of(Path(p.bank_dir), p.name)
        manifest = read_bank_manifest(root, p.name, p.manifest_sha256)
        weights = checked_weights(session, p.model_weights_id, manifest)
        existing = session.execute(
            select(LexiconBank).where(
                LexiconBank.model_weights_id == weights.id,
                LexiconBank.manifest_sha256 == p.manifest_sha256,
            )
        ).scalar_one_or_none()
        if existing is not None:
            emit("load_lexicon_bank.already_loaded", None, {"bank": existing.short_name}, "info")
            return OperationResult(result={"bank_id": str(existing.id), "skipped": True})
        photos = loaded_photos(session, manifest["photos"], LexiconBankRefused)
        shards = check_shards(root, manifest)
        cut_files = [
            root / "cut" / f"shard-{int(s['shard']):05d}.parquet"
            for s in shards
            if s["pass"] == "cut"
        ]
        gaps = population_gaps(session, cut_files, [photo.id for photo in photos])
        if gaps:
            raise LexiconBankRefused(f"the bank is not the photos' members: {gaps}")
        manifest_uri = store_bank(root, p.name, p.manifest_sha256, shards)
        row = new_bank(manifest, (p.manifest_sha256, manifest_uri), shards, weights.id)
        row.job_id = job_id_from_payload(payload)
        session.add(row)
        session.flush()
        result = {
            "bank_id": str(row.id),
            "short_name": row.short_name,
            "shards": len(shards),
            "manifest_uri": manifest_uri,
        }
        emit("load_lexicon_bank.done", None, result, "info")
        return OperationResult(result=result)


__all__ = ["LexiconBankRefused", "LoadLexiconBankOperation", "LoadLexiconBankPayload"]
