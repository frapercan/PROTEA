"""Load the embedding bank of a set of photos computed away from the server (ADR-D62).

The cluster half is :mod:`protea.core.embedding_bank.build`. This operation refuses
before writing anything unless:

- ``bank-<config>.json`` hashes to the sha256 the payload names, is ``complete``
  and names the payload's configuration, whose recipe derives its id and is the
  stored configuration's recipe, field for field;
- every photo it names is a loaded photo, found by its manifest's sha256, of the
  same cut and kind;
- every shard still hashes to what the manifest recorded and holds the rows it
  counted, each a vector of the manifest's width;
- the bank's md5s are unique, each is a ``sequence`` row, and together they are
  exactly the md5s of the members of those photos.

Then it puts the manifest in the artifact store, writes the ``embedding_bank`` row
and loads the shards, one transaction each: a binary COPY into a temporary table,
then an INSERT into ``sequence_embedding`` that names the bank and keeps (and
counts) a row that is already there. A rerun of the same manifest resumes at the
first shard not loaded; a loaded bank is a no-op.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from pydantic import field_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from protea.core.code_revision import code_revision
from protea.core.contracts.operation import EmitFn, OperationResult, ProteaPayload
from protea.core.embedding_identity import derive_embedding_config_id, recipe_of
from protea.core.source_guards import SourceGuardError
from protea.core.utils import contract_payload, job_id_from_payload
from protea.infrastructure.orm.models.annotation.cut_photo import CutPhoto
from protea.infrastructure.orm.models.embedding.embedding_bank import EmbeddingBank
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.settings import load_settings
from protea.infrastructure.storage import get_artifact_store

#: Rows read from a shard at a time, so a worker capped at two gigabytes never
#: holds a whole shard of the widest model (some 150,000 rows of 2,560 halves).
BATCH_ROWS = 10_000


class EmbeddingBankRefused(SourceGuardError):
    """An embedding bank the loader will not write, with the reason."""


class LoadEmbeddingBankPayload(ProteaPayload, frozen=True):
    """Where the bank's files are, which configuration and what its manifest hashes to."""

    bank_dir: str
    embedding_config_id: str
    manifest_sha256: str

    @field_validator("embedding_config_id", mode="before")
    @classmethod
    def config_must_be_a_uuid(cls, v: Any) -> str:
        return str(uuid.UUID(str(v)))

    @property
    def config_id(self) -> uuid.UUID:
        return uuid.UUID(self.embedding_config_id)

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


def manifest_path(bank_dir: Path, config_id: uuid.UUID) -> Path:
    """Where the builder writes a configuration's manifest: the one spelling."""
    return bank_dir / f"bank-{config_id}.json"


def shard_path(bank_dir: Path, config_id: uuid.UUID, shard: int) -> Path:
    """Where the builder writes a configuration's shard: the one spelling."""
    return bank_dir / "vectors" / str(config_id) / f"shard-{shard:05d}.parquet"


def read_bank_manifest(
    bank_dir: Path, config_id: uuid.UUID, expected_sha256: str
) -> dict[str, Any]:
    """The verified manifest, refused unless it hashes as named, is complete and is this config's."""
    path = manifest_path(bank_dir, config_id)
    if not path.is_file():
        raise EmbeddingBankRefused(f"{path} does not exist")
    got = _file_sha256(path)
    if got != expected_sha256:
        raise EmbeddingBankRefused(f"{path.name} hashes to {got}, not to {expected_sha256}")
    manifest: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("status") != "complete":
        raise EmbeddingBankRefused(
            f"the bank's status is {manifest.get('status')!r}: only a complete bank loads"
        )
    if manifest.get("config_id") != str(config_id):
        raise EmbeddingBankRefused(
            f"the manifest is the bank of {manifest.get('config_id')}, not of {config_id}"
        )
    for key in ("model_revision", "software", "card"):
        if not manifest.get(key):
            raise EmbeddingBankRefused(f"the manifest does not name its {key}")
    return manifest


def checked_config(session: Session, manifest: dict[str, Any]) -> EmbeddingConfig:
    """The stored configuration, refused unless the manifest's recipe is it, field for field."""
    recipe = manifest["recipe"]
    derived = derive_embedding_config_id(recipe)
    if str(derived) != manifest["config_id"]:
        raise EmbeddingBankRefused(f"the manifest's recipe derives {derived}")
    config = session.get(EmbeddingConfig, derived)
    if config is None:
        raise EmbeddingBankRefused(f"no stored embedding configuration {derived}")
    stored = json.loads(json.dumps(recipe_of(config), default=str))
    if stored != json.loads(json.dumps(recipe, default=str)):
        raise EmbeddingBankRefused(f"configuration {derived} is not the recipe the bank used")
    return config


def photos_of(session: Session, manifest: dict[str, Any]) -> list[CutPhoto]:
    """The loaded photos the bank covers, each by its manifest's sha256, cut and kind."""
    photos = []
    for named in manifest["photos"]:
        photo = session.execute(
            select(CutPhoto).where(CutPhoto.manifest_sha256 == named["manifest_sha256"])
        ).scalar_one_or_none()
        if photo is None:
            raise EmbeddingBankRefused(
                f"no loaded photo has manifest {named['manifest_sha256']}: load the photo first"
            )
        if photo.cut_release != int(named["cut_release"]) or photo.kind != named["kind"]:
            raise EmbeddingBankRefused(
                f"the photo with that manifest is cut {photo.cut_release} ({photo.kind}), not "
                f"cut {named['cut_release']} ({named['kind']})"
            )
        photos.append(photo)
    return photos


def check_shards(bank_dir: Path, manifest: dict[str, Any]) -> None:
    """Every shard hashes as recorded and holds its rows, each of the manifest's width."""
    config_id = uuid.UUID(manifest["config_id"])
    total = 0
    for shard in manifest["shards"]:
        path = shard_path(bank_dir, config_id, int(shard["shard"]))
        if not path.is_file() or _file_sha256(path) != shard["sha256"]:
            raise EmbeddingBankRefused(f"{path.name} is missing or does not hash as recorded")
        handle = pq.ParquetFile(path)
        width = handle.schema_arrow.field("vector").type.list_size
        if handle.metadata.num_rows != int(shard["rows"]) or width != int(manifest["dim"]):
            raise EmbeddingBankRefused(
                f"{path.name} holds {handle.metadata.num_rows} rows of {width}, the manifest "
                f"{shard['rows']} of {manifest['dim']}"
            )
        total += int(shard["rows"])
    if total != int(manifest["sequences"]):
        raise EmbeddingBankRefused(f"the shards hold {total} rows for {manifest['sequences']}")


def _raw(session: Session) -> Any:
    return session.connection().connection.driver_connection


_POPULATION_GAPS = {
    "md5s with no sequence row": (
        "SELECT count(*) FROM bank_md5 b WHERE NOT EXISTS "
        "(SELECT 1 FROM sequence s WHERE s.sequence_hash = b.md5)"
    ),
    "member md5s the bank lacks": (
        "SELECT count(*) FROM (SELECT DISTINCT m.sequence_md5 AS md5 FROM cut_photo_member m "
        "WHERE m.photo_id = ANY(%(photos)s) AND m.sequence_md5 IS NOT NULL) x "
        "WHERE NOT EXISTS (SELECT 1 FROM bank_md5 b WHERE b.md5 = x.md5)"
    ),
    "bank md5s that are no member's": (
        "SELECT count(*) FROM bank_md5 b WHERE NOT EXISTS (SELECT 1 FROM cut_photo_member m "
        "WHERE m.photo_id = ANY(%(photos)s) AND m.sequence_md5 = b.md5)"
    ),
}


def check_population(
    session: Session, bank_dir: Path, manifest: dict[str, Any], photos: list[CutPhoto]
) -> None:
    """The bank's md5s are unique, each a sequence, and exactly the photos' member md5s.

    The md5s go into a temporary table that the transaction drops; the shards are read
    by their md5 column alone.
    """
    import psycopg

    config_id = uuid.UUID(manifest["config_id"])
    with _raw(session).cursor() as cur:
        cur.execute("CREATE TEMP TABLE bank_md5 (md5 varchar(32) PRIMARY KEY) ON COMMIT DROP")
        try:
            with cur.copy("COPY bank_md5 (md5) FROM STDIN") as copy:
                for shard in manifest["shards"]:
                    path = shard_path(bank_dir, config_id, int(shard["shard"]))
                    for md5 in pq.read_table(path, columns=["md5"]).column("md5").to_pylist():
                        copy.write_row((md5,))
        except psycopg.errors.UniqueViolation as dup:
            raise EmbeddingBankRefused("a sequence md5 appears twice in the bank") from dup
        params = {"photos": [photo.id for photo in photos]}
        gaps = {}
        for name, sql in _POPULATION_GAPS.items():
            cur.execute(sql, params)
            gaps[name] = int(cur.fetchone()[0])
    found = {name: count for name, count in gaps.items() if count}
    if found:
        raise EmbeddingBankRefused(f"the bank is not the photos' members: {found}")


def bank_key_for(config_id: uuid.UUID, manifest_sha256: str) -> str:
    """Where a bank's manifest lives in the artifact store: the one spelling."""
    return f"embedding-banks/{config_id}/{manifest_sha256}/bank.json"


def store_manifest(bank_dir: Path, config_id: uuid.UUID, manifest_sha256: str) -> str:
    """Put the verified manifest in the artifact store; return its URI."""
    project_root = Path(__file__).resolve().parents[3]
    store = get_artifact_store(load_settings(project_root))
    key = bank_key_for(config_id, manifest_sha256)
    return str(store.put(key, manifest_path(bank_dir, config_id)))


def descriptors(manifest: dict[str, Any]) -> tuple[str, str]:
    """ADR-D58's short and long descriptors, derived from the manifest, never typed."""
    recipe = manifest["recipe"]
    model = str(recipe["model_name"])
    slug = re.sub(r"-+", "-", re.sub(r"[^a-z0-9]", "-", model.rsplit("/", 1)[-1].lower()))
    cuts = sorted(int(p["cut_release"]) for p in manifest["photos"])
    short = f"bank-{slug.strip('-')}-cuts-{'-'.join(map(str, cuts))}"
    if len(short) > 64:
        short = f"bank-{slug.strip('-')}-cuts-{cuts[0]}-to-{cuts[-1]}"[:64]
    software = manifest["software"]
    backends = str(software.get("protea-backends_commit") or software.get("protea-backends"))
    layers = ", ".join(map(str, recipe["layer_indices"]))
    norm = "L2-normalised" if recipe["normalize"] else "not normalised"
    return short, (
        f"The embedding bank of {model} over the {manifest['sequences']:,} unique member "
        f"sequences of the photos of cuts {', '.join(map(str, cuts))} (ADR-D62): layers "
        f"[{layers}] counted from the last, {recipe['layer_agg']} over layers, "
        f"{recipe['pooling']} pooling, {norm}, max_length {recipe['max_length']}; "
        f"{manifest['dim']} dimensions stored as halfvec. Computed on {manifest['card']} "
        f"with the padded pass of protea-backends {backends[:12]} (token budgets "
        f"{manifest['token_budgets']}), weights at {str(manifest['model_revision'])[:12]}."
    )


_STAGE = (
    "CREATE TEMP TABLE bank_stage (md5 varchar(32) NOT NULL, embedding halfvec NOT NULL) "
    "ON COMMIT DROP"
)
_INSERT = (
    "INSERT INTO sequence_embedding (sequence_id, embedding_config_id, chunk_index_s, "
    "chunk_index_e, embedding, embedding_dim, embedding_bank_id, created_at) "
    "SELECT s.id, %(config)s, 0, NULL, b.embedding, %(dim)s, %(bank)s, now() "
    "FROM bank_stage b JOIN sequence s ON s.sequence_hash = b.md5 "
    "ON CONFLICT (sequence_id, embedding_config_id, chunk_index_s) DO NOTHING"
)


def load_shard(session: Session, path: Path, bank: EmbeddingBank, rows: int) -> tuple[int, int]:
    """One shard into ``sequence_embedding``: (rows inserted, rows already there).

    The bank's fields are read BEFORE the copy starts. After a commit the ORM
    reloads an expired field with a query on this same connection, and a query
    issued while the connection is copying waits for the copy, which waits for
    rows that never come: the shard hangs.
    """
    from pgvector import HalfVector
    from pgvector.psycopg import register_vector

    params = {"config": bank.embedding_config_id, "dim": bank.dim, "bank": bank.id}
    raw = _raw(session)
    register_vector(raw)
    with raw.cursor() as cur:
        cur.execute(_STAGE)
        with cur.copy("COPY bank_stage (md5, embedding) FROM STDIN (FORMAT BINARY)") as copy:
            copy.set_types(["varchar", "halfvec"])
            for batch in pq.ParquetFile(path).iter_batches(batch_size=BATCH_ROWS):
                md5s = batch.column("md5").to_pylist()
                flat = batch.column("vector").flatten().to_numpy(zero_copy_only=False)
                vectors = flat.reshape(len(md5s), params["dim"])
                for md5, vector in zip(md5s, vectors, strict=True):
                    copy.write_row((md5, HalfVector(vector)))
        cur.execute(_INSERT, params)
        inserted = int(cur.rowcount)
    return inserted, rows - inserted


def _new_bank(
    manifest: dict[str, Any], manifest_sha256: str, uri: str, payload: dict[str, Any]
) -> EmbeddingBank:
    short_name, description = descriptors(manifest)
    return EmbeddingBank(
        embedding_config_id=uuid.UUID(manifest["config_id"]),
        photos=[
            {k: p[k] for k in ("cut_release", "kind", "manifest_sha256")}
            for p in manifest["photos"]
        ],
        sequences=int(manifest["sequences"]),
        residues=int(manifest["residues"]),
        dim=int(manifest["dim"]),
        shards=len(manifest["shards"]),
        shards_loaded=0,
        rows_inserted=0,
        rows_present=0,
        sequences_sha256=str(manifest["sequences_parquet_sha256"]),
        model_revision=str(manifest["model_revision"]),
        software=dict(manifest["software"]),
        card=str(manifest["card"]),
        token_budgets=list(manifest["token_budgets"]),
        manifest=manifest,
        manifest_uri=uri,
        manifest_sha256=manifest_sha256,
        status="loading",
        short_name=short_name,
        description=description,
        code_revision=code_revision(),
        builder_revision=manifest.get("revision"),
        job_id=job_id_from_payload(payload),
    )


class LoadEmbeddingBankOperation:
    """Write one verified embedding bank of a set of photos (ADR-D62)."""

    name = "load_embedding_bank"
    description = (
        "Verify the embedding bank of a set of photos computed away from the server (its "
        "manifest, its configuration, its photos, its shards, its population) and load it "
        "into sequence_embedding, shard by shard (ADR-D62)."
    )

    def summarize_payload(self, payload: dict[str, Any]) -> str:
        digest = str(payload.get("manifest_sha256", ""))[:12]
        return f"bank {digest} <- {payload.get('bank_dir', '')}" if digest else ""

    def _verified_or_resumed(
        self, session: Session, p: LoadEmbeddingBankPayload, payload: dict[str, Any]
    ) -> tuple[EmbeddingBank, dict[str, Any]]:
        bank_dir = Path(p.bank_dir)
        manifest = read_bank_manifest(bank_dir, p.config_id, p.manifest_sha256)
        existing = session.execute(
            select(EmbeddingBank).where(
                EmbeddingBank.embedding_config_id == p.config_id,
                EmbeddingBank.manifest_sha256 == p.manifest_sha256,
            )
        ).scalar_one_or_none()
        if existing is not None and existing.status == "loaded":
            return existing, manifest
        checked_config(session, manifest)
        photos = photos_of(session, manifest)
        check_shards(bank_dir, manifest)
        if existing is not None:
            return existing, manifest
        check_population(session, bank_dir, manifest, photos)
        short_name, _ = descriptors(manifest)
        taken = session.execute(
            select(EmbeddingBank.id).where(EmbeddingBank.short_name == short_name)
        ).scalar_one_or_none()
        if taken is not None:
            raise EmbeddingBankRefused(f"another bank is already called {short_name}")
        uri = store_manifest(bank_dir, p.config_id, p.manifest_sha256)
        bank = _new_bank(manifest, p.manifest_sha256, uri, payload)
        session.add(bank)
        session.commit()
        return bank, manifest

    def execute(
        self, session: Session, payload: dict[str, Any], *, emit: EmitFn
    ) -> OperationResult:
        p = LoadEmbeddingBankPayload.model_validate(contract_payload(payload))
        bank, manifest = self._verified_or_resumed(session, p, payload)
        if bank.status == "loaded":
            emit("load_embedding_bank.already_loaded", None, {"bank": bank.short_name}, "info")
            return OperationResult(result={"bank_id": str(bank.id), "skipped": True})
        for shard in manifest["shards"][bank.shards_loaded :]:
            path = shard_path(Path(p.bank_dir), p.config_id, int(shard["shard"]))
            inserted, present = load_shard(session, path, bank, int(shard["rows"]))
            bank.shards_loaded += 1
            bank.rows_inserted += inserted
            bank.rows_present += present
            session.commit()
            emit(
                "load_embedding_bank.shard",
                None,
                {"shard": shard["shard"], "inserted": inserted, "present": present},
                "info",
            )
        bank.status = "loaded"
        bank.loaded_at = datetime.now(UTC)
        session.flush()
        result = {
            "bank_id": str(bank.id),
            "short_name": bank.short_name,
            "sequences": bank.sequences,
            "rows_inserted": bank.rows_inserted,
            "rows_present": bank.rows_present,
            "manifest_uri": bank.manifest_uri,
        }
        emit("load_embedding_bank.done", None, result, "info")
        return OperationResult(result=result)


__all__ = [
    "EmbeddingBankRefused",
    "LoadEmbeddingBankOperation",
    "LoadEmbeddingBankPayload",
]
