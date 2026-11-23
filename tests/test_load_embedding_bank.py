"""The embedding bank's loader (ADR-D62): what it verifies, what it writes, what it refuses.

A photo is built and loaded the way ``tests/test_load_cut_photo.py`` does it; its bank
is then built by the real file stage over that photo's directory, with the plugin
replaced by one whose vector of a sequence depends on that sequence alone, and loaded
into Postgres.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq
import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from protea.core.embedding_bank import build
from protea.core.embedding_bank.build import embed_pass, manifest_pass, prepare_pass
from protea.core.operations import load_embedding_bank as loader
from protea.core.operations.load_embedding_bank import (
    EmbeddingBankRefused,
    LoadEmbeddingBankOperation,
    bank_key_for,
)
from protea.infrastructure.orm.models.embedding.embedding_bank import EmbeddingBank
from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
from protea.infrastructure.orm.models.sequence.sequence import Sequence
from tests.test_embedding_bank_build import CONFIG_ID, RECIPE, _Backend, _vector
from tests.test_load_cut_photo import _load, build_photo, pg_session  # noqa: F401


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _md5(sequence: str) -> str:
    return hashlib.md5(sequence.encode(), usedforsecurity=False).hexdigest()


def _as_half(stored: Any) -> np.ndarray:
    """A halfvec as read back: pgvector 0.4 hands a HalfVector, 0.5 a plain list."""
    values = stored.to_numpy() if hasattr(stored, "to_numpy") else stored
    return np.asarray(values, dtype=np.float16)


@pytest.fixture()
def db(pg_session: Session) -> Session:  # noqa: F811
    """A fresh schema with the graph and the Swiss-Prot release of cut 211 loaded."""
    return pg_session


@pytest.fixture()
def backend(monkeypatch: pytest.MonkeyPatch) -> _Backend:
    plugin = _Backend()
    monkeypatch.setattr(build, "discover_plugins", lambda group: {"fake": plugin})
    monkeypatch.setattr(
        loader, "store_manifest", lambda _dir, cfg, sha: f"memory://{bank_key_for(cfg, sha)}"
    )
    return plugin


def _bank(tmp_path: Path, photo_dir: Path) -> Path:
    """The photo's bank, built by the file stage in small shards; manifest not written."""
    bank = tmp_path / "bank"
    prepare_pass([photo_dir], bank, 8)
    configs = {"configs": [{"id": CONFIG_ID, "recipe": RECIPE}]}
    (bank / "recipes.json").write_text(json.dumps(configs), encoding="utf-8")
    run = {"first": 0, "last": 999, "token_budget": 64, "device": "cpu", "run_id": "job-1"}
    embed_pass(bank, CONFIG_ID, {**run, "stop_after": None})
    return bank


def _manifest(bank: Path) -> str:
    manifest_pass(bank, CONFIG_ID, "builder-revision")
    return _sha256(bank / f"bank-{CONFIG_ID}.json")


def _with_config(session: Session) -> None:
    session.add(EmbeddingConfig(**RECIPE))
    session.commit()


def _run(session: Session, bank: Path, sha: str) -> dict[str, Any]:
    payload = {"bank_dir": str(bank), "embedding_config_id": CONFIG_ID, "manifest_sha256": sha}
    result = LoadEmbeddingBankOperation().execute(session, payload, emit=lambda *a, **k: None)
    session.commit()
    return result.result


def _loaded_photo_and_bank(session: Session, tmp_path: Path) -> tuple[Path, str]:
    photo_dir, photo_sha = build_photo(tmp_path)
    _load(session, photo_dir, photo_sha)
    _with_config(session)
    bank = _bank(tmp_path, photo_dir)
    return bank, _manifest(bank)


def _tamper(bank: Path, shard: int, extra: list[tuple[str, np.ndarray]]) -> None:
    """Append rows to a shard and keep every record consistent, so only the loader can tell."""
    out = bank / "vectors" / CONFIG_ID
    table = pq.read_table(out / f"shard-{shard:05d}.parquet")
    md5s = table.column("md5").to_pylist() + [m for m, _ in extra]
    flat = table.column("vector").combine_chunks().flatten().to_numpy()
    vectors = np.vstack([flat.reshape(table.num_rows, -1), *[v[None, :] for _, v in extra]])
    path = build.write_shard(out, shard, md5s, vectors.astype(np.float16))
    record = json.loads((out / f"shard-{shard:05d}.json").read_text())
    record.update(rows=len(md5s), sha256=_sha256(path))
    (out / f"shard-{shard:05d}.json").write_text(json.dumps(record))
    shards = json.loads((bank / "shards.json").read_text())
    shards["shards"][shard]["rows"] += len(extra)
    (bank / "shards.json").write_text(json.dumps(shards))
    prepare = json.loads((bank / "prepare.json").read_text())
    prepare["sequences"] += len(extra)
    (bank / "prepare.json").write_text(json.dumps(prepare))


class TestALoadedBank:
    def test_every_member_sequence_gets_its_vector_naming_the_bank(
        self, db: Session, tmp_path: Path, backend: _Backend
    ) -> None:
        bank, sha = _loaded_photo_and_bank(db, tmp_path)
        result = _run(db, bank, sha)
        manifest = json.loads((bank / f"bank-{CONFIG_ID}.json").read_text())
        row = db.execute(select(EmbeddingBank)).scalar_one()
        assert row.status == "loaded" and row.loaded_at is not None
        assert row.sequences == manifest["sequences"] == result["rows_inserted"]
        assert row.rows_present == 0 and row.shards_loaded == row.shards > 1
        assert row.short_name == "bank-fake-model-cuts-211"
        assert row.model_revision == "c0ffee" and row.card == "cpu"
        assert row.manifest_uri == f"memory://{bank_key_for(row.embedding_config_id, sha)}"
        stored = db.execute(
            select(
                Sequence.sequence, SequenceEmbedding.embedding, SequenceEmbedding.embedding_bank_id
            ).join(Sequence, Sequence.id == SequenceEmbedding.sequence_id)
        ).all()
        assert len(stored) == row.sequences
        for sequence, embedding, bank_id in stored:
            assert bank_id == row.id
            expected = _vector(sequence).astype(np.float16)
            assert np.array_equal(_as_half(embedding), expected)

    def test_the_same_manifest_again_is_a_no_op(
        self, db: Session, tmp_path: Path, backend: _Backend
    ) -> None:
        bank, sha = _loaded_photo_and_bank(db, tmp_path)
        _run(db, bank, sha)
        assert _run(db, bank, sha)["skipped"] is True
        assert db.execute(select(func.count(EmbeddingBank.id))).scalar_one() == 1

    def test_a_row_already_there_is_kept_and_counted(
        self, db: Session, tmp_path: Path, backend: _Backend
    ) -> None:
        bank, sha = _loaded_photo_and_bank(db, tmp_path)
        first = db.execute(select(Sequence).order_by(Sequence.id)).scalars().first()
        assert first is not None
        mine = np.full(4, 0.5, dtype=np.float16)
        db.add(
            SequenceEmbedding(
                sequence_id=first.id,
                embedding_config_id=db.execute(select(EmbeddingConfig.id)).scalar_one(),
                chunk_index_s=0,
                embedding=mine,
                embedding_dim=4,
            )
        )
        db.commit()
        result = _run(db, bank, sha)
        assert result["rows_present"] == 1
        kept = db.execute(
            select(SequenceEmbedding).where(SequenceEmbedding.sequence_id == first.id)
        ).scalar_one()
        assert kept.embedding_bank_id is None
        assert np.array_equal(_as_half(kept.embedding), mine)

    def test_an_interrupted_load_resumes_at_the_first_shard_not_loaded(
        self,
        db: Session,
        tmp_path: Path,
        backend: _Backend,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        bank, sha = _loaded_photo_and_bank(db, tmp_path)
        real = loader.load_shard
        calls = {"n": 0}

        def once_then_fail(*args: Any) -> tuple[int, int]:
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("the worker died")
            return real(*args)

        monkeypatch.setattr(loader, "load_shard", once_then_fail)
        with pytest.raises(RuntimeError, match="died"):
            _run(db, bank, sha)
        db.rollback()
        row = db.execute(select(EmbeddingBank)).scalar_one()
        assert (row.status, row.shards_loaded) == ("loading", 1)
        monkeypatch.setattr(loader, "load_shard", real)
        result = _run(db, bank, sha)
        assert result["rows_inserted"] == row.sequences
        count = db.execute(select(func.count(SequenceEmbedding.id))).scalar_one()
        assert count == row.sequences


class TestARefusedBank:
    def _refused(self, session: Session, bank: Path, sha: str, message: str) -> None:
        with pytest.raises(EmbeddingBankRefused, match=message):
            _run(session, bank, sha)
        session.rollback()
        assert session.execute(select(func.count(EmbeddingBank.id))).scalar_one() == 0
        assert session.execute(select(func.count(SequenceEmbedding.id))).scalar_one() == 0

    def test_a_manifest_that_does_not_hash_as_named(
        self, db: Session, tmp_path: Path, backend: _Backend
    ) -> None:
        bank, _ = _loaded_photo_and_bank(db, tmp_path)
        self._refused(db, bank, "0" * 64, "hashes to")

    def test_a_bank_whose_manifest_was_refused(
        self, db: Session, tmp_path: Path, backend: _Backend
    ) -> None:
        bank, _ = _loaded_photo_and_bank(db, tmp_path)
        (bank / "vectors" / CONFIG_ID / "shard-00000.json").unlink()
        self._refused(db, bank, _manifest(bank), "only a complete bank loads")

    def test_a_shard_that_changed_after_its_manifest(
        self, db: Session, tmp_path: Path, backend: _Backend
    ) -> None:
        bank, sha = _loaded_photo_and_bank(db, tmp_path)
        out = bank / "vectors" / CONFIG_ID
        table = pq.read_table(out / "shard-00000.parquet")
        pq.write_table(table.slice(0, max(1, table.num_rows - 1)), out / "shard-00000.parquet")
        self._refused(db, bank, sha, "does not hash as recorded")

    def test_a_configuration_the_platform_does_not_hold(
        self, db: Session, tmp_path: Path, backend: _Backend
    ) -> None:
        photo_dir, photo_sha = build_photo(tmp_path)
        _load(db, photo_dir, photo_sha)
        bank = _bank(tmp_path, photo_dir)
        self._refused(db, bank, _manifest(bank), "no stored embedding configuration")

    def test_a_photo_the_platform_has_not_loaded(
        self, db: Session, tmp_path: Path, backend: _Backend
    ) -> None:
        photo_dir, _ = build_photo(tmp_path)
        _with_config(db)
        bank = _bank(tmp_path, photo_dir)
        self._refused(db, bank, _manifest(bank), "no loaded photo has manifest")

    def test_a_sequence_that_is_no_members(
        self, db: Session, tmp_path: Path, backend: _Backend
    ) -> None:
        bank, _ = _loaded_photo_and_bank(db, tmp_path)
        _tamper(bank, 0, [(_md5("MXXXXW"), _vector("MXXXXW").astype(np.float16))])
        self._refused(db, bank, _manifest(bank), "is not the photos' members")

    def test_a_sequence_twice(self, db: Session, tmp_path: Path, backend: _Backend) -> None:
        bank, _ = _loaded_photo_and_bank(db, tmp_path)
        table = pq.read_table(bank / "vectors" / CONFIG_ID / "shard-00000.parquet")
        md5 = table.column("md5").to_pylist()[0]
        _tamper(bank, 1, [(md5, np.zeros(4, dtype=np.float16))])
        self._refused(db, bank, _manifest(bank), "appears twice")


_MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/b8e3f1c6d2a4_the_embedding_bank_of_the_photos.py"
)


class TestTheSchema:
    def test_the_migration_freezes_the_models_checks_and_adds_every_column(self) -> None:
        import importlib.util

        from protea.infrastructure.orm.models.annotation import cut_photo
        from protea.infrastructure.orm.models.embedding import embedding_bank as model

        spec = importlib.util.spec_from_file_location("bank_migration", _MIGRATION)
        assert spec is not None and spec.loader is not None
        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)
        for name in ("DIGESTS_CHECK", "COUNTS_CHECK", "STATUS_CHECK"):
            assert getattr(migration, name) == getattr(model, name), name
        for name in ("SHORT_NAME_CHECK", "DESCRIPTIONS_CHECK"):
            assert getattr(migration, name) == getattr(cut_photo, name), name
        assert (migration.revision, migration.down_revision) == ("b8e3f1c6d2a4", "d2f6a8c4e0b7")
        source = _MIGRATION.read_text(encoding="utf-8")
        for column in EmbeddingBank.__table__.columns:
            assert f'"{column.name}"' in source, column.name
        assert '"embedding_bank_id"' in source
        assert SequenceEmbedding.__table__.columns["embedding_bank_id"].nullable

    def test_the_descriptors_name_the_model_and_every_cut(self) -> None:
        manifest = {
            "recipe": {**RECIPE, "model_name": "Rostlab/prot_t5_xl_half_uniref50-enc"},
            "photos": [{"cut_release": c} for c in (227, 160, 177, 194, 205, 211)],
            "sequences": 2523987,
            "dim": 1024,
            "card": "NVIDIA B200",
            "software": {"protea-backends_commit": "2e9202bf980a"},
            "token_budgets": [65536],
            "model_revision": "94a6abc029ae",
        }
        short, long = loader.descriptors(manifest)
        assert short == "bank-prot-t5-xl-half-uniref50-enc-cuts-160-177-194-205-211-227"
        assert len(short) <= 64
        assert "2,523,987" in long and "NVIDIA B200" in long and "160, 177, 194" in long
