"""The lexicon bank's loader (ADR-D63): what it verifies, what it writes, what it refuses.

A photo is built and loaded the way ``tests/test_load_cut_photo.py`` does it; its
population is prepared by the embedding bank's pass and decoded by the lexicon bank's
file stage with a stand-in decoder (``tests/test_lexicon_bank_build.py``); then the
loader runs against Postgres.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from protea.core.embedding_bank.build import prepare_pass
from protea.core.lexicon_bank.build import decode_pass, manifest_pass
from protea.core.operations import load_lexicon_bank as loader
from protea.core.operations.load_lexicon_bank import LexiconBankRefused, LoadLexiconBankOperation
from protea.infrastructure.orm.models.embedding.lexicon_bank import LexiconBank
from protea.infrastructure.orm.models.embedding.model_weights import ModelWeights
from tests.test_lexicon_bank_build import fake_decoder  # noqa: F401
from tests.test_load_cut_photo import _load, build_photo, pg_session  # noqa: F401

NAME = "lexico-test"


@pytest.fixture()
def db(pg_session: Session, monkeypatch: pytest.MonkeyPatch) -> Session:  # noqa: F811
    """A fresh schema with the graph and the Swiss-Prot release of cut 211 loaded."""

    def store(root: Path, name: str, sha: str, shards: list[dict[str, Any]]) -> str:
        for shard in shards:
            shard["uri"] = f"memory://lexicon-banks/{name}/{sha}/{shard['pass']}/{shard['shard']}"
        return f"memory://lexicon-banks/{name}/{sha}/lexicon-bank.json"

    monkeypatch.setattr(loader, "store_bank", store)
    return pg_session


@pytest.fixture()
def decoder(fake_decoder: None) -> None:  # noqa: F811
    """The stand-in decoder of the lexicon bank's own tests."""


def _bank(tmp_path: Path, photo_dir: Path, parity_sha: str | None = None) -> tuple[Path, str, str]:
    """The photo's lexicon bank decoded by the stand-in; its directory, manifest sha256 and lexicon sha256."""
    bank = tmp_path / "bank"
    prepare_pass([photo_dir], bank, 8)
    lexicon = tmp_path / "lexico.pt"
    lexicon.write_bytes(b"weights")
    sha = hashlib.sha256(b"weights").hexdigest()
    run = {"lexicon": str(lexicon), "lexicon_sha256": sha, "first": 0, "last": 999, "device": "cpu"}
    for name in ("cut", "mass"):
        decode_pass(bank, NAME, {**run, "pass": name, "run_id": f"job-{name}", "stop_after": None})
    parity = tmp_path / "parity.json"
    parity.write_text(
        json.dumps({"lexicon_sha256": parity_sha or sha, "cut": {"labels": 9, "proteins": 9}})
    )
    manifest_pass(bank, NAME, {"sha256": sha, "parity": parity}, "builder-revision")
    manifest = bank / "lexicon" / NAME / "lexicon-bank.json"
    return bank, hashlib.sha256(manifest.read_bytes()).hexdigest(), sha


def _weights(session: Session, sha: str, kind: str = "lexicon") -> str:
    row = ModelWeights(
        kind=kind,
        name=NAME,
        format="pt",
        uri=f"memory://w/{sha}",
        sha256=sha,
        size_bytes=7,
        meta={},
    )
    session.add(row)
    session.commit()
    return str(row.id)


def _run(session: Session, bank: Path, sha: str, weights: str) -> dict[str, Any]:
    payload = {
        "bank_dir": str(bank),
        "name": NAME,
        "model_weights_id": weights,
        "manifest_sha256": sha,
    }
    result = LoadLexiconBankOperation().execute(session, payload, emit=lambda *a, **k: None)
    session.commit()
    return result.result


def _loaded_bank(session: Session, tmp_path: Path, **kw: Any) -> tuple[Path, str, str]:
    photo_dir, photo_sha = build_photo(tmp_path)
    _load(session, photo_dir, photo_sha)
    return _bank(tmp_path, photo_dir, **kw)


@pytest.mark.integration
def test_a_bank_loads_and_declares_every_shard(db: Session, tmp_path: Path, decoder: None) -> None:
    bank, sha, lex_sha = _loaded_bank(db, tmp_path)
    result = _run(db, bank, sha, _weights(db, lex_sha))
    row = db.execute(select(LexiconBank)).scalar_one()
    shards = json.loads((bank / "shards.json").read_text())["shards"]
    assert result["shards"] == len(row.shards) == 2 * len(shards)
    assert {s["pass"] for s in row.shards} == {"cut", "mass"}
    assert all(s["uri"].startswith("memory://") for s in row.shards)
    assert row.short_name == "lexicon-lexico-test-cuts-211"
    assert row.parity["lexicon_sha256"] == lex_sha and row.passes["cut"]["card"] == "cpu"
    assert _run(db, bank, sha, str(row.model_weights_id))["skipped"] is True
    assert db.execute(select(func.count(LexiconBank.id))).scalar_one() == 1


@pytest.mark.integration
def test_weights_that_are_not_the_lexicon_are_refused(
    db: Session, tmp_path: Path, decoder: None
) -> None:
    bank, sha, lex_sha = _loaded_bank(db, tmp_path)
    with pytest.raises(LexiconBankRefused, match="not a registered lexicon"):
        _run(db, bank, sha, _weights(db, lex_sha, kind="residue_vocabulary"))
    db.rollback()
    with pytest.raises(LexiconBankRefused, match="was decoded with"):
        _run(db, bank, sha, _weights(db, "f" * 64))


@pytest.mark.integration
def test_a_parity_record_about_another_lexicon_is_refused(
    db: Session, tmp_path: Path, decoder: None
) -> None:
    bank, sha, lex_sha = _loaded_bank(db, tmp_path, parity_sha="e" * 64)
    with pytest.raises(LexiconBankRefused, match="parity record is not about this lexicon"):
        _run(db, bank, sha, _weights(db, lex_sha))


@pytest.mark.integration
def test_a_changed_shard_is_refused(db: Session, tmp_path: Path, decoder: None) -> None:
    bank, sha, lex_sha = _loaded_bank(db, tmp_path)
    shard = bank / "lexicon" / NAME / "mass" / "shard-00000.parquet"
    pq.write_table(pq.read_table(shard).slice(1), shard)
    with pytest.raises(LexiconBankRefused, match="does not hash as recorded"):
        _run(db, bank, sha, _weights(db, lex_sha))
    db.rollback()
    assert db.execute(select(func.count(LexiconBank.id))).scalar_one() == 0


@pytest.mark.integration
def test_a_photo_the_platform_has_not_loaded_is_refused(
    db: Session, tmp_path: Path, decoder: None
) -> None:
    photo_dir, _ = build_photo(tmp_path)
    bank, sha, lex_sha = _bank(tmp_path, photo_dir)
    with pytest.raises(LexiconBankRefused, match="no loaded photo"):
        _run(db, bank, sha, _weights(db, lex_sha))


_MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/e7c4a9b2d5f1_the_lexicon_bank_of_the_photos.py"
)


def test_the_migration_freezes_the_models_checks_and_creates_every_column() -> None:
    import importlib.util

    from protea.infrastructure.orm.models.annotation import cut_photo
    from protea.infrastructure.orm.models.embedding import lexicon_bank as model

    spec = importlib.util.spec_from_file_location("lexicon_bank_migration", _MIGRATION)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    for name in ("DIGESTS_CHECK", "COUNTS_CHECK"):
        assert getattr(migration, name) == getattr(model, name), name
    for name in ("SHORT_NAME_CHECK", "DESCRIPTIONS_CHECK"):
        assert getattr(migration, name) == getattr(cut_photo, name), name
    assert (migration.revision, migration.down_revision) == ("e7c4a9b2d5f1", "c3a9e7d1f5b2")
    source = _MIGRATION.read_text(encoding="utf-8")
    for column in LexiconBank.__table__.columns:
        assert f'"{column.name}"' in source, column.name
