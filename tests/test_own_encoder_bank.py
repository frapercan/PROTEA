"""The own segment encoder's cluster stage: its training, its vectors and its bank.

A small photo is written on disk the way the photo builder writes it, its population
prepared by the embedding bank's own pass, and a lexicon cut is written for it the way
the lexicon bank writes one (segments derived from each sequence's md5, one protein
with more than 64). The encoder then trains for a step or two on the CPU, writes its
vectors and its manifest, and the platform's loader checks them: with Postgres, it
loads them under the configuration the encoder's weights name.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq
import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from protea.core import own_encoder
from protea.core.embedding_bank.build import prepare_pass
from protea.core.embedding_identity import derive_embedding_config_id
from protea.core.file_stage import file_sha256, read_record, write_record
from protea.core.lexicon_bank import build as lexicon_bank
from protea.core.operations import load_embedding_bank as loader
from protea.core.own_encoder import train, vectors
from protea.core.own_encoder.corpus import CorpusRefused, checked_lexicon_cut, read_corpus
from protea.core.own_encoder.model import MAX_SEGMENTS, encoder_from_bytes
from tests.test_embedding_bank_build import _photo
from tests.test_load_cut_photo import pg_session  # noqa: F401  (the Postgres fixture)

LEXICON = "lexico-test"
ROOT = Path(__file__).resolve().parents[1]
ALPHABET = np.array(list("ACDEFGHIKLMNPQRSTVWY"))


def _md5(sequence: str) -> str:
    return hashlib.md5(sequence.encode(), usedforsecurity=False).hexdigest()


def _sequences() -> dict[str, str]:
    rng = np.random.default_rng(0)
    return {
        f"P{i:02d}": "".join(rng.choice(ALPHABET, size=int(rng.integers(80, 300))))
        for i in range(40)
    }


SEQUENCES = _sequences()
#: The protein whose cut has more segments than the encoder reads.
LONG = _md5(SEQUENCES["P07"])


def _segments(md5: str, length: int) -> list[tuple[int, int, int]]:
    """1 to 4 segments from the md5 (70 for the long protein), tiling the sequence."""
    n = 70 if md5 == LONG else 1 + int(md5[:2], 16) % 4
    edges = np.linspace(0, length, n + 1).astype(int)
    themes = [int(md5[8 + (k % 12) : 12 + (k % 12)], 16) % 32_768 for k in range(n)]
    return [(int(edges[k]), int(edges[k + 1]), themes[k]) for k in range(n)]


def _bank_rows(bank: Path, shard: int) -> tuple[list[str], list[str]]:
    table = pq.ParquetFile(bank / "sequences.parquet").read_row_group(shard)
    return table.column("md5").to_pylist(), table.column("sequence").to_pylist()


def write_lexicon_cut(bank: Path, segments: Any = _segments) -> Path:
    """A complete lexicon bank's cut of the bank's population, one shard per bank shard."""
    folder = bank / "lexicon" / LEXICON
    (folder / "cut").mkdir(parents=True, exist_ok=True)
    records = []
    for shard in read_record(bank / "shards.json")["shards"]:
        md5s, sequences = _bank_rows(bank, int(shard["shard"]))
        cut = [
            {"best_bits": 1.0, "segments": segments(m, len(s)), "columns": [0] * len(s)}
            for m, s in zip(md5s, sequences, strict=True)
        ]
        table = lexicon_bank.cut_table(md5s, cut)
        records.append(
            lexicon_bank.write_shard(folder / "cut", int(shard["shard"]), table, {"run": "test"})
        )
    write_record(
        folder / "lexicon-bank.json",
        {
            "table": "lexicon_bank",
            "name": LEXICON,
            "lexicon_sha256": "e" * 64,
            "status": "complete",
            "passes": {
                "cut": {"shards": [{k: r[k] for k in ("shard", "rows", "sha256")} for r in records]}
            },
        },
    )
    return folder


def _bank(root: Path) -> Path:
    spec = {
        "sequences": {a: s for a, s in SEQUENCES.items() if int(a[1:]) < 36},
        "unreviewed_sequences": {a: s for a, s in SEQUENCES.items() if int(a[1:]) >= 36},
        "members": sorted(SEQUENCES),
    }
    photo = _photo(root, 211, spec)
    bank = root / "bank"
    prepare_pass([photo], bank, 2_000)
    write_lexicon_cut(bank)
    return bank


def _members(root: Path, accessions: list[str]) -> Path:
    path = root / "members.txt"
    path.write_text("".join(f"{_md5(SEQUENCES[a])}\n" for a in accessions), encoding="ascii")
    return path


def _run(root: Path, bank: Path, out: str, **settings: Any) -> train.TrainRun:
    fields = {
        "bank": bank,
        "lexicon": LEXICON,
        "members": _members(root, [a for a in SEQUENCES if a != "P39"]),
        "out": root / out,
        "plan_minutes": 10.0,
        "wall_minutes": 5.0,
        "device": "cpu",
        "tokens_per_batch": 256,
        "revision": "test-revision",
        "run_id": "job-1",
    }
    return train.TrainRun(**{**fields, **settings})


def _quiet(*_: Any) -> None:
    return None


@pytest.fixture(scope="module")
def trained(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path, dict[str, Any]]:
    """A bank with its lexicon cut and an encoder whose plan is spent: (bank, encoder, record)."""
    root = tmp_path_factory.mktemp("own-encoder")
    bank = _bank(root)
    run = _run(root, bank, "encoder", plan_minutes=1e-6)
    return bank, run.out, train.train_pass(run, _quiet)


# ---------------------------------------------------------------- training


def test_a_complete_training_names_its_weights_and_declares_its_population(
    trained: tuple[Path, Path, dict[str, Any]],
) -> None:
    bank, out, record = trained
    sha = file_sha256(out / "encoder.npz")
    assert record["status"] == "complete" and record["progress"]["complete"]
    assert record["encoder"]["sha256"] == sha and record["family"] == "world-model"
    assert record["model_name"] == f"jepa-segments/{sha}"
    assert record["configuration_id"] == str(own_encoder.configuration_id(sha))
    members = (out.parent / "members.txt").read_bytes()
    assert record["members"] == {
        "file": "members.txt",
        "sha256": hashlib.sha256(members).hexdigest(),
        "md5s": 39,
    }
    assert record["corpus"]["proteins"] == 39 and record["corpus"]["over_max_segments"] == 1
    assert record["corpus"]["pool"] + record["corpus"]["held_out"] <= 39
    assert (
        record["lexicon"]["bank_manifest_sha256"]
        == checked_lexicon_cut(bank, LEXICON).manifest_sha256
    )
    assert record["held_out"][0]["step"] == 0 and len(record["held_out"]) == 2
    assert [json.loads(line)["kind"] for line in (out / "metrics.jsonl").read_text().splitlines()][
        -1
    ] == "done"


def test_a_measured_first_job_trains_records_its_curve_and_gives_no_bank(tmp_path: Path) -> None:
    bank = _bank(tmp_path)
    run = _run(tmp_path, bank, "sample", measurement=True, max_steps=3, eval_minutes=0.0)
    record = train.train_pass(run, _quiet)
    assert record["status"] == "measurement" and record["progress"]["steps"] == 3
    assert [point["step"] for point in record["held_out"]] == [0, 1, 2, 3]
    assert record["progress"]["tokens"] > 0 and record["progress"]["tokens_per_second"] > 0
    assert record["encoder"]["sha256"] != record["initial_encoder"]["sha256"]
    with pytest.raises(CorpusRefused, match="measurement"):
        vectors.checked_encoder(run.out, record["encoder"]["sha256"])


def test_a_stopped_job_resumes_where_it_stopped(tmp_path: Path) -> None:
    bank = _bank(tmp_path)
    first = train.train_pass(_run(tmp_path, bank, "encoder", max_steps=1), _quiet)
    assert first["status"] == "stopped" and first["progress"]["steps"] == 1
    again = train.train_pass(_run(tmp_path, bank, "encoder", max_steps=2), _quiet)
    assert again["progress"]["steps"] == 2 and len(again["held_out"]) == 3
    assert again["initial_encoder"] == first["initial_encoder"]
    with pytest.raises(CorpusRefused, match="keep its plan"):
        train.train_pass(_run(tmp_path, bank, "encoder", plan_minutes=11.0), _quiet)


def test_the_plan_warms_up_then_falls_by_a_cosine_and_the_momentum_rises() -> None:
    assert train.schedule(5e-4, 0.0) == (0.0, pytest.approx(0.996))
    assert train.schedule(5e-4, 0.03)[0] == pytest.approx(5e-4)
    rate, momentum = train.schedule(5e-4, 1.0)
    assert rate == pytest.approx(5e-6) and momentum == pytest.approx(1.0)


def test_the_held_out_proteins_are_drawn_once_and_one_segment_proteins_never_train(
    tmp_path: Path,
) -> None:
    bank = _bank(tmp_path)
    cut = checked_lexicon_cut(bank, LEXICON)
    corpus = read_corpus(cut, sorted(cut.shards))
    pool, held = train.split(corpus)
    counts = corpus.counts()
    assert set(pool).isdisjoint(held) and (counts[pool] >= 2).all() and (counts[held] >= 2).all()
    assert np.array_equal(train.split(corpus)[1], held)


def test_members_the_bank_does_not_hold_are_refused(tmp_path: Path) -> None:
    bank = _bank(tmp_path)
    run = _run(tmp_path, bank, "encoder")
    run.members.write_text("0" * 32 + "\n", encoding="ascii")
    with pytest.raises(CorpusRefused, match="no cut in the lexicon bank"):
        train.train_pass(run, _quiet)
    run.members.write_text("not an md5\n", encoding="ascii")
    with pytest.raises(CorpusRefused, match="not a list of sequence md5s"):
        train.train_pass(run, _quiet)


def test_a_cut_shard_that_changed_is_refused(tmp_path: Path) -> None:
    bank = _bank(tmp_path)
    path = bank / "lexicon" / LEXICON / "cut" / "shard-00000.parquet"
    pq.write_table(pq.read_table(path).slice(1), path)
    with pytest.raises(CorpusRefused, match="does not hash as recorded"):
        train.train_pass(_run(tmp_path, bank, "encoder"), _quiet)


# ---------------------------------------------------------------- vectors and the bank


def _vectors_run(
    bank: Path, out: Path, sha: str, first: int = 0, last: int = 999
) -> vectors.VectorsRun:
    return vectors.VectorsRun(
        bank=bank,
        lexicon=LEXICON,
        encoder=out,
        encoder_sha256=sha,
        first=first,
        last=last,
        device="cpu",
    )


def test_the_vectors_are_a_bank_the_loader_reads(
    trained: tuple[Path, Path, dict[str, Any]],
) -> None:
    bank, out, record = trained
    sha = record["encoder"]["sha256"]
    done = vectors.vectors_pass(_vectors_run(bank, out, sha))
    manifest = vectors.manifest_pass(bank, LEXICON, out, "builder-revision")
    config = own_encoder.configuration_id(sha)
    assert manifest["status"] == "complete", manifest["refusals"]
    assert done["config_id"] == manifest["config_id"] == str(config)
    assert derive_embedding_config_id(manifest["recipe"]) == config
    assert (
        manifest["model_revision"] == sha and manifest["card"] == "cpu" and manifest["dim"] == 256
    )
    assert manifest["own_encoder"]["read_on_their_first_segments"] == 1
    assert manifest["own_encoder"]["segments_read"] == MAX_SEGMENTS
    sha_manifest = file_sha256(bank / f"bank-{config}.json")
    assert loader.read_bank_manifest(bank, config, sha_manifest)["sequences"] == 40
    loader.check_shards(bank, manifest)
    short, long = loader.descriptors(manifest)
    assert short == f"bank-jepa-segments-{sha[:12]}-cuts-211"
    assert "world-model" in long and "lexico-test" in long and "first 64 segments" in long


def test_a_vector_is_the_target_encoders_mean_over_the_first_segments(
    trained: tuple[Path, Path, dict[str, Any]],
) -> None:
    bank, out, record = trained
    sha = record["encoder"]["sha256"]
    vectors.vectors_pass(_vectors_run(bank, out, sha))
    encoder = encoder_from_bytes((out / "encoder.npz").read_bytes())
    cut = checked_lexicon_cut(bank, LEXICON)
    folder = bank / "vectors" / str(own_encoder.configuration_id(sha))
    for shard in sorted(cut.shards):
        corpus = read_corpus(cut, [shard])
        table = pq.read_table(folder / f"shard-{shard:05d}.parquet")
        assert table.column("md5").to_pylist() == corpus.md5
        stored = table.column("vector").combine_chunks().flatten().to_numpy()
        stored = stored.reshape(table.num_rows, -1)
        assert np.array_equal(stored, vectors.encode(encoder, corpus, "cpu").astype(np.float16))
        # Another batching changes nothing but the rounding of the sums.
        one_by_one = vectors.encode(encoder, corpus, "cpu", budget=1).astype(np.float32)
        assert np.allclose(stored.astype(np.float32), one_by_one, atol=2e-2, rtol=1e-2)
    long = read_corpus(cut, sorted(cut.shards), {LONG})
    first = (long.start[:MAX_SEGMENTS], long.end[:MAX_SEGMENTS], long.theme[:MAX_SEGMENTS])
    alone = vectors.Corpus(long.md5, np.array([0, MAX_SEGMENTS]), *first)
    assert len(long.theme) == 70
    assert np.array_equal(
        vectors.encode(encoder, long, "cpu"), vectors.encode(encoder, alone, "cpu")
    )


def test_a_pilot_shard_leaves_the_manifest_for_the_last_job(tmp_path: Path) -> None:
    bank = _bank(tmp_path)
    run = _run(tmp_path, bank, "encoder", plan_minutes=1e-6)
    sha = train.train_pass(run, _quiet)["encoder"]["sha256"]
    args = ["--bank", str(bank), "--lexicon", LEXICON, "--encoder", str(run.out)]
    args += ["--encoder-sha256", sha, "--device", "cpu", "--run-id", "job-2", "--revision", "r"]
    config = own_encoder.configuration_id(sha)
    assert vectors.main([*args, "--first", "0", "--last", "0"]) == 0
    assert not (bank / f"bank-{config}.json").exists()
    assert vectors.main([*args, "--first", "0", "--last", "999"]) == 0
    assert read_record(bank / f"bank-{config}.json")["status"] == "complete"
    wrong = [*args[:6], "--encoder-sha256", "0" * 64, *args[8:], "--first", "0", "--last", "0"]
    assert vectors.main(wrong) == 2


def test_a_cut_that_is_not_its_bank_shard_is_refused(
    trained: tuple[Path, Path, dict[str, Any]], tmp_path: Path
) -> None:
    _, out, record = trained
    bank = _bank(tmp_path)
    folder = bank / "lexicon" / LEXICON
    path = folder / "cut" / "shard-00000.parquet"
    table = pq.read_table(path)
    pq.write_table(table.take(list(reversed(range(table.num_rows)))), path)
    manifest = read_record(folder / "lexicon-bank.json")
    manifest["passes"]["cut"]["shards"][0]["sha256"] = file_sha256(path)
    write_record(folder / "lexicon-bank.json", manifest)
    with pytest.raises(CorpusRefused, match="does not hold bank shard 0"):
        vectors.vectors_pass(_vectors_run(bank, out, record["encoder"]["sha256"], 0, 0))


def test_a_sequence_without_segments_refuses_the_manifest(
    trained: tuple[Path, Path, dict[str, Any]], tmp_path: Path
) -> None:
    _, out, record = trained
    bank = _bank(tmp_path)
    empty = _md5(SEQUENCES["P03"])

    def no_segment_for_one(md5: str, length: int) -> list[tuple[int, int, int]]:
        return [] if md5 == empty else _segments(md5, length)

    write_lexicon_cut(bank, no_segment_for_one)
    sha = record["encoder"]["sha256"]
    vectors.vectors_pass(_vectors_run(bank, out, sha))
    manifest = vectors.manifest_pass(bank, LEXICON, out, "builder-revision")
    assert manifest["status"] == "refused"
    assert any("1 sequences without a segment" in r for r in manifest["refusals"])


def _stage(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(ROOT / "deploy" / "cluster" / "own_encoder_bank.sh"), *args],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


def test_the_stage_refuses_an_unknown_mode_and_a_bank_it_cannot_read(
    trained: tuple[Path, Path, dict[str, Any]], tmp_path: Path
) -> None:
    bank, out, _ = trained
    done = _stage("python3", str(ROOT), str(bank), LEXICON, str(out), "rev", "nothing")
    assert done.returncode == 2 and "REFUSED: unknown mode nothing" in done.stdout
    done = _stage("python3", str(ROOT), str(tmp_path), LEXICON, str(out), "rev", "vectors")
    assert done.returncode == 1 and "MISSING" in done.stdout


# ---------------------------------------------------------------- the platform loads it


@pytest.mark.integration
def test_the_platform_loads_the_bank_under_the_encoders_configuration(
    pg_session: Session,  # noqa: F811
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from protea.infrastructure.orm.models.embedding.embedding_bank import EmbeddingBank
    from protea.infrastructure.orm.models.embedding.embedding_config import EmbeddingConfig
    from protea.infrastructure.orm.models.embedding.sequence_embedding import SequenceEmbedding
    from tests.test_load_cut_photo import _load, build_photo

    monkeypatch.setattr(
        loader, "store_manifest", lambda _d, cfg, sha: f"memory://{loader.bank_key_for(cfg, sha)}"
    )
    photo, photo_sha = build_photo(tmp_path)
    _load(pg_session, photo, photo_sha)
    bank = tmp_path / "bank"
    prepare_pass([photo], bank, 8)
    write_lexicon_cut(bank, _two_or_more)
    members = tmp_path / "members.txt"
    members.write_text("".join(f"{m}\n" for m in _all_md5s(bank)), encoding="ascii")
    run = train.TrainRun(
        bank=bank,
        lexicon=LEXICON,
        members=members,
        out=tmp_path / "encoder",
        plan_minutes=1e-6,
        wall_minutes=5.0,
        device="cpu",
        tokens_per_batch=256,
        revision="r",
        run_id="job-1",
    )
    record = train.train_pass(run, _quiet)
    sha = record["encoder"]["sha256"]
    vectors.vectors_pass(_vectors_run(bank, run.out, sha))
    manifest = vectors.manifest_pass(bank, LEXICON, run.out, "builder-revision")
    body = own_encoder.configuration(sha, record)
    pg_session.add(EmbeddingConfig(**{k: v for k, v in body.items()}))
    pg_session.commit()
    config = own_encoder.configuration_id(sha)
    payload = {
        "bank_dir": str(bank),
        "embedding_config_id": str(config),
        "manifest_sha256": file_sha256(bank / f"bank-{config}.json"),
    }
    result = loader.LoadEmbeddingBankOperation().execute(
        pg_session, payload, emit=lambda *a, **k: None
    )
    pg_session.commit()
    row = pg_session.execute(select(EmbeddingBank)).scalar_one()
    assert row.status == "loaded" and row.embedding_config_id == config
    assert row.short_name == f"bank-jepa-segments-{sha[:12]}-cuts-211"
    assert result.result["rows_inserted"] == manifest["sequences"] == row.sequences
    count = pg_session.execute(
        select(func.count(SequenceEmbedding.id)).where(
            SequenceEmbedding.embedding_config_id == config
        )
    ).scalar_one()
    assert count == row.sequences


def _all_md5s(bank: Path) -> list[str]:
    return pq.read_table(bank / "sequences.parquet", columns=["md5"]).column("md5").to_pylist()


def _two_or_more(md5: str, length: int) -> list[tuple[int, int, int]]:
    """Two segments or more for every protein, so the photo's few proteins all train."""
    return _segments(md5, length) + [(length, length + 1, 7)]
