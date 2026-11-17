"""The lexicon bank's file stage (ADR-D63): what it writes, resumes and refuses.

The population is prepared by the embedding bank's own pass over two small photos,
and the decoder is replaced by one whose cut and theme mass of a sequence depend on
that sequence alone, so the stage runs here without a card. The decoder itself is
tested in ``tests/test_lexicon_decoder.py``.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq
import pytest

from protea.core.embedding_bank.build import prepare_pass
from protea.core.lexicon import decode, model
from protea.core.lexicon_bank import build
from protea.core.lexicon_bank.build import decode_pass, lexicon_dir, manifest_pass
from tests.test_embedding_bank_build import _photos


@pytest.fixture
def fake_decoder(monkeypatch: pytest.MonkeyPatch) -> None:
    def cut(lex: Any, seqs: list[str], device: str, budget: Any) -> list[dict]:
        return [
            {
                "segments": [(0, len(s), len(s) % 5)],
                "columns": np.arange(len(s), dtype=np.int16),
                "best_bits": float(len(s)),
            }
            for s in seqs
        ]

    def theme_mass(lex: Any, seqs: list[str], device: str, budget: Any) -> list[tuple]:
        return [(np.array([len(s) % 5], np.int32), np.array([0.5], np.float16)) for s in seqs]

    monkeypatch.setattr(decode, "cut", cut)
    monkeypatch.setattr(decode, "theme_mass", theme_mass)
    monkeypatch.setattr(model, "load_lexicon", lambda path, device: object())


def _setup(tmp_path: Path) -> tuple[Path, Path, str]:
    bank = tmp_path / "bank"
    prepare_pass(_photos(tmp_path), bank, 12)
    lexicon = tmp_path / "lexico.pt"
    lexicon.write_bytes(b"weights")
    return bank, lexicon, hashlib.sha256(b"weights").hexdigest()


def _run(lexicon: Path, sha: str, **over: Any) -> dict[str, Any]:
    run = {
        "pass": "cut",
        "lexicon": str(lexicon),
        "lexicon_sha256": sha,
        "first": 0,
        "last": 999,
        "device": "cpu",
        "run_id": "job-1",
        "stop_after": None,
    }
    return {**run, **over}


def test_the_cut_writes_every_shard_with_its_record(tmp_path: Path, fake_decoder: None) -> None:
    bank, lexicon, sha = _setup(tmp_path)
    record = decode_pass(bank, "lexico-test", _run(lexicon, sha))
    shards = json.loads((bank / "shards.json").read_text())["shards"]
    assert record["shards_done"] == [s["shard"] for s in shards] and not record["shards_left"]
    out = lexicon_dir(bank, "lexico-test") / "cut"
    order = pq.read_table(bank / "sequences.parquet", columns=["md5", "sequence"])
    length_of = dict(
        zip(
            order.column("md5").to_pylist(),
            map(len, order.column("sequence").to_pylist()),
            strict=True,
        )
    )
    for shard in shards:
        table = pq.read_table(out / f"shard-{shard['shard']:05d}.parquet")
        assert table.num_rows == shard["rows"]
        for row in table.to_pylist():
            n = length_of[row["md5"]]
            assert row["seg_start"] == [0] and row["seg_end"] == [n] and row["seg_theme"] == [n % 5]
            assert row["columns"] == list(range(n)) and row["best_bits"] == float(n)
    assert (out / "runs" / "job-1.json").exists()


def test_a_rerun_skips_the_shards_already_written(tmp_path: Path, fake_decoder: None) -> None:
    bank, lexicon, sha = _setup(tmp_path)
    assert decode_pass(bank, "lexico-test", _run(lexicon, sha, first=0, last=0))["shards_done"] == [
        0
    ]
    again = decode_pass(bank, "lexico-test", _run(lexicon, sha, first=0, last=1, run_id="job-2"))
    assert again["shards_done"] == [1]


def test_a_lexicon_that_is_not_the_one_named_is_refused(tmp_path: Path, fake_decoder: None) -> None:
    bank, lexicon, _ = _setup(tmp_path)
    args = [
        "cut",
        "--bank",
        str(bank),
        "--name",
        "x",
        "--lexicon",
        str(lexicon),
        "--lexicon-sha256",
        "0" * 64,
    ]
    assert (
        build.main([*args, "--first", "0", "--last", "9", "--device", "cpu", "--run-id", "j"]) == 2
    )
    assert not (lexicon_dir(bank, "x") / "cut" / "shard-00000.parquet").exists()


def _both_passes(tmp_path: Path) -> tuple[Path, str, Path]:
    bank, lexicon, sha = _setup(tmp_path)
    decode_pass(bank, "lexico-test", _run(lexicon, sha))
    decode_pass(bank, "lexico-test", _run(lexicon, sha, **{"pass": "mass"}))
    parity = tmp_path / "parity.json"
    parity.write_text(json.dumps({"cut_identical": 94, "proteins": 94}))
    return bank, sha, parity


def test_the_manifest_is_complete_with_both_passes_and_the_parity(
    tmp_path: Path, fake_decoder: None
) -> None:
    bank, sha, parity = _both_passes(tmp_path)
    manifest = manifest_pass(bank, "lexico-test", {"sha256": sha, "parity": parity}, "rev")
    assert manifest["status"] == "complete", manifest["refusals"]
    assert manifest["parity"] == {"cut_identical": 94, "proteins": 94}
    assert manifest["passes"]["mass"]["card"] == "cpu"
    rows = sum(s["rows"] for s in manifest["passes"]["cut"]["shards"])
    assert rows == manifest["sequences"] == 5


def test_the_manifest_refuses_without_parity_or_with_a_changed_shard(
    tmp_path: Path, fake_decoder: None
) -> None:
    bank, sha, parity = _both_passes(tmp_path)
    assert (
        "no parity record"
        in manifest_pass(bank, "lexico-test", {"sha256": sha}, "rev")["refusals"][0]
    )
    shard = lexicon_dir(bank, "lexico-test") / "mass" / "shard-00000.parquet"
    pq.write_table(pq.read_table(shard).slice(1), shard)
    refused = manifest_pass(bank, "lexico-test", {"sha256": sha, "parity": parity}, "rev")
    assert refused["status"] == "refused"
    assert "mass/shard-00000 does not hash to its record" in refused["refusals"]


def test_the_manifest_refuses_shards_computed_with_another_lexicon(
    tmp_path: Path, fake_decoder: None
) -> None:
    bank, sha, parity = _both_passes(tmp_path)
    refused = manifest_pass(bank, "lexico-test", {"sha256": "f" * 64, "parity": parity}, "rev")
    assert "cut was computed with another lexicon" in refused["refusals"]
