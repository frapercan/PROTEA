"""The embedding bank's file stage (ADR-D62): what it reads, writes and refuses.

Two small photos are written on disk the way the photo builder writes them (a full one
with its TrEMBL file and a truth-only one without), and the protea-backends plugin is
replaced by one whose vector of a sequence depends on that sequence alone, so every
pass runs here, offline and without a card.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from protea.core.embedding_bank import build
from protea.core.embedding_bank.build import (
    BankRefused,
    compare_pass,
    cut_shards,
    embed_pass,
    length_batches,
    manifest_pass,
    prepare_pass,
    to_half,
)
from protea.core.embedding_identity import derive_embedding_config_id
from protea.core.operations._compute_embeddings_helpers import scale_and_clip_embedding


def _md5(sequence: str) -> str:
    return hashlib.md5(sequence.encode(), usedforsecurity=False).hexdigest()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


#: Cut 211 (full): P1 and P2 share a sequence; Q9 comes from TrEMBL; N2 has none.
FULL = {
    "sequences": {"P1": "MKKLL", "P2": "MKKLL", "P3": "MAAGGWW"},
    "unreviewed_sequences": {"Q9": "MQ", "N1": "MNNPPAAAAA"},
    "members": ["P1", "P2", "P3", "Q9", "N1", "N2"],
}
#: Cut 205 (truth only): P3 again, and one sequence the full photo lacks.
TRUTH_ONLY = {
    "sequences": {"P3": "MAAGGWW", "P7": "MCCCC"},
    "members": ["P3", "P7"],
}


def _photo(root: Path, cut: int, spec: dict[str, Any], kind: str = "full") -> Path:
    photo = root / f"photo-{cut}"
    photo.mkdir(parents=True)
    pq.write_table(pa.table({"accession": spec["members"]}), photo / "members.parquet")
    for name in ("sequences", "unreviewed_sequences"):
        if name not in spec:
            continue
        table = spec[name]
        pq.write_table(
            pa.table(
                {
                    "accession": list(table),
                    "md5": [_md5(s) for s in table.values()],
                    "sequence": list(table.values()),
                }
            ),
            photo / f"{name}.parquet",
        )
    manifest = {
        "cut_release": cut,
        "kind": kind,
        "status": "complete",
        "revision": "photo-revision",
        "outputs": {name: _sha256(photo / name) for name in build.PHOTO_INPUTS[kind]},
    }
    (photo / "photo.json").write_text(json.dumps(manifest), encoding="utf-8")
    return photo


def _photos(tmp_path: Path) -> list[Path]:
    return [
        _photo(tmp_path, 211, FULL),
        _photo(tmp_path, 205, TRUTH_ONLY, kind="truth_only"),
    ]


RECIPE = {
    "chunk_overlap": 0,
    "chunk_size": 512,
    "embedding_scale": 1.0,
    "layer_agg": "mean",
    "layer_indices": [0],
    "max_length": 1022,
    "model_backend": "fake",
    "model_name": "org/fake-model",
    "normalize": True,
    "normalize_residues": False,
    "pooling": "mean",
    "use_chunking": False,
}
CONFIG_ID = str(derive_embedding_config_id(RECIPE))


def _vector(sequence: str) -> np.ndarray:
    raw = np.array([len(sequence), sum(map(ord, sequence)) % 97, ord(sequence[-1]), 1.0])
    return (raw / np.linalg.norm(raw)).astype(np.float32)


class _Backend:
    """A plugin whose vector of a sequence depends on that sequence alone."""

    name = "fake"

    def __init__(self) -> None:
        self.padded_batches: list[int] = []

    def load_model(self, model_name: str, device: str, emit: Any) -> tuple[Any, Any]:
        emit("backend.fake.load_done", None, {"model_name": model_name}, "info")
        return SimpleNamespace(config=SimpleNamespace(_commit_hash="c0ffee")), None

    def embed_chunks(self, model: Any, tok: Any, seqs: list[str], cfg: Any, dev: str) -> list:
        return [
            [SimpleNamespace(chunk_index_s=0, chunk_index_e=None, vector=_vector(s))] for s in seqs
        ]

    def embed_chunks_padded(
        self, model: Any, tok: Any, seqs: list[str], cfg: Any, dev: str
    ) -> list:
        self.padded_batches.append(len(seqs))
        return self.embed_chunks(model, tok, seqs, cfg, dev)


@pytest.fixture
def backend(monkeypatch: pytest.MonkeyPatch) -> _Backend:
    plugin = _Backend()
    monkeypatch.setattr(build, "discover_plugins", lambda group: {"fake": plugin})
    return plugin


def _bank(tmp_path: Path, shard_tokens: int = 12, recipe: dict | None = None) -> Path:
    bank = tmp_path / "bank"
    prepare_pass(_photos(tmp_path), bank, shard_tokens)
    recipe = recipe or RECIPE
    configs = [{"id": str(derive_embedding_config_id(recipe)), "recipe": recipe}]
    (bank / "recipes.json").write_text(json.dumps({"configs": configs}), encoding="utf-8")
    return bank


def _run(**over: Any) -> dict[str, Any]:
    run = {
        "first": 0,
        "last": 99,
        "token_budget": 64,
        "device": "cpu",
        "stop_after": None,
        "run_id": "job-1",
    }
    return {**run, **over}


# Prepare -------------------------------------------------------------------------


def test_prepare_writes_every_members_sequence_once_in_length_order(tmp_path: Path) -> None:
    record = prepare_pass(_photos(tmp_path), tmp_path / "bank", 12)
    table = pq.read_table(tmp_path / "bank" / "sequences.parquet")
    expected = sorted(
        {"MKKLL", "MAAGGWW", "MQ", "MNNPPAAAAA", "MCCCC"}, key=lambda s: (len(s), _md5(s))
    )
    assert table.column("sequence").to_pylist() == expected
    assert table.column("md5").to_pylist() == [_md5(s) for s in expected]
    assert table.column("length").to_pylist() == [len(s) for s in expected]
    assert record["sequences"] == 5
    assert record["residues"] == sum(map(len, expected))
    by_cut = {p["cut_release"]: p for p in record["photos"]}
    assert by_cut[211]["members_without_sequence"] == 1
    assert by_cut[211]["members_without_sequence_examples"] == ["N2"]
    assert by_cut[211]["member_sequences"] == 4
    assert by_cut[205]["kind"] == "truth_only"
    assert record["sequences_parquet_sha256"] == _sha256(tmp_path / "bank" / "sequences.parquet")


def test_prepare_writes_one_row_group_per_shard(tmp_path: Path) -> None:
    record = prepare_pass(_photos(tmp_path), tmp_path / "bank", 12)
    shards = json.loads((tmp_path / "bank" / "shards.json").read_text())["shards"]
    parquet = pq.ParquetFile(tmp_path / "bank" / "sequences.parquet")
    assert parquet.num_row_groups == len(shards) == record["shards"] > 1
    for shard in shards:
        group = parquet.read_row_group(shard["shard"])
        assert group.num_rows == shard["rows"]
        assert set(group.column("shard").to_pylist()) == {shard["shard"]}


def test_shards_cut_at_about_the_token_target() -> None:
    shards = cut_shards([3, 3, 3, 3, 3], 10)
    assert [(s["first_row"], s["rows"], s["tokens"]) for s in shards] == [
        (0, 2, 10),
        (2, 2, 10),
        (4, 1, 5),
    ]
    assert [s["shard"] for s in shards] == [0, 1, 2]
    assert sum(s["residues"] for s in shards) == 15


def test_a_long_sequence_costs_the_cap_not_its_length() -> None:
    (shard,) = cut_shards([5000], 10**9)
    assert shard["tokens"] == build.TOKEN_CAP
    assert shard["residues"] == 5000


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda m: m.update(cut_release=228), "after the mark"),
        (lambda m: m.update(status="measurement"), "'measurement'"),
        (lambda m: m["outputs"].update({"members.parquet": "0" * 64}), "no longer hashes"),
        (lambda m: m.update(kind="mystery"), "unknown kind"),
    ],
    ids=["past-the-mark", "not-complete", "changed-file", "unknown-kind"],
)
def test_prepare_refuses_a_photo_it_cannot_trust(tmp_path: Path, change: Any, message: str) -> None:
    photo = _photo(tmp_path, 211, FULL)
    manifest = json.loads((photo / "photo.json").read_text())
    change(manifest)
    (photo / "photo.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(BankRefused, match=message):
        prepare_pass([photo], tmp_path / "bank", 12)
    assert not (tmp_path / "bank").exists()


def test_prepare_refuses_a_sequence_that_does_not_hash_to_its_md5(tmp_path: Path) -> None:
    photo = _photo(tmp_path, 211, FULL)
    table = pq.read_table(photo / "sequences.parquet")
    wrong = table.set_column(1, "md5", pa.array(["0" * 32] * table.num_rows))
    pq.write_table(wrong, photo / "sequences.parquet")
    manifest = json.loads((photo / "photo.json").read_text())
    manifest["outputs"]["sequences.parquet"] = _sha256(photo / "sequences.parquet")
    (photo / "photo.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(BankRefused, match="does not hash to"):
        prepare_pass([photo], tmp_path / "bank", 12)


def test_prepare_refuses_a_cut_named_twice(tmp_path: Path) -> None:
    photo = _photo(tmp_path, 211, FULL)
    with pytest.raises(BankRefused, match="named twice"):
        prepare_pass([photo, photo], tmp_path / "bank", 12)


# Batches and the stored precision ------------------------------------------------


def test_batches_keep_rows_times_the_longest_under_the_budget() -> None:
    lengths = [1, 2, 2, 6, 6, 6, 30]
    batches = length_batches(lengths, None, 24)
    # Rows cost their length plus two special tokens: 3, 4, 4, then 8, 8, 8, then 32.
    assert [list(b) for b in batches] == [[0, 1, 2], [3, 4, 5], [6]]
    for batch in batches[:-1]:
        assert len(batch) * max(lengths[i] + 2 for i in batch) <= 24


def test_a_row_longer_than_the_budget_runs_alone_and_max_length_caps_its_cost() -> None:
    assert [list(b) for b in length_batches([50, 50], None, 10)] == [[0], [1]]
    assert [list(b) for b in length_batches([50, 50], 3, 10)] == [[0, 1]]


@pytest.mark.parametrize("scale", [1.0, 2.0, 1e-6])
def test_the_stored_half_is_what_the_store_path_writes(scale: float) -> None:
    vector = np.random.default_rng(7).normal(size=64).astype(np.float32)
    stored, _ = scale_and_clip_embedding(vector.tolist(), scale)
    expected = np.asarray(stored, dtype=np.float32 if scale == 1.0 else np.float64).astype(
        np.float16
    )
    assert np.array_equal(to_half(vector, scale), expected)


# Embed ---------------------------------------------------------------------------


def _shard_vectors(bank: Path, shard: int) -> tuple[list[str], np.ndarray]:
    table = pq.read_table(bank / "vectors" / CONFIG_ID / f"shard-{shard:05d}.parquet")
    flat = table.column("vector").combine_chunks().flatten().to_numpy()
    return table.column("md5").to_pylist(), flat.reshape(table.num_rows, -1)


def test_embed_writes_every_shard_as_md5_and_half_vectors(
    tmp_path: Path, backend: _Backend
) -> None:
    bank = _bank(tmp_path)
    record = embed_pass(bank, CONFIG_ID, _run())
    shards = json.loads((bank / "shards.json").read_text())["shards"]
    assert record["shards_done"] == [s["shard"] for s in shards]
    assert record["shards_left"] == [] and record["model_revision"] == "c0ffee"
    order = pq.read_table(bank / "sequences.parquet")
    sequence_of = dict(
        zip(order.column("md5").to_pylist(), order.column("sequence").to_pylist(), strict=True)
    )
    for shard in shards:
        md5s, vectors = _shard_vectors(bank, shard["shard"])
        assert vectors.dtype == np.float16
        expected = np.stack([_vector(sequence_of[m]).astype(np.float16) for m in md5s])
        assert np.array_equal(vectors, expected)
    assert (bank / "vectors" / CONFIG_ID / "runs" / "job-1.json").exists()


def test_embed_skips_done_shards_and_runs_only_its_range(tmp_path: Path, backend: _Backend) -> None:
    bank = _bank(tmp_path)
    first = embed_pass(bank, CONFIG_ID, _run(first=0, last=0))
    assert first["shards_done"] == [0]
    again = embed_pass(bank, CONFIG_ID, _run(first=0, last=1, run_id="job-2"))
    assert again["shards_done"] == [1]


def test_embed_stops_before_a_shard_it_could_not_finish(
    tmp_path: Path, backend: _Backend, monkeypatch: pytest.MonkeyPatch
) -> None:
    bank = _bank(tmp_path)
    clock = iter(range(0, 10_000, 100))
    monkeypatch.setattr(build.time, "monotonic", lambda: float(next(clock)))
    record = embed_pass(bank, CONFIG_ID, _run(stop_after=250))
    assert record["stopped_for_time"] is True
    assert record["shards_done"] and record["shards_left"]
    assert (
        build.main(["manifest", "--bank", str(bank), "--config", CONFIG_ID, "--revision", "r"]) == 3
    )


def test_embed_refuses_a_recipe_that_does_not_derive_its_id(
    tmp_path: Path, backend: _Backend
) -> None:
    bank = _bank(tmp_path)
    configs = {"configs": [{"id": CONFIG_ID, "recipe": {**RECIPE, "max_length": 512}}]}
    (bank / "recipes.json").write_text(json.dumps(configs), encoding="utf-8")
    with pytest.raises(BankRefused, match="derives"):
        embed_pass(bank, CONFIG_ID, _run())


def test_embed_refuses_a_chunking_recipe(tmp_path: Path, backend: _Backend) -> None:
    recipe = {**RECIPE, "use_chunking": True}
    bank = _bank(tmp_path, recipe=recipe)
    with pytest.raises(BankRefused, match="chunks"):
        embed_pass(bank, str(derive_embedding_config_id(recipe)), _run())


def test_embed_refuses_a_backend_without_a_padded_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bank = _bank(tmp_path)
    monkeypatch.setattr(
        build, "discover_plugins", lambda group: {"fake": SimpleNamespace(name="fake")}
    )
    with pytest.raises(BankRefused, match="no padded pass"):
        embed_pass(bank, CONFIG_ID, _run())


# Compare and manifest --------------------------------------------------------------


def test_compare_records_the_gap_and_both_speeds(tmp_path: Path, backend: _Backend) -> None:
    bank = _bank(tmp_path)
    record = compare_pass(bank, CONFIG_ID, {**_run(), "sample": 3})
    assert record["sample"] == 3
    assert record["difference"]["identical_rows"] == 3
    assert record["difference"]["max_abs_diff"] == 0.0
    assert record["one_by_one_residues_per_second"] > 0 and record["padded_residues_per_second"] > 0
    assert (bank / f"compare-{CONFIG_ID}-job-1.json").exists()


def test_manifest_is_complete_when_every_shard_is_there(tmp_path: Path, backend: _Backend) -> None:
    bank = _bank(tmp_path)
    embed_pass(bank, CONFIG_ID, _run())
    manifest = manifest_pass(bank, CONFIG_ID, "rev")
    assert manifest["status"] == "complete", manifest["refusals"]
    assert manifest["config_id"] == CONFIG_ID and manifest["recipe"] == RECIPE
    assert manifest["sequences"] == 5 and manifest["dim"] == 4
    assert manifest["model_revision"] == "c0ffee" and manifest["card"] == "cpu"
    assert [s["sha256"] for s in manifest["shards"]] == [
        _sha256(bank / "vectors" / CONFIG_ID / f"shard-{s['shard']:05d}.parquet")
        for s in manifest["shards"]
    ]


def test_manifest_refuses_a_shard_that_changed(tmp_path: Path, backend: _Backend) -> None:
    bank = _bank(tmp_path)
    embed_pass(bank, CONFIG_ID, _run())
    md5s, vectors = _shard_vectors(bank, 0)
    build.write_shard(bank / "vectors" / CONFIG_ID, 0, md5s, vectors[::-1].copy())
    manifest = manifest_pass(bank, CONFIG_ID, "rev")
    assert manifest["status"] == "refused"
    assert manifest["refusals"] == ["shard-00000 does not hash to its record"]


def test_manifest_refuses_shards_from_two_kinds_of_card(tmp_path: Path, backend: _Backend) -> None:
    bank = _bank(tmp_path)
    embed_pass(bank, CONFIG_ID, _run(first=0, last=0))
    embed_pass(bank, CONFIG_ID, _run(first=1, last=99, run_id="job-2"))
    run_path = bank / "vectors" / CONFIG_ID / "runs" / "job-2.json"
    run = json.loads(run_path.read_text())
    run["environment"]["card"] = "NVIDIA A100-SXM4-40GB"
    run_path.write_text(json.dumps(run), encoding="utf-8")
    manifest = manifest_pass(bank, CONFIG_ID, "rev")
    assert manifest["status"] == "refused"
    assert "2 values of card" in manifest["refusals"][0]


def test_manifest_refuses_a_vector_that_is_not_finite(
    tmp_path: Path, backend: _Backend, monkeypatch: pytest.MonkeyPatch
) -> None:
    bank = _bank(tmp_path)
    monkeypatch.setattr(build, "to_half", lambda v, s: np.full(4, np.inf, dtype=np.float16))
    embed_pass(bank, CONFIG_ID, _run(first=0, last=0))
    record = json.loads((bank / "vectors" / CONFIG_ID / "shard-00000.json").read_text())
    assert record["not_finite"] == record["rows"]
    manifest = manifest_pass(bank, CONFIG_ID, "rev")
    assert any("not finite" in p for p in manifest["refusals"])


def test_the_command_line_answers_with_the_documented_exit_codes(
    tmp_path: Path, backend: _Backend
) -> None:
    bank = _bank(tmp_path)
    args = ["--bank", str(bank), "--config", CONFIG_ID, "--device", "cpu", "--run-id", "j"]
    assert build.main(["embed", *args, "--first", "0", "--last", "99", "--token-budget", "64"]) == 0
    assert (
        build.main(["manifest", "--bank", str(bank), "--config", CONFIG_ID, "--revision", "r"]) == 0
    )
    assert (
        build.main(
            [
                "embed",
                "--bank",
                str(bank),
                "--config",
                "nope",
                "--run-id",
                "j",
                "--first",
                "0",
                "--last",
                "1",
            ]
        )
        == 2
    )
