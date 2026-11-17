"""The embedding bank of the photos, computed away from the server (ADR-D62).

Four passes over photos already on the cluster (ADR-D59), with no database:

``prepare`` reads every photo it is given, checks each input against that photo's
manifest, and writes ``sequences.parquet``: every member's sequence once, keyed by
its md5 (recomputed from the sequence), sorted by length and then by md5, with one
row group per shard. A shard is a run of consecutive sequences holding about
``--shard-tokens`` tokens, so shards cost about the same; ``shards.json`` lists
them. A member whose sequence the photo lacks is counted and named, never guessed.

``embed`` loads ONE configuration's model through its protea-backends plugin and
embeds a range of shards with the plugin's padded pass (``embed_chunks_padded``):
a shard's sequences, already in length order, are cut into batches whose rows
times their longest member stay under the token budget. Each shard is written as
``vectors/<config>/shard-NNNNN.parquet`` (``md5`` and ``vector`` in half precision,
the dtype of the ``halfvec`` column the platform stores) with its record. A shard
with a record is skipped, and the pass stops before a shard it could not finish
in the time it has, so a job of the cluster's two hours resumes where the last one
ended.

``compare`` embeds a sample of the bank both ways, one sequence per pass
(``embed_chunks``, the byte-reproducible regime of stage 1) and padded, and records
how far apart the vectors are and how fast each path runs: the measurement behind
the author's rule for this bank (efficiency, with differences that must be small).

``manifest`` writes ``bank-<config>.json``: the photos read, the configuration and
its id, the model's revision, the software and the card, the sha256 of every shard
and the counts. Its status is ``complete``, or ``refused`` when a shard is missing,
does not hash to its record or holds a vector that is not finite, or when the shards
were computed under more than one revision, software set or kind of card.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from protea.core.embedding_identity import derive_embedding_config_id
from protea.core.marks import MARK_RELEASE
from protea.core.plugins import discover_plugins

#: The photo inputs a full photo's bank reads; a truth-only photo has no TrEMBL file.
PHOTO_INPUTS: dict[str, tuple[str, ...]] = {
    "full": ("members.parquet", "sequences.parquet", "unreviewed_sequences.parquet"),
    "truth_only": ("members.parquet", "sequences.parquet"),
}
#: Tokens a sequence costs when shards are cut: the stage-1 models read at most 1022
#: residues and add their own one or two special tokens.
TOKEN_CAP = 1024
DEFAULT_SHARD_TOKENS = 8_000_000
#: Rows times the longest row of a padded batch, the default of the measured pilot.
DEFAULT_TOKEN_BUDGET = 65_536
BACKEND_GROUP = "protea.backends"
#: Where the ESM SDK keeps a checkpoint it names without a hub id.
SDK_REPOS = {
    "esmc_300m": "EvolutionaryScale/esmc-300m-2024-12",
    "esmc_600m": "EvolutionaryScale/esmc-600m-2024-12",
}
HALF_MAX = 65504.0
_EXAMPLES = 20
_SOFTWARE = ("torch", "transformers", "tokenizers", "esm", "numpy", "protea-backends")


class BankRefused(RuntimeError):
    """The inputs cannot make a bank; nothing is written past the refusal."""


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 22), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_record(path: Path, record: dict[str, Any]) -> None:
    path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_record(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _md5(sequence: str) -> str:
    return hashlib.md5(sequence.encode(), usedforsecurity=False).hexdigest()


# Prepare -------------------------------------------------------------------------


def checked_photo(photo: Path) -> dict[str, Any]:
    """The photo's identity, refused unless it is complete, up to the mark and unchanged."""
    manifest_path = photo / "photo.json"
    manifest = _read_record(manifest_path)
    cut = int(manifest.get("cut_release", 0))
    kind = manifest.get("kind", "full")
    if cut > MARK_RELEASE:
        raise BankRefused(f"cut {cut} is after the mark {MARK_RELEASE}")
    if manifest.get("status") != "complete":
        raise BankRefused(f"the photo of cut {cut} is {manifest.get('status')!r}")
    if kind not in PHOTO_INPUTS:
        raise BankRefused(f"the photo of cut {cut} is of an unknown kind {kind!r}")
    outputs = manifest.get("outputs", {})
    for name in PHOTO_INPUTS[kind]:
        if _file_sha256(photo / name) != outputs.get(name):
            raise BankRefused(f"{name} of cut {cut} no longer hashes to what the photo recorded")
    return {
        "cut_release": cut,
        "kind": kind,
        "manifest_sha256": _file_sha256(manifest_path),
        "revision": manifest.get("revision"),
    }


def _members_sequences(
    photo: Path, kind: str, sequence_of: dict[str, str]
) -> tuple[set[str], dict[str, Any]]:
    """The md5s of the photo's members, their sequences added to ``sequence_of``; the counts."""
    md5_of: dict[str, str] = {}
    for name in PHOTO_INPUTS[kind][1:]:
        table = pq.read_table(photo / name, columns=["accession", "md5", "sequence"])
        for accession, md5, sequence in zip(
            table.column("accession").to_pylist(),
            table.column("md5").to_pylist(),
            table.column("sequence").to_pylist(),
            strict=True,
        ):
            if _md5(sequence) != md5:
                raise BankRefused(f"the sequence of {accession} does not hash to {md5}")
            md5_of[accession] = md5
            sequence_of.setdefault(md5, sequence)
    accessions = pq.read_table(photo / "members.parquet", columns=["accession"])
    members = accessions.column("accession").to_pylist()
    missing = sorted(a for a in members if a not in md5_of)
    md5s = {md5_of[a] for a in members if a in md5_of}
    return md5s, {
        "members": len(members),
        "member_sequences": len(md5s),
        "members_without_sequence": len(missing),
        "members_without_sequence_examples": missing[:_EXAMPLES],
    }


def cut_shards(lengths: Sequence[int], shard_tokens: int) -> list[dict[str, int]]:
    """Consecutive runs of about ``shard_tokens`` tokens each, over lengths in bank order."""
    shards: list[dict[str, int]] = []
    first, tokens, residues = 0, 0, 0
    for row, length in enumerate(lengths):
        tokens += min(length + 2, TOKEN_CAP)
        residues += length
        if tokens >= shard_tokens:
            shards.append({"first_row": first, "rows": row + 1 - first, "tokens": tokens})
            shards[-1]["residues"] = residues
            first, tokens, residues = row + 1, 0, 0
    if first < len(lengths):
        shards.append(
            {
                "first_row": first,
                "rows": len(lengths) - first,
                "tokens": tokens,
                "residues": residues,
            }
        )
    for index, shard in enumerate(shards):
        shard["shard"] = index
    return shards


def _write_bank_sequences(out: Path, ordered: list[tuple[int, str, str]], shards: list) -> None:
    """``sequences.parquet``: md5, sequence, length and shard, one row group per shard."""
    schema = pa.schema(
        [
            ("md5", pa.string()),
            ("sequence", pa.large_string()),
            ("length", pa.int32()),
            ("shard", pa.int32()),
        ]
    )
    with pq.ParquetWriter(out / "sequences.parquet", schema, compression="zstd") as writer:
        for shard in shards:
            rows = ordered[shard["first_row"] : shard["first_row"] + shard["rows"]]
            table = pa.table(
                {
                    "md5": [md5 for _, md5, _ in rows],
                    "sequence": [sequence for _, _, sequence in rows],
                    "length": [length for length, _, _ in rows],
                    "shard": [shard["shard"]] * len(rows),
                },
                schema=schema,
            )
            writer.write_table(table, row_group_size=max(1, len(rows)))


def prepare_pass(photos: Sequence[Path], out: Path, shard_tokens: int) -> dict[str, Any]:
    """Write ``sequences.parquet``, ``shards.json`` and their record, ``prepare.json``."""
    if not photos:
        raise BankRefused("a bank needs at least one photo")
    sources: list[dict[str, Any]] = []
    sequence_of: dict[str, str] = {}
    wanted: set[str] = set()
    for photo in photos:
        source = checked_photo(photo)
        md5s, counts = _members_sequences(photo, source["kind"], sequence_of)
        sources.append({**source, **counts})
        wanted |= md5s
    cuts = [s["cut_release"] for s in sources]
    if len(set(cuts)) != len(cuts):
        raise BankRefused(f"a cut is named twice: {sorted(cuts)}")
    ordered = sorted((len(sequence_of[m]), m, sequence_of[m]) for m in wanted)
    shards = cut_shards([length for length, _, _ in ordered], shard_tokens)
    out.mkdir(parents=True, exist_ok=True)
    _write_bank_sequences(out, ordered, shards)
    _write_record(out / "shards.json", {"shards": shards})
    record = {
        "pass": "prepare",
        "photos": sorted(sources, key=lambda s: s["cut_release"]),
        "sequences": len(ordered),
        "residues": sum(length for length, _, _ in ordered),
        "longest": ordered[-1][0] if ordered else 0,
        "shard_tokens": shard_tokens,
        "shards": len(shards),
        "sequences_parquet_sha256": _file_sha256(out / "sequences.parquet"),
        "shards_json_sha256": _file_sha256(out / "shards.json"),
    }
    _write_record(out / "prepare.json", record)
    return record


# The configuration and its model -------------------------------------------------


def bank_recipe(bank: Path, config_id: str) -> dict[str, Any]:
    """The recipe ``recipes.json`` holds for ``config_id``, refused unless it derives that id."""
    configs = {c["id"]: c["recipe"] for c in _read_record(bank / "recipes.json")["configs"]}
    if config_id not in configs:
        raise BankRefused(f"recipes.json holds no configuration {config_id}")
    recipe = configs[config_id]
    derived = str(derive_embedding_config_id(recipe))
    if derived != config_id:
        raise BankRefused(f"the recipe of {config_id} derives {derived}")
    if recipe["use_chunking"]:
        raise BankRefused(f"{config_id} chunks its sequences; a bank row is one whole sequence")
    return recipe


def backend_of(recipe: dict[str, Any]) -> Any:
    """The recipe's protea-backends plugin, refused unless it has a padded pass."""
    plugins = discover_plugins(BACKEND_GROUP)
    name = recipe["model_backend"]
    if name not in plugins:
        raise BankRefused(f"no backend {name!r}; installed: {sorted(plugins)}")
    plugin = plugins[name]
    if not callable(getattr(plugin, "embed_chunks_padded", None)):
        raise BankRefused(f"the {name!r} backend has no padded pass (embed_chunks_padded)")
    return plugin


def _say(event: str, message: Any = None, fields: Any = None, level: str = "info") -> None:
    print(f"{time.strftime('%H:%M:%S')} {level} {event} {fields or message or ''}", flush=True)


def _hub_cache() -> Path:
    if os.environ.get("HF_HUB_CACHE"):
        return Path(os.environ["HF_HUB_CACHE"])
    home = os.environ.get("HF_HOME") or str(Path.home() / ".cache" / "huggingface")
    return Path(home) / "hub"


def model_revision(model: Any, model_name: str) -> str | None:
    """The commit the weights came from: the model's own record, or the cache's ref."""
    commit = getattr(getattr(model, "config", None), "_commit_hash", None)
    if commit:
        return str(commit)
    repo = SDK_REPOS.get(model_name, model_name)
    ref = _hub_cache() / f"models--{repo.replace('/', '--')}" / "refs" / "main"
    return ref.read_text(encoding="ascii").strip() if ref.is_file() else None


def environment(device: str) -> dict[str, Any]:
    """The software and the card a pass runs with, as a record names them."""
    from importlib.metadata import PackageNotFoundError, distribution

    software: dict[str, Any] = {}
    for name in _SOFTWARE:
        try:
            dist = distribution(name)
        except PackageNotFoundError:
            continue
        software[name] = dist.version
        direct = dist.read_text("direct_url.json")
        if direct and "vcs_info" in direct:
            software[f"{name}_commit"] = json.loads(direct)["vcs_info"].get("commit_id")
    card = "cpu"
    try:
        import torch

        software["cuda"] = torch.version.cuda
        if device.startswith("cuda") and torch.cuda.is_available():
            card = torch.cuda.get_device_name(torch.device(device))
    except ImportError:
        pass
    return {"software": software, "card": card, "host": socket.gethostname()}


# Embed ---------------------------------------------------------------------------


def length_batches(lengths: Sequence[int], max_length: int | None, budget: int) -> list[range]:
    """Consecutive batches whose rows times their longest row stay under ``budget``.

    A row costs its residues as the model reads them (cut at ``max_length``) plus two
    special tokens. A row longer than the budget alone runs alone.
    """
    batches: list[range] = []
    start, longest = 0, 0
    for row, length in enumerate(lengths):
        tokens = min(length, max_length or length) + 2
        widest = max(longest, tokens)
        if row > start and (row + 1 - start) * widest > budget:
            batches.append(range(start, row))
            start, widest = row, tokens
        longest = widest
    if start < len(lengths):
        batches.append(range(start, len(lengths)))
    return batches


def to_half(vector: np.ndarray, scale: float) -> np.ndarray:
    """The halfvec the platform stores: ``scale_and_clip_embedding``, then half precision."""
    if scale == 1.0:
        return np.asarray(vector, dtype=np.float32).astype(np.float16)
    scaled = np.asarray(vector, dtype=np.float64) / scale
    return np.clip(scaled, -HALF_MAX, HALF_MAX).astype(np.float16)


def embed_rows(
    plugin: Any, loaded: tuple[Any, Any], sequences: Sequence[str], config: Any, run: dict
) -> np.ndarray:
    """Every sequence's vector through the padded pass, in input order, as halfvec rows."""
    model, tokenizer = loaded
    rows: list[np.ndarray] = []
    lengths = [len(s) for s in sequences]
    for batch in length_batches(lengths, config.max_length, int(run["token_budget"])):
        chunks = plugin.embed_chunks_padded(
            model, tokenizer, [sequences[i] for i in batch], config, run["device"]
        )
        for one in chunks:
            if len(one) != 1:
                raise BankRefused(f"a sequence came back in {len(one)} chunks, not one")
            rows.append(to_half(one[0].vector, float(config.embedding_scale)))
    dim = rows[0].shape[0] if rows else 0
    return np.stack(rows) if rows else np.zeros((0, dim), dtype=np.float16)


def _shard_rows(bank: Path, shard: int) -> tuple[list[str], list[str]]:
    table = pq.ParquetFile(bank / "sequences.parquet").read_row_group(shard)
    return table.column("md5").to_pylist(), table.column("sequence").to_pylist()


def write_shard(out: Path, shard: int, md5s: list[str], vectors: np.ndarray) -> Path:
    """``shard-NNNNN.parquet`` (``md5``, ``vector`` as fixed-size half lists), written whole."""
    dim = int(vectors.shape[1]) if vectors.ndim == 2 else 0
    flat = pa.array(vectors.reshape(-1), type=pa.float16())
    table = pa.table(
        {"md5": pa.array(md5s, pa.string()), "vector": pa.FixedSizeListArray.from_arrays(flat, dim)}
    )
    path = out / f"shard-{shard:05d}.parquet"
    partial = path.with_suffix(".partial")
    pq.write_table(table, partial, compression="zstd")
    partial.rename(path)
    return path


def _embed_shard(bank: Path, shard: int, ctx: dict[str, Any], run: dict) -> dict[str, Any]:
    started = time.monotonic()
    md5s, sequences = _shard_rows(bank, shard)
    vectors = embed_rows(ctx["plugin"], ctx["loaded"], sequences, ctx["config"], run)
    finite: list[bool] = np.isfinite(vectors.astype(np.float32)).all(axis=1).tolist()
    path = write_shard(ctx["out"], shard, md5s, vectors)
    record = {
        "shard": shard,
        "rows": len(md5s),
        "dim": int(vectors.shape[1]) if len(md5s) else 0,
        "residues": sum(len(s) for s in sequences),
        "not_finite": finite.count(False),
        "not_finite_examples": [m for m, ok in zip(md5s, finite, strict=True) if not ok][:5],
        "sha256": _file_sha256(path),
        "seconds": round(time.monotonic() - started, 1),
        "run": run["run_id"],
    }
    _write_record(path.with_suffix(".json"), record)
    return record


def _load(recipe: dict[str, Any], device: str) -> dict[str, Any]:
    plugin = backend_of(recipe)
    loaded = plugin.load_model(recipe["model_name"], device, _say)
    config = SimpleNamespace(**recipe)
    return {"plugin": plugin, "loaded": loaded, "config": config}


def embed_pass(bank: Path, config_id: str, run: dict[str, Any]) -> dict[str, Any]:
    """Embed shards ``run['first']`` to ``run['last']``; skip done ones, stop before the time.

    ``run`` holds ``first``, ``last``, ``token_budget``, ``device``, ``stop_after`` (seconds
    from the start this pass may use, or None) and ``run_id``.
    """
    started = time.monotonic()
    recipe = bank_recipe(bank, config_id)
    shards = _read_record(bank / "shards.json")["shards"]
    wanted = [s["shard"] for s in shards if run["first"] <= s["shard"] <= run["last"]]
    out = bank / "vectors" / config_id
    out.mkdir(parents=True, exist_ok=True)
    todo = [k for k in wanted if not (out / f"shard-{k:05d}.json").exists()]
    ctx = _load(recipe, run["device"]) if todo else {}
    if ctx:
        ctx["out"] = out
    done: list[dict[str, Any]] = []
    slowest, stopped = 0.0, False
    for shard in todo:
        elapsed = time.monotonic() - started
        if run.get("stop_after") and done and elapsed + 1.25 * slowest > run["stop_after"]:
            stopped = True
            break
        record = _embed_shard(bank, shard, ctx, run)
        slowest = max(slowest, record["seconds"])
        done.append(record)
        _say("shard", fields={k: record[k] for k in ("shard", "rows", "seconds", "not_finite")})
    revision = model_revision(ctx["loaded"][0], recipe["model_name"]) if ctx else None
    record = {
        "pass": "embed",
        "config_id": config_id,
        "run_id": run["run_id"],
        "range": [run["first"], run["last"]],
        "token_budget": int(run["token_budget"]),
        "model_revision": revision,
        "environment": environment(run["device"]),
        "shards_done": [r["shard"] for r in done],
        "shards_left": [k for k in todo if k not in {r["shard"] for r in done}],
        "stopped_for_time": stopped,
        "seconds": round(time.monotonic() - started),
    }
    if ctx:
        (out / "runs").mkdir(exist_ok=True)
        _write_record(out / "runs" / f"{run['run_id']}.json", record)
    return record


# Compare -------------------------------------------------------------------------


def _sample(bank: Path, size: int) -> tuple[list[str], list[str]]:
    """``size`` rows spread evenly over the bank's length order."""
    table = pq.read_table(bank / "sequences.parquet", columns=["md5", "sequence"])
    step = max(1, table.num_rows // max(1, size))
    rows = list(range(0, table.num_rows, step))[:size]
    picked = table.take(pa.array(rows))
    return picked.column("md5").to_pylist(), picked.column("sequence").to_pylist()


def _timed(work: Callable[[], Any], device: str) -> tuple[Any, float]:
    def sync() -> None:
        if device.startswith("cuda"):
            import torch

            torch.cuda.synchronize()

    sync()
    started = time.monotonic()
    result = work()
    sync()
    return result, time.monotonic() - started


def compare_vectors(one_by_one: np.ndarray, padded: np.ndarray) -> dict[str, Any]:
    """How far the padded vectors are from the one-sequence ones, as stored (half)."""
    a = one_by_one.astype(np.float32)
    b = padded.astype(np.float32)
    diff = np.abs(a - b).max(axis=1)
    cosine = (a * b).sum(axis=1) / (np.linalg.norm(a, axis=1) * np.linalg.norm(b, axis=1))
    return {
        "rows": int(a.shape[0]),
        "identical_rows": int((one_by_one == padded).all(axis=1).sum()),
        "max_abs_diff": float(diff.max()) if diff.size else 0.0,
        "p99_row_max_abs_diff": float(np.quantile(diff, 0.99)) if diff.size else 0.0,
        "median_row_max_abs_diff": float(np.median(diff)) if diff.size else 0.0,
        "min_cosine": float(cosine.min()) if cosine.size else 1.0,
        "mean_cosine": float(cosine.mean()) if cosine.size else 1.0,
    }


def compare_pass(bank: Path, config_id: str, run: dict[str, Any]) -> dict[str, Any]:
    """Embed a sample one sequence per pass and padded; record the gap and the speeds."""
    recipe = bank_recipe(bank, config_id)
    ctx = _load(recipe, run["device"])
    md5s, sequences = _sample(bank, int(run["sample"]))
    model, tokenizer = ctx["loaded"]
    config, scale = ctx["config"], float(recipe["embedding_scale"])

    def one(sequence: str) -> np.ndarray:
        chunks = ctx["plugin"].embed_chunks(model, tokenizer, [sequence], config, run["device"])
        return to_half(chunks[0][0].vector, scale)

    def one_by_one() -> np.ndarray:
        return np.stack([one(s) for s in sequences])

    single, single_seconds = _timed(one_by_one, run["device"])
    padded, padded_seconds = _timed(
        lambda: embed_rows(ctx["plugin"], ctx["loaded"], sequences, config, run), run["device"]
    )
    residues = sum(min(len(s), recipe["max_length"] or len(s)) for s in sequences)
    record = {
        "pass": "compare",
        "config_id": config_id,
        "sample": len(md5s),
        "residues_read": residues,
        "token_budget": int(run["token_budget"]),
        "one_by_one_seconds": round(single_seconds, 2),
        "padded_seconds": round(padded_seconds, 2),
        "one_by_one_residues_per_second": round(residues / max(single_seconds, 1e-9)),
        "padded_residues_per_second": round(residues / max(padded_seconds, 1e-9)),
        "difference": compare_vectors(single, padded),
        "model_revision": model_revision(model, recipe["model_name"]),
        "environment": environment(run["device"]),
    }
    _write_record(bank / f"compare-{config_id}-{run['run_id']}.json", record)
    return record


# Manifest ------------------------------------------------------------------------


def _shard_problems(out: Path, shard: dict[str, Any]) -> list[str]:
    name = f"shard-{shard['shard']:05d}"
    if not (out / f"{name}.json").exists() or not (out / f"{name}.parquet").exists():
        return [f"{name} missing"]
    record = _read_record(out / f"{name}.json")
    problems = []
    if _file_sha256(out / f"{name}.parquet") != record["sha256"]:
        problems.append(f"{name} does not hash to its record")
    if record["rows"] != shard["rows"]:
        problems.append(f"{name} holds {record['rows']} rows, the shard {shard['rows']}")
    if record["not_finite"]:
        problems.append(f"{name} holds {record['not_finite']} vectors that are not finite")
    return problems


def _runs_used(out: Path, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    used = {r["run"] for r in records}
    return [_read_record(out / "runs" / f"{run_id}.json") for run_id in sorted(used)]


def _single(values: list[str]) -> str | None:
    return values[0] if len(values) == 1 else None


def manifest_pass(bank: Path, config_id: str, revision: str) -> dict[str, Any]:
    """Write ``bank-<config>.json`` from the prepare record, the shard records and the runs."""
    prepare = _read_record(bank / "prepare.json")
    recipe = bank_recipe(bank, config_id)
    shards = _read_record(bank / "shards.json")["shards"]
    out = bank / "vectors" / config_id
    problems = [p for shard in shards for p in _shard_problems(out, shard)]
    records = []
    if not problems:
        records = [_read_record(out / f"shard-{s['shard']:05d}.json") for s in shards]
    runs = _runs_used(out, records) if records else []
    regime = {
        "model_revision": sorted({str(r["model_revision"]) for r in runs}),
        "software": sorted(
            {json.dumps(r["environment"]["software"], sort_keys=True) for r in runs}
        ),
        "card": sorted({r["environment"]["card"] for r in runs}),
    }
    for key, values in regime.items():
        if len(values) > 1:
            problems.append(f"the shards were computed under {len(values)} values of {key}")
    if runs and "None" in regime["model_revision"]:
        problems.append("a run could not name the model's revision")
    manifest = {
        "table": "embedding_bank",
        "config_id": config_id,
        "recipe": recipe,
        "photos": prepare["photos"],
        "sequences": prepare["sequences"],
        "residues": prepare["residues"],
        "sequences_parquet_sha256": prepare["sequences_parquet_sha256"],
        "dim": records[0]["dim"] if records else None,
        "shards": [
            {"shard": r["shard"], "rows": r["rows"], "sha256": r["sha256"]} for r in records
        ],
        "model_revision": _single(regime["model_revision"]),
        "software": json.loads(regime["software"][0]) if len(regime["software"]) == 1 else None,
        "card": _single(regime["card"]),
        "token_budgets": sorted({r["token_budget"] for r in runs}),
        "runs": [r["run_id"] for r in runs],
        "status": "refused" if problems else "complete",
        "refusals": problems,
        "revision": revision,
    }
    _write_record(bank / f"bank-{config_id}.json", manifest)
    return manifest


# Command line --------------------------------------------------------------------


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m protea.core.embedding_bank.build")
    sub = parser.add_subparsers(dest="pass_name", required=True)
    p_prep = sub.add_parser("prepare")
    p_prep.add_argument("--photo", type=Path, action="append", required=True)
    p_prep.add_argument("--out", type=Path, required=True)
    p_prep.add_argument("--shard-tokens", type=int, default=DEFAULT_SHARD_TOKENS)
    for name in ("embed", "compare"):
        p = sub.add_parser(name)
        p.add_argument("--bank", type=Path, required=True)
        p.add_argument("--config", required=True)
        p.add_argument("--token-budget", type=int, default=DEFAULT_TOKEN_BUDGET)
        p.add_argument("--device", default="cuda")
        p.add_argument("--run-id", required=True)
    p_embed = sub.choices["embed"]
    p_embed.add_argument("--first", type=int, required=True)
    p_embed.add_argument("--last", type=int, required=True)
    p_embed.add_argument("--stop-after", type=int, default=None)
    sub.choices["compare"].add_argument("--sample", type=int, default=2000)
    p_man = sub.add_parser("manifest")
    p_man.add_argument("--bank", type=Path, required=True)
    p_man.add_argument("--config", required=True)
    p_man.add_argument("--revision", required=True)
    return parser


def _run_of(args: argparse.Namespace) -> dict[str, Any]:
    run = {"token_budget": args.token_budget, "device": args.device, "run_id": args.run_id}
    if args.pass_name == "embed":
        run.update(first=args.first, last=args.last, stop_after=args.stop_after)
    else:
        run["sample"] = args.sample
    return run


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.pass_name == "prepare":
            prepare_pass(args.photo, args.out, args.shard_tokens)
        elif args.pass_name == "embed":
            record = embed_pass(args.bank, args.config, _run_of(args))
            return 4 if record["shards_left"] else 0
        elif args.pass_name == "compare":
            compare_pass(args.bank, args.config, _run_of(args))
        else:
            manifest = manifest_pass(args.bank, args.config, args.revision)
            return 0 if manifest["status"] == "complete" else 3
    except BankRefused as refused:
        print(f"REFUSED: {refused}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
