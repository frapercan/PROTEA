"""Train the segment encoder on a cluster card, away from the server (the toy's P3).

The input is the lexicon cut of a declared list of member sequences (one md5 per
line; the list decides the population: for instance 211, the members of photos 160
to 211, so nothing later than 211), read from a complete lexicon bank on the cluster
(``protea.core.own_encoder.corpus``). No label is read at all. The run is the
laboratory's (``jepa_lexico.py train --arm tramos``), ported:

- 2,000 proteins (or a tenth of the corpus if smaller) drawn with seed 7 are kept
  out for a label-free learning curve; proteins with one segment do not train;
- each step draws proteins uniformly from the pool until ``tokens_per_batch / 8``
  tokens, from a generator seeded by (seed, step), so a resumed run draws the same
  batches;
- AdamW (betas 0.9 and 0.95, weight decay 0.05), the learning rate rising over the
  first 3 % of the planned time and falling by a cosine to 1 % of its peak, the
  gradient clipped at norm 1; the target encoder's momentum rises from 0.996 to 1
  over the plan;
- the plan is in minutes of training (the steps' own time), the held-out loss is
  written every ``eval_minutes``, the state every 20 minutes and at the end.

Outputs in ``--out``: ``state.pt`` (the state a resumed job continues from),
``metrics.jsonl``, ``initial-encoder.npz`` (the untrained twin), ``encoder.npz``
(the target encoder: the weights the vectors read, the same bytes for the same
weights) and ``own-encoder.json``, the record: ``complete`` when the plan was
spent, ``stopped`` when the job's time ran out first (exit 4: submit it again and
it resumes), or ``measurement`` for the sampled first job (``--measurement``), whose
weights no bank may use. Inputs it refuses (exit 2) leave no record.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, TextIO

import numpy as np
import torch

from protea.core import file_stage
from protea.core.file_stage import file_sha256, write_record
from protea.core.own_encoder import FAMILY, NAME, configuration_id, model_name
from protea.core.own_encoder.corpus import Corpus, CorpusRefused, checked_lexicon_cut, read_corpus
from protea.core.own_encoder.model import (
    HIDDEN_SHARE,
    MAX_SEGMENTS,
    WIDTH,
    Model,
    build,
    collate,
    draw,
    encoder_bytes,
    masked_segment_loss,
    micro_batches,
    segment_item,
)

__all__ = ["TrainRun", "main", "read_members", "schedule", "split", "train_pass"]

#: The held-out proteins: at most this many, drawn with this seed.
HELD_OUT, HELD_OUT_SEED, HELD_OUT_DRAW_SEED = 2_000, 7, 12_345
WEIGHT_DECAY, BETAS, CLIP = 0.05, (0.9, 0.95), 1.0
WARMUP_SHARE, FINAL_SHARE, MOMENTUM = 0.03, 0.01, 0.996
SAVE_SECONDS, LOG_EVERY = 20 * 60, 50
_SOFTWARE = ("torch", "numpy", "pyarrow")


@dataclass(frozen=True)
class TrainRun:
    """One training job: its inputs, its plan and its limits."""

    bank: Path
    lexicon: str
    members: Path
    out: Path
    plan_minutes: float
    wall_minutes: float
    measurement: bool = False
    device: str = "cuda"
    seed: int = 0
    tokens_per_batch: int = 65_536
    learning_rate: float = 5e-4
    eval_minutes: float = 10.0
    max_steps: int | None = None
    revision: str = "unknown"
    run_id: str = "local"

    @property
    def budget(self) -> int:
        """Tokens per step: the laboratory gave the segment arm an eighth of the batch."""
        return self.tokens_per_batch // 8


@dataclass
class Progress:
    """Steps taken, seconds spent on them, tokens read and the held-out curve so far."""

    step: int = 0
    spent: float = 0.0
    tokens: int = 0
    held: list[dict[str, Any]] = field(default_factory=list)


def read_members(path: Path) -> tuple[list[str], str]:
    """The declared member md5s, sorted and distinct, and the list file's sha256."""
    md5s = sorted(
        {line.strip() for line in path.read_text(encoding="ascii").splitlines() if line.strip()}
    )
    bad = [m for m in md5s if len(m) != 32 or any(c not in "0123456789abcdef" for c in m)]
    if not md5s or bad:
        raise CorpusRefused(f"{path} is not a list of sequence md5s ({len(bad)} malformed lines)")
    return md5s, file_sha256(path)


def split(corpus: Corpus) -> tuple[np.ndarray, np.ndarray]:
    """The training pool and the held-out proteins, as the laboratory drew them.

    The held-out draw is over the whole corpus; proteins with one segment then
    leave both (they have nothing to predict).
    """
    proteins = len(corpus)
    rng = np.random.default_rng(HELD_OUT_SEED)
    held = np.sort(rng.choice(proteins, size=min(HELD_OUT, proteins // 10), replace=False))
    usable = np.ones(proteins, dtype=bool)
    usable[held] = False
    two_or_more = corpus.counts() >= 2
    usable &= two_or_more
    return np.flatnonzero(usable), held[two_or_more[held]]


def schedule(learning_rate: float, fraction: float) -> tuple[float, float]:
    """The learning rate and the target's momentum at ``fraction`` of the plan."""
    if fraction < WARMUP_SHARE:
        rate = learning_rate * fraction / WARMUP_SHARE
    else:
        cosine = 0.5 * (1 + math.cos(math.pi * (fraction - WARMUP_SHARE) / (1 - WARMUP_SHARE)))
        rate = learning_rate * (FINAL_SHARE + (1 - FINAL_SHARE) * cosine)
    momentum = 1 - (1 - MOMENTUM) * 0.5 * (1 + math.cos(math.pi * fraction))
    return rate, momentum


def held_out_loss(model: Model, corpus: Corpus, held: np.ndarray, run: TrainRun) -> dict[str, Any]:
    """The loss on the held-out proteins, hidden tokens drawn with a fixed seed every time."""
    model.encoder.eval()
    model.predictor.eval()
    rng = np.random.default_rng(HELD_OUT_DRAW_SEED)
    total, tokens = 0.0, 0
    spread: list[float] = []
    with torch.no_grad():
        items = [segment_item(corpus.segments(int(p)), rng) for p in held]
        for group in micro_batches(items, run.budget):
            batch = collate(group, run.device)
            count = int((batch["hidden"] & ~batch["padding"]).sum())
            if count:
                total += float(masked_segment_loss(model, batch, spread)) * count
                tokens += count
    model.encoder.train()
    model.predictor.train()
    return {
        "loss": total / max(tokens, 1),
        "target_spread": float(np.mean(spread)) if spread else None,
    }


def _step(
    model: Model,
    optimizer: torch.optim.Optimizer,
    data: tuple[Corpus, np.ndarray],
    run: TrainRun,
    progress: Progress,
) -> dict[str, Any]:
    """One step of the plan; the progress gains the step and its time."""
    corpus, pool = data
    rate, momentum = schedule(run.learning_rate, min(progress.spent / (run.plan_minutes * 60), 1.0))
    for group in optimizer.param_groups:
        group["lr"] = rate
    started = time.time()
    rng = np.random.default_rng([run.seed, progress.step])
    batches = [collate(g, run.device) for g in draw(corpus.segments, pool, rng, run.budget)]
    optimizer.zero_grad(set_to_none=True)
    counts = [int((b["hidden"] & ~b["padding"]).sum()) for b in batches]
    loss = 0.0
    for batch, count in zip(batches, counts, strict=True):
        if count:
            value = masked_segment_loss(model, batch) * (count / max(sum(counts), 1))
            value.backward()
            loss += float(value.detach())
    torch.nn.utils.clip_grad_norm_(model.trained_parameters(), CLIP)
    optimizer.step()
    with torch.no_grad():
        for target, context in zip(
            model.target.parameters(), model.encoder.parameters(), strict=True
        ):
            target.mul_(momentum).add_(context.detach(), alpha=1 - momentum)
    progress.step += 1
    progress.spent += time.time() - started
    tokens = sum(int((~b["padding"]).sum()) for b in batches)
    progress.tokens += tokens
    return {
        "loss": loss,
        "lr": rate,
        "momentum": momentum,
        "micro_batches": len(batches),
        "tokens": tokens,
    }


def _state_path(run: TrainRun) -> Path:
    return run.out / "state.pt"


def save_state(
    run: TrainRun, model: Model, optimizer: torch.optim.Optimizer, progress: Progress
) -> None:
    """Everything a resumed job needs, written whole and then moved into place."""
    partial = run.out / "state.pt.partial"
    torch.save(
        {
            "arm": "segments",
            "seed": run.seed,
            "plan_minutes": run.plan_minutes,
            "encoder": model.encoder.state_dict(),
            "predictor": model.predictor.state_dict(),
            "target": model.target.state_dict(),
            "optimizer": optimizer.state_dict(),
            "progress": asdict(progress),
        },
        partial,
    )
    os.replace(partial, _state_path(run))


def resume_or_start(run: TrainRun, model: Model, optimizer: torch.optim.Optimizer) -> Progress:
    """The saved progress of an earlier job of this plan, or a new start with its untrained twin."""
    path = _state_path(run)
    if not path.exists():
        (run.out / "initial-encoder.npz").write_bytes(encoder_bytes(model.encoder))
        return Progress()
    state = torch.load(path, map_location=run.device, weights_only=False)
    if state["plan_minutes"] != run.plan_minutes or state["seed"] != run.seed:
        raise CorpusRefused("a resumed run must keep its plan and its seed")
    model.encoder.load_state_dict(state["encoder"])
    model.predictor.load_state_dict(state["predictor"])
    model.target.load_state_dict(state["target"])
    optimizer.load_state_dict(state["optimizer"])
    return Progress(**state["progress"])


def _log(handle: TextIO, record: dict[str, Any]) -> None:
    handle.write(json.dumps(record) + "\n")
    handle.flush()


def _record_held_out(
    progress: Progress, model: Model, data: tuple[Corpus, np.ndarray], run: TrainRun, log: TextIO
) -> None:
    """The held-out loss now, added to the curve the record and the state keep."""
    corpus, held = data
    point = {"step": progress.step, "spent": round(progress.spent, 1)}
    progress.held.append({**point, **held_out_loss(model, corpus, held, run)})
    _log(log, {"kind": "held", **progress.held[-1]})


def _may_step(run: TrainRun, progress: Progress, started: float) -> bool:
    """Whether the plan has time left, the job has minutes left and ``max_steps`` allows a step."""
    if run.max_steps is not None and progress.step >= run.max_steps:
        return False
    in_time = (time.time() - started) / 60 < run.wall_minutes
    return in_time and progress.spent < run.plan_minutes * 60


def _loop(
    model: Model, data: tuple[Corpus, np.ndarray, np.ndarray], run: TrainRun, log: TextIO
) -> Progress:
    """Steps until the plan is spent, the job's minutes run out or ``max_steps``."""
    corpus, pool, held = data
    optimizer = torch.optim.AdamW(
        model.trained_parameters(), lr=run.learning_rate, weight_decay=WEIGHT_DECAY, betas=BETAS
    )
    progress = resume_or_start(run, model, optimizer)
    if progress.step == 0:
        _record_held_out(progress, model, (corpus, held), run, log)
    started, last_eval, last_save = time.time(), time.time(), time.time()
    while _may_step(run, progress, started):
        record = _step(model, optimizer, (corpus, pool), run, progress)
        if progress.step % LOG_EVERY == 0:
            peak = (
                torch.cuda.max_memory_allocated() / 2**30 if run.device.startswith("cuda") else None
            )
            step = {"step": progress.step, "spent": round(progress.spent, 1), "gpu_peak_gb": peak}
            _log(log, {"kind": "train", **step, **record})
        if time.time() - last_eval > run.eval_minutes * 60:
            _record_held_out(progress, model, (corpus, held), run, log)
            last_eval = time.time()
        if time.time() - last_save > SAVE_SECONDS:
            save_state(run, model, optimizer, progress)
            last_save = time.time()
    if progress.held[-1]["step"] != progress.step:
        _record_held_out(progress, model, (corpus, held), run, log)
    save_state(run, model, optimizer, progress)
    return progress


def _inputs(run: TrainRun, say: Any) -> tuple[dict[str, Any], Corpus]:
    """The verified lexicon cut of the declared members, and what the record says of them."""
    cut = checked_lexicon_cut(run.bank, run.lexicon)
    members, members_sha256 = read_members(run.members)
    corpus = read_corpus(cut, sorted(cut.shards), set(members), say)
    if len(corpus) != len(members):
        raise CorpusRefused(
            f"{len(members) - len(corpus)} of the {len(members)} listed md5s have no cut in the "
            f"lexicon bank {run.lexicon}: the list is not of the bank's population"
        )
    counts = corpus.counts()
    record = {
        "lexicon": {
            "name": cut.name,
            "bank_manifest_sha256": cut.manifest_sha256,
            "lexicon_sha256": cut.lexicon_sha256,
        },
        "members": {"file": run.members.name, "sha256": members_sha256, "md5s": len(members)},
        "corpus": {
            "proteins": len(corpus),
            "segments": int(counts.sum()),
            "two_or_more_segments": int((counts >= 2).sum()),
            "over_max_segments": int((counts > MAX_SEGMENTS).sum()),
        },
    }
    return record, corpus


def _plan(run: TrainRun) -> dict[str, Any]:
    return {
        "plan_minutes": run.plan_minutes,
        "seed": run.seed,
        "tokens_per_batch": run.tokens_per_batch,
        "tokens_per_step": run.budget,
        "learning_rate": run.learning_rate,
        "weight_decay": WEIGHT_DECAY,
        "betas": list(BETAS),
        "gradient_clip": CLIP,
        "warmup_share": WARMUP_SHARE,
        "final_share": FINAL_SHARE,
        "momentum": [MOMENTUM, 1.0],
        "hidden_share": HIDDEN_SHARE,
        "max_segments": MAX_SEGMENTS,
        "width": WIDTH,
        "held_out": HELD_OUT,
        "eval_minutes": run.eval_minutes,
    }


def _status(run: TrainRun, progress: Progress) -> str:
    if run.measurement:
        return "measurement"
    return "complete" if progress.spent >= run.plan_minutes * 60 else "stopped"


def _finish(
    run: TrainRun, model: Model, progress: Progress, record: dict[str, Any]
) -> dict[str, Any]:
    """``encoder.npz`` and ``own-encoder.json``, the record of the encoder these weights are."""
    blob = encoder_bytes(model.target)
    (run.out / "encoder.npz").write_bytes(blob)
    sha = file_sha256(run.out / "encoder.npz")
    manifest = {
        "name": NAME,
        "family": FAMILY,
        "arm": "segments (the laboratory's jepa-tramos)",
        "status": _status(run, progress),
        "encoder": {"file": "encoder.npz", "sha256": sha, "bytes": len(blob)},
        "initial_encoder": {
            "file": "initial-encoder.npz",
            "sha256": file_sha256(run.out / "initial-encoder.npz"),
        },
        "model_name": model_name(sha),
        "configuration_id": str(configuration_id(sha)),
        **record,
        "plan": _plan(run),
        "progress": {
            "steps": progress.step,
            "spent_seconds": round(progress.spent, 1),
            "tokens": progress.tokens,
            "tokens_per_second": round(progress.tokens / max(progress.spent, 1e-9)),
            "complete": progress.spent >= run.plan_minutes * 60,
        },
        "held_out": progress.held,
        "environment": file_stage.environment(run.device, _SOFTWARE),
        "gpu_peak_gb": torch.cuda.max_memory_allocated() / 2**30
        if run.device.startswith("cuda")
        else None,
        "run_id": run.run_id,
        "revision": run.revision,
    }
    write_record(run.out / "own-encoder.json", manifest)
    return manifest


def train_pass(run: TrainRun, say: Any = print) -> dict[str, Any]:
    """Train (or resume) the segment encoder; write its weights and its record."""
    loaded = time.monotonic()
    run.out.mkdir(parents=True, exist_ok=True)
    record, corpus = _inputs(run, say)
    pool, held = split(corpus)
    if not len(pool):
        raise CorpusRefused("no listed protein has two segments or more: nothing to train on")
    record["corpus"].update(pool=len(pool), held_out=len(held))
    record["load_seconds"] = round(time.monotonic() - loaded, 1)
    model = build(run.seed).to(run.device)
    with open(run.out / "metrics.jsonl", "a", encoding="utf-8") as log:
        _log(
            log,
            {
                "kind": "start",
                "run_id": run.run_id,
                **record["corpus"],
                "plan_minutes": run.plan_minutes,
            },
        )
        progress = _loop(model, (corpus, pool, held), run, log)
        manifest = _finish(run, model, progress, record)
        _log(log, {"kind": "done", "step": progress.step, "status": manifest["status"]})
    say(json.dumps({k: manifest[k] for k in ("status", "model_name", "progress")}))
    return manifest


# Command line --------------------------------------------------------------------


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m protea.core.own_encoder.train")
    parser.add_argument("--bank", type=Path, required=True)
    parser.add_argument("--lexicon", required=True)
    parser.add_argument("--members", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--plan-minutes", type=float, required=True)
    parser.add_argument("--wall-minutes", type=float, required=True)
    parser.add_argument("--measurement", action="store_true")
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--eval-minutes", type=float, default=10.0)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--run-id", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """0 complete or measured, 2 refused, 4 stopped before the plan was spent (resubmit)."""
    args = _parser().parse_args(argv)
    run = TrainRun(
        bank=args.bank,
        lexicon=args.lexicon,
        members=args.members,
        out=args.out,
        plan_minutes=args.plan_minutes,
        wall_minutes=args.wall_minutes,
        measurement=args.measurement,
        device=args.device,
        seed=args.seed,
        eval_minutes=args.eval_minutes,
        revision=args.revision,
        run_id=args.run_id,
    )
    try:
        manifest = train_pass(run)
    except CorpusRefused as refused:
        print(f"REFUSED: {refused}", file=sys.stderr)
        return 2
    return 4 if manifest["status"] == "stopped" else 0


if __name__ == "__main__":
    raise SystemExit(main())
