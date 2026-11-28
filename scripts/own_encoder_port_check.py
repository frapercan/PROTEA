#!/usr/bin/env python
"""The desktop check of the segment encoder's port: the laboratory's model and the stage's.

The segment encoder (``jepa-segments``) was ported from the laboratory's
``jepa_lexico.py`` (arm ``tramos``) into ``protea.core.own_encoder.model``. Before the
cluster trains it, this builds both models from the same seed, on the CPU, and checks:

- **parameters**: every tensor of the context encoder, the predictor and the target
  encoder, by the laboratory's names mapped to the stage's (``len``, ``ord`` and
  ``pos`` became ``length``, ``order`` and ``position``);
- **one step's draw**: the micro-batches a seeded generator draws from a synthetic
  corpus (proteins of 1 to 80 segments, so the run of 64 is exercised), the same
  tokens and the same hidden marks;
- **the loss** on one synthetic batch, item by item through each side's own
  item builder and collate.

Each must agree within ``--tolerance`` (1e-6). The record goes to ``--out`` as JSON
and the exit status is 0 when everything agrees, 1 otherwise. Not a test: the
laboratory's script is not in this repository. Read it from its branch with ``git
show`` (never from a working tree) and run this with no card visible, in a unit with
a memory cap::

    git -C <lab repository> show compresion:compresion/scripts/jepa_lexico.py > lab.py
    CUDA_VISIBLE_DEVICES= python scripts/own_encoder_port_check.py --lab-script lab.py \\
        --lab-commit 3e0a2a1 --out port-check.json
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import sys
from collections.abc import Mapping
from pathlib import Path
from types import ModuleType
from typing import Any

import numpy as np
import torch

from protea.core.own_encoder import model as stage

#: The laboratory's module names that the stage renamed, as prefixes of a parameter's name.
RENAMED = (("len.", "length."), ("ord.", "order."), ("pos.", "position."))
#: The laboratory's batch keys and the stage's.
BATCH_KEYS = {"t": "theme", "l": "length", "o": "order", "m": "hidden", "pad": "padding"}
TOLERANCE = 1e-6


def load_lab(path: Path) -> ModuleType:
    """The laboratory's script as a module (it runs nothing at import)."""
    spec = importlib.util.spec_from_file_location("laboratory_segment_arm", path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"{path} is not a Python module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def stage_name(lab_name: str) -> str:
    """A laboratory parameter's name as the stage spells it."""
    for old, new in RENAMED:
        if lab_name.startswith(old):
            return new + lab_name[len(old) :]
    return lab_name


def compare_states(
    lab_state: Mapping[str, torch.Tensor], stage_state: Mapping[str, torch.Tensor]
) -> dict[str, Any]:
    """The tensors of two state dicts, the laboratory's names mapped: count and largest gap."""
    renamed = {stage_name(name): tensor for name, tensor in lab_state.items()}
    if set(renamed) != set(stage_state):
        missing = sorted(set(renamed) ^ set(stage_state))
        return {"same_names": False, "unmatched": missing, "max_abs_difference": float("inf")}
    largest, values = 0.0, 0
    for name, tensor in renamed.items():
        other = stage_state[name]
        if tensor.shape != other.shape:
            return {"same_names": True, "shape_mismatch": name, "max_abs_difference": float("inf")}
        values += tensor.numel()
        largest = max(largest, float((tensor.float() - other.float()).abs().max()))
    return {
        "same_names": True,
        "tensors": len(renamed),
        "values": values,
        "max_abs_difference": largest,
    }


def synthetic_corpus(seed: int, proteins: int = 48) -> list[stage.Segments]:
    """Proteins of 1 to 80 segments: random lengths and themes over the whole vocabulary."""
    rng = np.random.default_rng(seed)
    out = []
    for _ in range(proteins):
        n = int(rng.integers(1, 81))
        lengths = rng.integers(1, 3000, size=n)
        start = np.concatenate([[0], np.cumsum(lengths)[:-1]]).astype(np.int64)
        end = (start + lengths).astype(np.int64)
        out.append((start, end, rng.integers(0, stage.THEMES, size=n).astype(np.int64)))
    return out


class _LabCorpus:
    """What the laboratory's item builder reads: ``segments(p)``."""

    def __init__(self, corpus: list[stage.Segments]) -> None:
        self.corpus = corpus

    def segments(self, protein: int) -> stage.Segments:
        return self.corpus[protein]


def _as_stage(lab_batch: Mapping[str, torch.Tensor]) -> dict[str, torch.Tensor]:
    return {BATCH_KEYS[key]: value for key, value in lab_batch.items()}


def _same_batches(lab_groups: list[list[dict]], stage_groups: list[list[dict]]) -> bool:
    if [len(g) for g in lab_groups] != [len(g) for g in stage_groups]:
        return False
    for lab_group, stage_group in zip(lab_groups, stage_groups, strict=True):
        for lab_item, stage_item in zip(lab_group, stage_group, strict=True):
            for key, name in BATCH_KEYS.items():
                if key != "pad" and not np.array_equal(lab_item[key], stage_item[name]):
                    return False
    return True


def compare_draws(lab: ModuleType, corpus: list[stage.Segments], seed: int) -> dict[str, Any]:
    """One step's draw on each side from the same seeded generator."""
    pool = np.flatnonzero([len(theme) >= 2 for _, _, theme in corpus])
    budget = 65_536 // 8
    lab_groups = lab.draw(
        _LabCorpus(corpus), "tramos", pool, np.random.default_rng([seed, 0]), budget
    )
    stage_groups = stage.draw(corpus.__getitem__, pool, np.random.default_rng([seed, 0]), budget)
    return {
        "micro_batches": len(stage_groups),
        "proteins": sum(len(g) for g in stage_groups),
        "identical": _same_batches(lab_groups, stage_groups),
    }


def compare_losses(lab: ModuleType, corpus: list[stage.Segments], seed: int) -> dict[str, Any]:
    """Both models from ``seed``; their parameters and their loss on one synthetic batch."""
    lab_encoder, lab_predictor, lab_target = lab.build("tramos", seed)
    model = stage.build(seed)
    parameters = {
        name: compare_states(a.state_dict(), b.state_dict())
        for name, a, b in (
            ("encoder", lab_encoder, model.encoder),
            ("predictor", lab_predictor, model.predictor),
            ("target", lab_target, model.target),
        )
    }
    proteins = [p for p, (_, _, theme) in enumerate(corpus) if len(theme) >= 2]
    lab_items = [
        lab.tramos_item(_LabCorpus(corpus), p, np.random.default_rng([seed, p])) for p in proteins
    ]
    stage_items = [
        stage.segment_item(corpus[p], np.random.default_rng([seed, p])) for p in proteins
    ]
    lab_batch = lab.collate(lab_items, "cpu")
    stage_batch = stage.collate(stage_items, "cpu")
    same_batch = all(torch.equal(v, stage_batch[k]) for k, v in _as_stage(lab_batch).items())
    lab_loss = float(lab.jepa_loss(lab_encoder, lab_predictor, lab_target, lab_batch))
    stage_loss = float(stage.masked_segment_loss(model, stage_batch))
    return {
        "parameters": parameters,
        "batch": {
            "proteins": len(proteins),
            "tokens": int((~stage_batch["padding"]).sum()),
            "hidden": int(stage_batch["hidden"].sum()),
            "identical": same_batch,
        },
        "loss": {
            "laboratory": lab_loss,
            "stage": stage_loss,
            "abs_difference": abs(lab_loss - stage_loss),
        },
    }


def verdict(record: dict[str, Any], tolerance: float) -> bool:
    """Every parameter and the loss within ``tolerance``; the draws and the batch identical."""
    parameters_agree = all(
        p.get("same_names") and p["max_abs_difference"] <= tolerance
        for p in record["parameters"].values()
    )
    return bool(
        parameters_agree
        and record["draw"]["identical"]
        and record["batch"]["identical"]
        and record["loss"]["abs_difference"] <= tolerance
    )


def check(lab_script: Path, seed: int, tolerance: float = TOLERANCE) -> dict[str, Any]:
    """The whole comparison, on the CPU, as one record."""
    lab = load_lab(lab_script)
    corpus = synthetic_corpus(seed)
    record: dict[str, Any] = {
        "lab_script_sha256": hashlib.sha256(lab_script.read_bytes()).hexdigest(),
        "seed": seed,
        "device": "cpu",
        "torch": torch.__version__,
        "numpy": np.__version__,
        "cuda_visible": torch.cuda.is_available(),
        "tolerance": tolerance,
        "draw": compare_draws(lab, corpus, seed),
        **compare_losses(lab, corpus, seed),
    }
    record["agree"] = verdict(record, tolerance)
    return record


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--lab-script", type=Path, required=True)
    parser.add_argument("--lab-commit", default=None)
    parser.add_argument("--stage-revision", default=None)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--tolerance", type=float, default=TOLERANCE)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args(argv)
    torch.set_num_threads(max(1, min(8, torch.get_num_threads())))
    record = check(args.lab_script, args.seed, args.tolerance)
    record.update(lab_commit=args.lab_commit, stage_revision=args.stage_revision)
    text = json.dumps(record, indent=2, sort_keys=True)
    if args.out is not None:
        args.out.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if record["agree"] else 1


if __name__ == "__main__":
    sys.exit(main())
