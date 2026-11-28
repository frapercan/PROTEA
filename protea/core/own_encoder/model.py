"""The segment encoder's model, batches and loss: the laboratory's segment arm, ported.

The source is the laboratory's ``jepa_lexico.py`` (card jepa-lexico, 2026-11-13,
commit ``3e0a2a1``), arm ``tramos``: a predictive encoder over the lexicon's cut,
trained without labels (decision D46: the lexicon never sees function).

- **Tokens.** One per segment of the protein's cut, in order: the segment's
  theme, its length in log2 buckets (16) and its order (at most 64 segments; a
  longer protein trains on a random run of 64 consecutive segments and is read
  on its first 64).
- **Hidden tokens.** About 30 % of a protein's segments, at least one, chosen at
  random; a protein with one segment does not train.
- **Encoder.** A transformer of 6 layers, width 256, 8 heads (pre-norm, GELU,
  feed-forward 4 times the width, no dropout) and a final layer norm. The
  context encoder never sees the hidden tokens: they are keys it cannot attend
  to.
- **Predictor.** 3 layers of the same kind; it reads the visible tokens' outputs
  and, at the hidden positions, a learned mask vector plus the position only.
- **Target encoder.** The moving average of the context encoder, without
  gradient; a protein's vector is its mean output over the segments.
- **Loss.** The squared error between the predicted and the target
  representation of every hidden token, both normalised by layer.

Everything a seed decides is decided in the laboratory's order, so one seed gives
the laboratory's weights (``scripts/own_encoder_port_check.py`` compares the two
models and their loss on one batch). The only names that change are the
modules' (``length``, ``order``, ``position`` for the laboratory's ``len``,
``ord``, ``pos``) and the batch keys.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F

__all__ = [
    "HIDDEN_SHARE",
    "LENGTH_BUCKETS",
    "MAX_SEGMENTS",
    "PREDICTOR_POSITIONS",
    "THEMES",
    "WIDTH",
    "Model",
    "SegmentEncoder",
    "SegmentPredictor",
    "build",
    "collate",
    "draw",
    "encoder_bytes",
    "encoder_from_bytes",
    "length_bucket",
    "masked_segment_loss",
    "micro_batches",
    "segment_item",
]

#: The lexicon's themes: one embedding per theme id.
THEMES = 32_768
#: The longest run of segments a protein is read on.
MAX_SEGMENTS = 64
#: Segment lengths in log2 buckets, the last one open.
LENGTH_BUCKETS = 16
WIDTH = 256
ENCODER_LAYERS = 6
PREDICTOR_LAYERS = 3
HEADS = 8
#: The predictor's table of positions. The laboratory's script sized it for its
#: residue arm's windows of 1,024; the segment arm keeps the size so that one
#: seed gives the laboratory's weights. Positions past 63 are never read.
PREDICTOR_POSITIONS = 1_024
#: The share of a protein's segments hidden from the context encoder.
HIDDEN_SHARE = 0.3

#: A protein's segments: starts, ends (exclusive) and themes, in order.
Segments = tuple[np.ndarray, np.ndarray, np.ndarray]
Item = dict[str, np.ndarray]
Batch = dict[str, torch.Tensor]


def length_bucket(lengths: np.ndarray) -> np.ndarray:
    """A segment length's log2 bucket, the last bucket open."""
    return np.minimum(
        np.floor(np.log2(np.maximum(lengths, 1))).astype(np.int64), LENGTH_BUCKETS - 1
    )


def segment_item(segments: Segments, rng: np.random.Generator, hide: bool = True) -> Item:
    """One protein's tokens and, when ``hide``, the tokens hidden from the context encoder.

    The random numbers are drawn in the laboratory's order (the run of 64 first,
    then the hidden tokens), so a seeded draw is the laboratory's draw.
    """
    start, end, theme = segments
    n = len(theme)
    if n > MAX_SEGMENTS:
        first = int(rng.integers(0, n - MAX_SEGMENTS + 1)) if hide else 0
        start, end = start[first : first + MAX_SEGMENTS], end[first : first + MAX_SEGMENTS]
        theme, n = theme[first : first + MAX_SEGMENTS], MAX_SEGMENTS
    hidden = np.zeros(n, dtype=bool)
    if hide:
        count = max(1, int(round(HIDDEN_SHARE * n)))
        hidden[rng.choice(n, size=count, replace=False)] = True
    return {
        "theme": np.asarray(theme, dtype=np.int64),
        "length": length_bucket(np.asarray(end) - np.asarray(start)),
        "order": np.arange(n),
        "hidden": hidden,
    }


def collate(items: Sequence[Item], device: str | torch.device) -> Batch:
    """Items padded to the longest into tensors, with ``padding`` true past each item's end."""
    width = max(len(item["order"]) for item in items)
    out: Batch = {}
    for key in items[0]:
        first = items[0][key]
        array = np.zeros((len(items), width) + first.shape[1:], dtype=first.dtype)
        for row, item in enumerate(items):
            array[row, : len(item[key])] = item[key]
        out[key] = torch.as_tensor(array, device=device)
    padding = np.ones((len(items), width), dtype=bool)
    for row, item in enumerate(items):
        padding[row, : len(item["order"])] = False
    out["padding"] = torch.as_tensor(padding, device=device)
    return out


def micro_batches(items: Sequence[Item], cap: int) -> list[list[Item]]:
    """Items in length order, grouped so a group's padded size stays within ``cap`` tokens."""
    ordered = sorted(items, key=lambda item: len(item["order"]))
    groups: list[list[Item]] = []
    current: list[Item] = []
    for item in ordered:
        if current and (len(current) + 1) * len(item["order"]) > cap:
            groups.append(current)
            current = []
        current.append(item)
    if current:
        groups.append(current)
    return groups


def draw(
    segments_of: Callable[[int], Segments],
    pool: np.ndarray,
    rng: np.random.Generator,
    budget: int,
) -> list[list[Item]]:
    """A batch: proteins drawn uniformly from ``pool`` until ``budget`` tokens, as micro-batches."""
    items: list[Item] = []
    tokens = 0
    while tokens < budget:
        protein = int(pool[rng.integers(0, len(pool))])
        item = segment_item(segments_of(protein), rng)
        items.append(item)
        tokens += len(item["order"])
    return micro_batches(items, budget)


def _stack(layers: int) -> nn.TransformerEncoder:
    layer = nn.TransformerEncoderLayer(
        WIDTH,
        HEADS,
        4 * WIDTH,
        dropout=0.0,
        activation="gelu",
        batch_first=True,
        norm_first=True,
    )
    return nn.TransformerEncoder(layer, layers, enable_nested_tensor=False)


class SegmentEncoder(nn.Module):
    """The encoder: a protein's segment tokens to one representation per segment."""

    def __init__(self) -> None:
        super().__init__()
        self.theme = nn.Embedding(THEMES, WIDTH)
        self.length = nn.Embedding(LENGTH_BUCKETS, WIDTH)
        self.order = nn.Embedding(MAX_SEGMENTS, WIDTH)
        self.body = _stack(ENCODER_LAYERS)
        self.norm = nn.LayerNorm(WIDTH)

    def forward(self, batch: Batch, hidden: torch.Tensor | None = None) -> torch.Tensor:
        keys_out = batch["padding"] if hidden is None else (batch["padding"] | hidden)
        tokens = self.theme(batch["theme"]) + self.length(batch["length"])
        tokens = tokens + self.order(batch["order"])
        return self.norm(self.body(tokens, src_key_padding_mask=keys_out))


class SegmentPredictor(nn.Module):
    """The predictor: the visible tokens' outputs to the hidden tokens' representations."""

    def __init__(self) -> None:
        super().__init__()
        self.mask = nn.Parameter(torch.zeros(WIDTH))
        self.position = nn.Embedding(PREDICTOR_POSITIONS, WIDTH)
        self.body = _stack(PREDICTOR_LAYERS)
        self.norm = nn.LayerNorm(WIDTH)
        self.out = nn.Linear(WIDTH, WIDTH)

    def forward(
        self,
        context: torch.Tensor,
        hidden: torch.Tensor,
        order: torch.Tensor,
        padding: torch.Tensor,
    ) -> torch.Tensor:
        masked = torch.where(hidden.unsqueeze(-1), self.mask.expand_as(context), context)
        tokens = masked + self.position(order)
        return self.out(self.norm(self.body(tokens, src_key_padding_mask=padding)))


@dataclass
class Model:
    """The context encoder and the predictor, trained; the target encoder, their moving average."""

    encoder: SegmentEncoder
    predictor: SegmentPredictor
    target: SegmentEncoder

    def to(self, device: str | torch.device) -> Model:
        self.encoder.to(device)
        self.predictor.to(device)
        self.target.to(device)
        return self

    def trained_parameters(self) -> list[nn.Parameter]:
        return list(self.encoder.parameters()) + list(self.predictor.parameters())


def build(seed: int) -> Model:
    """The three modules from one seed, in the laboratory's order; the target a frozen copy."""
    torch.manual_seed(seed)
    encoder, predictor = SegmentEncoder(), SegmentPredictor()
    target = SegmentEncoder()
    target.load_state_dict(encoder.state_dict())
    for parameter in target.parameters():
        parameter.requires_grad_(False)
    return Model(encoder, predictor, target)


def masked_segment_loss(
    model: Model, batch: Batch, spread: list[float] | None = None
) -> torch.Tensor:
    """The mean squared error of the predicted hidden tokens against the layer-normed targets.

    Half precision (bfloat16) on a card, as the laboratory trained; full precision
    elsewhere. ``spread`` collects how much the targets differ between tokens, the
    laboratory's collapse check.
    """
    hidden = batch["hidden"] & ~batch["padding"]
    device = batch["padding"].device.type
    with torch.autocast(device, dtype=torch.bfloat16, enabled=device == "cuda"):
        context = model.encoder(batch, hidden)
        with torch.no_grad():
            target = F.layer_norm(model.target(batch).float(), (WIDTH,))
        predicted = model.predictor(context, hidden, batch["order"], batch["padding"]).float()
    if spread is not None:
        spread.append(float(target[hidden].std(0).mean()))
    error = ((predicted - target) ** 2).mean(-1)
    return error[hidden].mean()


#: Every entry's date in a weights file: the format's earliest, so the bytes do not
#: carry the time they were written.
_FIXED_DATE = (1980, 1, 1, 0, 0, 0)


def encoder_bytes(encoder: nn.Module) -> bytes:
    """The encoder's weights as an ``.npz`` that ``numpy.load`` reads: same weights, same bytes.

    One float32 array per parameter or buffer, by name in sorted order, stored
    uncompressed with fixed entry dates, so the sha256 of the file names the
    weights themselves and becomes the configuration's name.
    """
    sink = io.BytesIO()
    state = encoder.state_dict()
    with zipfile.ZipFile(sink, "w", compression=zipfile.ZIP_STORED) as archive:
        for name in sorted(state):
            buffer = io.BytesIO()
            array = state[name].detach().to("cpu", torch.float32).numpy()
            np.save(buffer, np.ascontiguousarray(array), allow_pickle=False)
            archive.writestr(
                zipfile.ZipInfo(f"{name}.npy", date_time=_FIXED_DATE), buffer.getvalue()
            )
    return sink.getvalue()


def _arrays(blob: bytes) -> Mapping[str, Any]:
    with np.load(io.BytesIO(blob), allow_pickle=False) as data:
        return {name: data[name] for name in data.files}


def encoder_from_bytes(blob: bytes) -> SegmentEncoder:
    """A segment encoder holding the weights :func:`encoder_bytes` wrote, refused if any is missing."""
    encoder = SegmentEncoder()
    arrays = _arrays(blob)
    state = {name: torch.from_numpy(np.array(array)) for name, array in arrays.items()}
    encoder.load_state_dict(state, strict=True)
    return encoder
