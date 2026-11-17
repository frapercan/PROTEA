"""A lexicon read for decoding: every table in log-probabilities, nothing trainable.

The laboratory's segmental model (``step57_segmental.Segmental``) is one hidden
Markov model per protein: a background G that emits the composition q, and K
themes, each a profile of up to M columns with match, insert and delete states.

    G --g1--> Begin --name_t + entry_t(j)--> M_tj
    M_tj -> M_t,j+1 | I_tj | D_t,j+1 | End;  I_tj -> I_tj | M_t,j+1;  D_tj -> D_t,j+1 | M_t,j+1
    End --e0--> Begin (a new hit right away) | --e1--> G

This module computes, once per lexicon, exactly the tables the laboratory's
``Segmental.tables()``, ``logP_like()`` and ``segmental_triton._scaled`` give at the
honest code (temperature 1): the emissions (K, M, 20), the eight transitions and
the entries (K, M) with every theme's end applied, the names (K,), the background
and restart moves (g0, g1, e0, e1) and the composition q. ``hit_costs_bits`` is the
laboratory's ``hit_costs_bits``: what opening a hit with each theme costs.
"""

from __future__ import annotations

import math
import pathlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import torch
import torch.nn.functional as F  # noqa: N812  PyTorch convention

NEG = -1.0e9
LN2 = math.log(2.0)
#: The region competition's keep (``step57 --cand-keep``): 6.6 bits at 0.99.
CAND_KEEP = 0.99


@dataclass(frozen=True)
class Lexicon:
    """The decoding tables of one lexicon, on one device."""

    K: int
    M: int
    #: (K, M, 20) log emissions of the match states.
    log_emissions: torch.Tensor
    #: mm, mi, md, me, ii, im, dd, dm and the entries, (K, M) each.
    tables: tuple[torch.Tensor, ...]
    #: (K,) the themes' shares, log-softmaxed.
    log_names: torch.Tensor
    #: (4,) G -> G, G -> Begin, End -> Begin, End -> G.
    ge: torch.Tensor
    #: (20,) the background composition.
    logq: torch.Tensor
    #: (K,) every theme's active columns.
    length: torch.Tensor
    step: int | None
    args: dict[str, Any]


def _emissions(state: dict[str, torch.Tensor], neural: bool) -> torch.Tensor:
    if neural:
        return F.log_softmax(state["V"] @ state["U"].T + state["b"], -1)
    return F.log_softmax(state["emit"], -1)


def _column_masks(length: torch.Tensor, M: int) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """(K, M): the active columns, every theme's last one, and that one and beyond."""
    j = torch.arange(M, device=length.device)[None]
    ln = length[:, None]
    return j < ln, j == ln - 1, j >= ln - 1


def _moves(state: dict[str, torch.Tensor], shape: tuple[int, int]) -> list[torch.Tensor]:
    """The match, insert and delete moves, log-softmaxed and expanded to (K, M, n)."""
    K, M = shape
    out = []
    for name in ("t_match", "t_ins", "t_del"):
        t = F.log_softmax(state[name], -1)
        out.append(t if t.dim() == 3 else t[:, None].expand(K, M, t.shape[-1]))
    return out


def _capped_inserts(ti: torch.Tensor, g: torch.Tensor) -> torch.Tensor:
    """An insert self-loop never cheaper than the background's stay (the lab's insert cap)."""
    ii_ = torch.minimum(ti[..., 0], F.log_softmax(g, -1)[0])
    return torch.stack([ii_, torch.log1p(-ii_.exp().clamp(max=1.0 - 1e-7))], -1)


def transition_tables(
    state: dict[str, torch.Tensor], length: torch.Tensor, M: int, insert_cap: bool
) -> tuple[torch.Tensor, ...]:
    """``Segmental.tables()``: mm, mi, md, me, ii, im, dd, dm and the entries, (K, M) each."""
    K = int(length.shape[0])
    tm, ti, td = _moves(state, (K, M))
    if insert_cap:
        ti = _capped_inserts(ti, state["g"])
    active, last, past = _column_masks(length, M)
    neg = torch.full((K, M), NEG, device=tm.device)
    mm = torch.where(last, neg, tm[..., 0])
    mi = torch.where(last, neg, tm[..., 1])
    md = torch.where(last, neg, tm[..., 2])
    me = torch.where(last, torch.zeros_like(neg), tm[..., 3])
    ii = ti[..., 0]
    im = torch.where(last, neg, ti[..., 1])
    dd = torch.where(past, neg, td[..., 0])
    dm = torch.where(last, neg, td[..., 1])
    ent = F.log_softmax(state["entry"].masked_fill(~active, NEG), -1)
    return mm, mi, md, me, ii, im, dd, dm, ent


def lexicon_from_state(
    state: dict[str, torch.Tensor], args: dict[str, Any], step: int | None = None
) -> Lexicon:
    """The decoding tables of a checkpoint's state dict and its training arguments."""
    K, M = int(args["themes"]), int(args["columns"])
    length = state.get("length")
    if length is None:  # checkpoints from before variable lengths: every theme has all columns
        length = torch.full((K,), M, dtype=torch.long, device=state["entry"].device)
    cap = bool(int(state["insert_cap"])) if "insert_cap" in state else False
    with torch.no_grad():
        ge = torch.cat([F.log_softmax(state["g"], -1), F.log_softmax(state["e"], -1)])
        return Lexicon(
            K=K,
            M=M,
            log_emissions=_emissions(state, args.get("emission") == "neural").contiguous().float(),
            tables=tuple(t.contiguous().float() for t in transition_tables(state, length, M, cap)),
            log_names=F.log_softmax(state["name"], -1).contiguous().float(),
            ge=ge.contiguous().float(),
            logq=state["logq"].contiguous().float(),
            length=length.to(torch.long),
            step=step,
            args=dict(args),
        )


def load_lexicon(path: Path, device: str) -> Lexicon:
    """A laboratory checkpoint (``modelo*.pt``), read for decoding on ``device``.

    Read with ``weights_only``: tensors and plain values only. The training
    arguments hold paths, so ``PosixPath`` is the one class allowed besides them.
    """
    with torch.serialization.safe_globals([pathlib.PosixPath]):
        ck = torch.load(path, map_location=device, weights_only=True)
    return lexicon_from_state(ck["model"], ck["args"], ck.get("step"))


def hit_costs_bits(lex: Lexicon) -> torch.Tensor:
    """(K,) what opening a hit with each theme costs, in bits.

    Leaving the background (G -> Begin), the theme's name, its cheapest entry column
    and coming back (End -> G). A hit pays off only if its emissions gain more than
    this over the background; the ungapped score is that gain on the best diagonal.
    """
    ent = lex.tables[-1].max(-1).values
    return -(lex.ge[1] + lex.log_names + ent + lex.ge[3]) / LN2
