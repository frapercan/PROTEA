"""Each protein's candidate themes and the states the exact passes visit.

The laboratory's ``vectores_posterior``/``cortes_viterbi`` candidates, ported:

- every (protein, theme) gets the best ungapped local score of the theme over
  the protein (``kernels_filter.ungapped_two_pass``); its gain is that score less
  the theme's hit cost, in bits;
- a theme with a non-negative gain competes only with the themes whose best run
  is centred inside its own (``region_competition``): it stays if it is within
  -log2(1 - keep) bits of the best of them;
- the survivors fit a state budget per protein, region bests first
  (``budget_per_protein``);
- a protein's states are its candidates' active columns laid end to end
  (``ragged_index``), with the tables gathered onto them (``prepare``).

Pure PyTorch: the filter's kernels are imported where the filter runs.
"""

from __future__ import annotations

import math
from collections.abc import Sequence

import numpy as np
import torch

from protea.core.lexicon.model import CAND_KEEP, NEG, Lexicon, hit_costs_bits

ALPHABET = "ACDEFGHIKLMNPQRSTVWY"
#: The padding token of the laboratory's packs (``step54.PAD``).
PAD = 20
#: Any other letter reads as the last amino acid, as the laboratory's packs do.
LUT = np.full(256, 19, dtype=np.uint8)
for _i, _a in enumerate(ALPHABET):
    LUT[ord(_a)] = _i


def tokens(sequence: str) -> np.ndarray:
    """A sequence as the decoder reads it: upper case, one token per residue."""
    return LUT[np.frombuffer(sequence.upper().encode(), dtype=np.uint8)].astype(np.int64)


def batch_of(sequences: Sequence[str], device: str) -> tuple[torch.Tensor, torch.Tensor]:
    """``(x, lens)``: the batch padded with PAD, and every protein's length (at least 1)."""
    longest = max((len(s) for s in sequences), default=1)
    x = torch.full((len(sequences), max(longest, 1)), PAD, dtype=torch.long)
    for row, sequence in enumerate(sequences):
        if sequence:
            x[row, : len(sequence)] = torch.from_numpy(tokens(sequence))
    lens = torch.tensor([max(len(s), 1) for s in sequences])
    return x.to(device), lens.to(device)


def _range_max_table(values: torch.Tensor, L: int) -> torch.Tensor:
    """(levels, B, L) sparse table of running maxima over spans of 2**level positions."""
    table = [values]
    k = 1
    while (1 << k) <= L:
        prev, half = table[-1], 1 << (k - 1)
        nxt = prev.clone()
        nxt[:, : L - half] = torch.maximum(prev[:, : L - half], prev[:, half:])
        table.append(nxt)
        k += 1
    return torch.stack(table)


@torch.no_grad()
def region_competition(
    gain: torch.Tensor, starts: torch.Tensor, ends: torch.Tensor, lens: torch.Tensor, keep: float
) -> tuple[torch.Tensor, torch.Tensor]:
    """``(keep mask, deficit)``, (B, K) each: the themes that survive in their region.

    A theme stays if its gain is within -log2(1 - keep) bits of the best gain among
    the themes whose best run is centred inside its own best run. The deficit is
    how far below that best it is (infinite for the themes that did not stay).
    """
    B, K = gain.shape
    L = max(int(lens.max()), 1)
    delta = -math.log2(1.0 - keep)
    pos = gain >= 0
    s_ = starts.long().clamp(0, L - 1)
    e_ = torch.maximum(ends.long().clamp(0, L - 1), s_)
    centre = (s_ + e_) // 2
    first = torch.full((B, L), float("-inf"), device=gain.device)
    first.scatter_reduce_(
        1, centre, torch.where(pos, gain, torch.full_like(gain, float("-inf"))), reduce="amax"
    )
    tab = _range_max_table(first, L)
    span = (e_ - s_ + 1).float()
    lev = torch.floor(torch.log2(span)).long().clamp(min=0, max=tab.shape[0] - 1)
    bidx = torch.arange(B, device=gain.device)[:, None].expand(B, K)
    left = tab[lev, bidx, s_]
    right = tab[lev, bidx, (e_ - (1 << lev) + 1).clamp(min=0)]
    best_here = torch.maximum(left, right)
    keepm = pos & (gain >= best_here - delta)
    return keepm, (best_here - gain).masked_fill(~keepm, float("inf"))


@torch.no_grad()
def budget_per_protein(
    lex: Lexicon, keepm: torch.Tensor, deficit: torch.Tensor, budget: torch.Tensor
) -> torch.Tensor:
    """The kept themes that fit each protein's state budget, region bests first."""
    order = torch.argsort(deficit, dim=1)
    kept = torch.gather(keepm, 1, order)
    ln = lex.length[order] * kept
    fits = torch.cumsum(ln, 1) <= budget[:, None]
    fits[:, 0] |= kept[:, 0]
    out = torch.zeros_like(keepm)
    out.scatter_(1, order, fits & kept)
    return out


@torch.no_grad()
def candidates(
    lex: Lexicon, x: torch.Tensor, lens: torch.Tensor, budget: tuple[float, float]
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """``(cand, cmask, n)``: each protein's candidate themes, best gain first.

    ``budget`` is (states per residue, gigabytes of stored states per protein).
    """
    from protea.core.lexicon.kernels_filter import ungapped_two_pass

    states_per_residue, max_gb = budget
    cost = hit_costs_bits(lex)
    sc, st, en = ungapped_two_pass(lex, x, lens, cost)
    gain = sc - cost[None, :]
    keepm, deficit = region_competition(gain, st, en, lens, CAND_KEEP)
    cap = torch.minimum(
        states_per_residue * lens.float(), max_gb * 1e9 / (16.0 * lens.float())
    ).long()
    keepm = budget_per_protein(lex, keepm, deficit, cap)
    n = keepm.sum(1)
    C = max(int(n.max()), 1)
    cand = torch.argsort(gain.masked_fill(~keepm, float("-inf")), dim=1, descending=True)[:, :C]
    cmask = torch.arange(C, device=x.device)[None] < n[:, None]
    return cand, cmask, n


def state_counts(lex: Lexicon, cand: torch.Tensor, cmask: torch.Tensor) -> torch.Tensor:
    """(B,) each protein's number of states: the sum of its candidates' lengths."""
    return torch.where(cmask, lex.length[cand], torch.zeros_like(cand)).sum(1)


def trim(cand: torch.Tensor, cmask: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
    """The candidate columns no protein of these rows uses, dropped."""
    width = int(cmask.sum(1).max().clamp(min=1))
    return cand[:, :width], cmask[:, :width]


@torch.no_grad()
def ragged_index(
    lex: Lexicon, cand: torch.Tensor, cmask: torch.Tensor
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    """Per protein, its candidates' active columns end to end: theme, column, used, count."""
    B, C = cand.shape
    dev = cand.device
    lens_c = torch.where(cmask, lex.length[cand], torch.zeros_like(cand))
    cms = lens_c.sum(1)
    S = max(int(cms.max()), 1)
    flat_len = lens_c.flatten()
    nz = flat_len > 0
    reps = flat_len[nz]
    owner = torch.arange(B * C, device=dev)[nz]
    st_owner = torch.repeat_interleave(owner, reps)
    first = torch.cumsum(reps, 0) - reps
    colj_t = torch.arange(int(reps.sum()), device=dev) - torch.repeat_interleave(first, reps)
    b_t = st_owner // C
    start = (torch.cumsum(lens_c, 1) - lens_c).flatten()
    pos_t = start[st_owner] + colj_t
    theme = torch.zeros(B, S, dtype=torch.long, device=dev)
    colj = torch.zeros(B, S, dtype=torch.long, device=dev)
    theme[b_t, pos_t] = cand.flatten()[st_owner]
    colj[b_t, pos_t] = colj_t
    used = torch.arange(S, device=dev)[None] < cms[:, None]
    return theme, colj, used, cms


def flags(colj: torch.Tensor, length: torch.Tensor, used: torch.Tensor) -> torch.Tensor:
    """1 for a theme's first column, 2 for its last; padding states are both."""
    f = (colj == 0).to(torch.int8) | ((colj == length - 1).to(torch.int8) << 1)
    return torch.where(used, f, torch.full_like(f, 3)).contiguous()


@torch.no_grad()
def prepare(lex: Lexicon, cand: torch.Tensor, cmask: torch.Tensor) -> dict[str, torch.Tensor]:
    """The tables gathered onto every protein's states, as the exact passes read them.

    E (B, 20, S), TR (B, 8, S) = mm, mi, md, me, ii, im, dd, dm, ENTW (B, S), COLF
    (B, S), CMS (B,), GE (4,), LQ (20,), and the theme, column and used of every state.
    """
    theme, colj, used, cms = ragged_index(lex, cand, cmask)
    E = lex.log_emissions[theme, colj].transpose(1, 2)
    TR = torch.stack([t[theme, colj] for t in lex.tables[:8]], 1)
    ENTW = torch.where(
        used,
        lex.log_names[theme] + lex.tables[8][theme, colj],
        torch.full_like(theme, 0).float() + NEG,
    )
    return {
        "E": E.contiguous().float(),
        "TR": TR.contiguous().float(),
        "ENTW": ENTW.contiguous().float(),
        "COLF": flags(colj, lex.length[theme], used),
        "CMS": cms.to(torch.int32).contiguous(),
        "GE": lex.ge,
        "LQ": lex.logq,
        "theme": theme,
        "colj": colj,
        "used": used,
    }
