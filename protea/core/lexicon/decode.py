"""The cut and the theme mass of a batch of sequences, as the laboratory computed them.

- **The cut** (``cortes_viterbi``): each protein's candidates, the forward in the
  max semiring with every alpha stored, and the traceback. Out of it, the
  segments (a maximal run of one theme; a new hit of the same theme starts a new
  one) and every residue's column on the path.
- **The theme mass** (``vectores_posterior``): each protein's candidates under a
  budget of states per residue, the forward and backward in the sum semiring,
  and the expected residues each theme's match states emit, over the protein's
  length.

Sequences are decoded in batches of padded residues, and each batch in chunks
whose stored states fit a memory budget, proteins with few states first. A
protein's result does not depend on its batch or chunk: every kernel runs one
protein per program.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass

import numpy as np
import torch

from protea.core.lexicon import candidates as cs
from protea.core.lexicon.model import Lexicon


@dataclass(frozen=True)
class Budget:
    """How much a decode may hold.

    ``states_per_residue`` and ``max_gb`` bound each protein's candidate states (the
    laboratory's defaults: unbounded for the cut, 4 for the theme mass, and 8 GB);
    ``residues`` bounds a batch's padded residues and ``chunk_gb`` a chunk's stored
    states.
    """

    states_per_residue: float
    max_gb: float = 8.0
    residues: int = 300_000
    max_batch: int = 512
    chunk_gb: float = 24.0


CUT_BUDGET = Budget(states_per_residue=1e6, chunk_gb=40.0)
MASS_BUDGET = Budget(states_per_residue=4.0, chunk_gb=24.0)


def segments(labels: np.ndarray, starts: np.ndarray, K: int) -> list[tuple[int, int, int]]:
    """``[(start, end, theme)]``: the maximal runs of one theme, a new hit starting a new one."""
    n = len(labels)
    if n == 0:
        return []
    themed = labels != K
    new = np.ones(n, dtype=bool)
    new[1:] = (labels[1:] != labels[:-1]) | starts[1:]
    b = np.flatnonzero(new)
    e = np.append(b[1:], n)
    keep = themed[b]
    return list(
        zip(b[keep].tolist(), e[keep].tolist(), labels[b[keep]].astype(int).tolist(), strict=True)
    )


def _batches(lengths: Sequence[int], budget: Budget) -> Iterator[list[int]]:
    """Consecutive rows (already in length order) under the padded-residue budget."""
    k0, N = 0, len(lengths)
    while k0 < N:
        k1 = k0 + 1
        while (
            k1 < N
            and k1 - k0 < budget.max_batch
            and (k1 - k0 + 1) * max(lengths[k1], 1) <= budget.residues
        ):
            k1 += 1
        yield list(range(k0, k1))
        k0 = k1


def _chunks(
    counts: list[int], lens: list[int], bytes_per: float, cap_gb: float
) -> Iterator[list[int]]:
    """A batch's rows, fewest states first, in chunks whose stored states fit ``cap_gb``."""
    chunk: list[int] = []
    for j in sorted(range(len(counts)), key=lambda r: counts[r]):
        trial = chunk + [j]
        size = len(trial) * max(lens[t] for t in trial) * max(max(counts[t] for t in trial), 1)
        if chunk and size * bytes_per / 1e9 > cap_gb:
            yield chunk
            chunk = [j]
        else:
            chunk = trial
    if chunk:
        yield chunk


def _decode(
    lex: Lexicon,
    sequences: Sequence[str],
    run: tuple[str, Budget, float],
    fn: Callable[[list[int], torch.Tensor, torch.Tensor, dict[str, torch.Tensor]], None],
) -> None:
    """Candidates per batch, then ``fn(rows, X, lens, tables)`` per chunk."""
    device, budget, bytes_per = run
    lengths = [len(s) for s in sequences]
    for rows in _batches(lengths, budget):
        x, lens = cs.batch_of([sequences[r] for r in rows], device)
        cand, cmask, _ = cs.candidates(lex, x, lens, (budget.states_per_residue, budget.max_gb))
        counts = cs.state_counts(lex, cand, cmask).tolist()
        for chunk in _chunks(
            counts, [max(n, 1) for n in lengths[rows[0] : rows[-1] + 1]], bytes_per, budget.chunk_gb
        ):
            idx = torch.tensor(chunk, device=x.device)
            c_, m_ = cs.trim(cand[idx], cmask[idx])
            sub_lens = lens[idx]
            X = x[idx][:, : int(sub_lens.max())].clamp(max=19).to(torch.int32).contiguous()
            fn(
                [rows[t] for t in chunk],
                X,
                sub_lens.to(torch.int32).contiguous(),
                cs.prepare(lex, c_, m_),
            )


@torch.no_grad()
def cut(
    lex: Lexicon, sequences: Sequence[str], device: str, budget: Budget = CUT_BUDGET
) -> list[dict]:
    """Every sequence's segments and its residues' columns, in input order.

    ``sequences`` must be in length order (the bank's order is).
    """
    from protea.core.lexicon.kernels_dp import launch_forward
    from protea.core.lexicon.kernels_trace import traceback

    out: list[dict] = [{} for _ in sequences]

    def on_chunk(
        rows: list[int], X: torch.Tensor, lens: torch.Tensor, tables: dict[str, torch.Tensor]
    ) -> None:
        Z, AM, AI, AD, AGE, _ = launch_forward(tables, X, lens, maxmode=True)
        labels, starts, columns = traceback((AM, AI, AD, AGE), tables, lens, lex.K)
        labels_h, starts_h, columns_h = (
            labels.cpu().numpy(),
            starts.cpu().numpy(),
            columns.cpu().numpy(),
        )
        for j, r in enumerate(rows):
            n = int(lens[j])
            out[r] = {
                "segments": segments(labels_h[j, :n], starts_h[j, :n], lex.K),
                "columns": columns_h[j, :n].copy(),
                "best_bits": float(-Z[j]) / float(np.log(2)),
            }

    _decode(lex, sequences, (device, budget, 12.0), on_chunk)
    return out


@torch.no_grad()
def theme_mass(
    lex: Lexicon, sequences: Sequence[str], device: str, budget: Budget = MASS_BUDGET
) -> list[tuple[np.ndarray, np.ndarray]]:
    """Every sequence's theme mass as (themes, shares), sparse and sorted by theme, in input order."""
    from protea.core.lexicon.kernels_dp import match_counts

    out: list[tuple[np.ndarray, np.ndarray]] = [
        (np.zeros(0, np.int32), np.zeros(0, np.float16))
    ] * len(sequences)

    def on_chunk(
        rows: list[int], X: torch.Tensor, lens: torch.Tensor, tables: dict[str, torch.Tensor]
    ) -> None:
        DE = match_counts(tables, X, lens)
        occ = torch.where(tables["used"], DE.sum(1), torch.zeros_like(DE[:, 0]))
        mass = torch.zeros(len(rows), lex.K, device=X.device).scatter_add_(
            1, tables["theme"], occ.float()
        )
        share = (mass / lens[:, None].float()).cpu().numpy().astype(np.float16)
        for j, r in enumerate(rows):
            themes = np.flatnonzero(share[j]).astype(np.int32)
            out[r] = (themes, share[j, themes])

    _decode(lex, sequences, (device, budget, 8.0), on_chunk)
    return out
