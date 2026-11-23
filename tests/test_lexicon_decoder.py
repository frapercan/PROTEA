"""The lexicon decoder (ADR-D63): its tables, candidates and segments on the CPU, its kernels on a card.

The CPU tests build a tiny lexicon by hand. The card tests (skipped without CUDA and
Triton) check the Triton forward against a direct PyTorch reference of the same
recursion, the traceback's path against the best score, and the theme mass against
its own definition. Parity with the laboratory's code was measured on photo 211 and
is recorded in the bank's manifest; it cannot run here, because the laboratory's
scripts are not part of the platform.
"""

from __future__ import annotations

import importlib.util
import math

import numpy as np
import pytest
import torch

from protea.core.lexicon import candidates as cs
from protea.core.lexicon.decode import segments
from protea.core.lexicon.model import NEG, hit_costs_bits, lexicon_from_state

K, M = 3, 6


def _state(seed: int = 0, lengths: tuple[int, ...] = (6, 4, 5)) -> dict[str, torch.Tensor]:
    g = torch.Generator().manual_seed(seed)
    return {
        "U": torch.randn(20, 2, generator=g),
        "V": torch.randn(K, M, 2, generator=g),
        "b": torch.log(torch.full((20,), 0.05)),
        "t_match": torch.randn(K, M, 4, generator=g),
        "t_ins": torch.randn(K, M, 2, generator=g),
        "t_del": torch.randn(K, M, 2, generator=g),
        "entry": torch.randn(K, M, generator=g),
        "name": torch.randn(K, generator=g),
        "g": torch.tensor([3.0, -3.0]),
        "e": torch.tensor([-2.0, 2.0]),
        "logq": torch.log(torch.full((20,), 0.05)),
        "length": torch.tensor(lengths),
        "insert_cap": torch.tensor(0, dtype=torch.uint8),
    }


ARGS = {"themes": K, "columns": M, "emission": "neural", "rank": 2, "per_column": True}


def test_every_theme_ends_at_its_last_active_column() -> None:
    lex = lexicon_from_state(_state(), ARGS)
    mm, mi, md, me, ii, im, dd, dm, ent = lex.tables
    for t, ln in enumerate((6, 4, 5)):
        last = ln - 1
        assert mm[t, last] == NEG and mi[t, last] == NEG and md[t, last] == NEG
        assert me[t, last] == 0.0
        assert torch.all(dd[t, last:] == NEG)
        assert torch.allclose(ent[t, :ln].exp().sum(), torch.tensor(1.0), atol=1e-6)
        assert torch.all(ent[t, ln:] < -1e8)
    assert torch.allclose(lex.log_emissions.exp().sum(-1), torch.ones(K, M), atol=1e-5)
    assert torch.isfinite(hit_costs_bits(lex)).all()


def test_the_insert_cap_never_lets_an_insert_stay_cheaper_than_the_background() -> None:
    state = _state()
    state["t_ins"] = torch.tensor([10.0, -10.0]).repeat(K, M, 1)
    state["insert_cap"] = torch.tensor(1, dtype=torch.uint8)
    lex = lexicon_from_state(state, ARGS)
    stay = torch.log_softmax(state["g"], -1)[0]
    assert torch.all(lex.tables[4] <= stay + 1e-6)


def test_tokens_read_any_other_letter_as_the_last_amino_acid() -> None:
    assert cs.tokens("ACy").tolist() == [0, 1, 19]
    assert cs.tokens("XUB").tolist() == [19, 19, 19]
    x, lens = cs.batch_of(["AC", "WWW", ""], "cpu")
    assert x.tolist() == [[0, 1, cs.PAD], [18, 18, 18], [cs.PAD] * 3]
    assert lens.tolist() == [2, 3, 1]


def test_a_theme_competes_only_inside_its_own_region() -> None:
    gain = torch.tensor([[10.0, 5.0, 2.0, -1.0]])
    starts = torch.tensor([[0, 0, 50, 0]])
    ends = torch.tensor([[20, 20, 60, 20]])
    keep, deficit = cs.region_competition(gain, starts, ends, torch.tensor([100]), 0.99)
    # theme 1 is 5 bits below theme 0 in the same region (within 6.6), theme 2 alone in
    # its own region, theme 3 has a negative gain.
    assert keep.tolist() == [[True, True, True, False]]
    assert deficit[0, :3].tolist() == [0.0, 5.0, 0.0]
    far = torch.tensor([[10.0, 1.0]])
    keep, _ = cs.region_competition(
        far, torch.tensor([[0, 0]]), torch.tensor([[20, 20]]), torch.tensor([100]), 0.99
    )
    assert keep.tolist() == [[True, False]]


def test_the_state_budget_keeps_region_bests_first() -> None:
    lex = lexicon_from_state(_state(), ARGS)
    keep = torch.tensor([[True, True, True]])
    deficit = torch.tensor([[3.0, 0.0, 0.0]])
    out = cs.budget_per_protein(lex, keep, deficit, torch.tensor([9]))
    # lengths 6, 4, 5: the two region bests (4 + 5 = 9 states) fit, the third does not.
    assert out.tolist() == [[False, True, True]]


def test_the_states_are_the_candidates_columns_end_to_end() -> None:
    lex = lexicon_from_state(_state(), ARGS)
    cand = torch.tensor([[1, 2], [0, 0]])
    cmask = torch.tensor([[True, True], [True, False]])
    theme, colj, used, cms = cs.ragged_index(lex, cand, cmask)
    assert cms.tolist() == [9, 6]
    assert theme[0, :9].tolist() == [1] * 4 + [2] * 5
    assert colj[0, :9].tolist() == [0, 1, 2, 3, 0, 1, 2, 3, 4]
    assert used[1].tolist() == [True] * 6 + [False] * 3
    f = cs.flags(colj, lex.length[theme], used)
    assert f[0, :9].tolist() == [1, 0, 0, 2, 1, 0, 0, 0, 2]
    assert f[1, 6:].tolist() == [3, 3, 3]


def test_segments_are_runs_of_one_theme_and_a_new_hit_starts_a_new_one() -> None:
    labels = np.array([3, 0, 0, 0, 1, 1, 3, 1, 1])
    starts = np.array([0, 1, 0, 1, 1, 0, 0, 1, 0], dtype=bool)
    assert segments(labels, starts, 3) == [(1, 3, 0), (3, 4, 0), (4, 6, 1), (7, 9, 1)]
    assert segments(np.array([], dtype=int), np.array([], dtype=bool), 3) == []


# On a card -----------------------------------------------------------------------

needs_card = pytest.mark.skipif(
    not torch.cuda.is_available() or importlib.util.find_spec("triton") is None,
    reason="needs a CUDA card and Triton",
)


def _reference(lex, x: list[int], cand: list[int], maxmode: bool) -> float:
    """log P(x) or the best path's score over the candidates, the recursion written directly."""
    op = (lambda a, b: max(a, b)) if maxmode else (lambda a, b: float(np.logaddexp(a, b)))
    mm, mi, md, me, ii, im, dd, dm, ent = (t.double().cpu() for t in lex.tables)
    E = lex.log_emissions.double().cpu()
    nm, lq = lex.log_names.double().cpu(), lex.logq.double().cpu()
    g0, g1, e0, e1 = (float(v) for v in lex.ge)
    states = [(c, j) for c in cand for j in range(int(lex.length[c]))]
    Mx = {s: NEG for s in states}
    Ix = dict(Mx)
    Dx = dict(Mx)
    G, End = 0.0, NEG
    for xi in x:
        Begin = op(G + g1, End + e0)
        Gn = float(lq[xi]) + op(G + g0, End + e1)
        nM, nI, nD = {}, {}, {}
        for c, j in states:
            fp = NEG
            if j > 0:
                fp = op(
                    op(Mx[(c, j - 1)] + mm[c, j - 1], Ix[(c, j - 1)] + im[c, j - 1]),
                    Dx[(c, j - 1)] + dm[c, j - 1],
                )
            nM[(c, j)] = float(E[c, j, xi]) + op(Begin + float(nm[c] + ent[c, j]), fp)
            nI[(c, j)] = float(lq[xi]) + op(Mx[(c, j)] + mi[c, j], Ix[(c, j)] + ii[c, j])
        for c, j in states:
            nD[(c, j)] = (
                NEG if j == 0 else op(nD[(c, j - 1)] + dd[c, j - 1], nM[(c, j - 1)] + md[c, j - 1])
            )
        End = NEG
        for s in states:
            End = op(End, nM[s] + me[s])
        Mx, Ix, Dx, G = nM, nI, nD, Gn
    return op(G, End)


@needs_card
@pytest.mark.parametrize("maxmode", [False, True])
def test_the_triton_forward_is_the_recursion(maxmode: bool) -> None:
    from protea.core.lexicon.kernels_dp import launch_forward

    lex = lexicon_from_state({k: v.cuda() for k, v in _state(seed=3).items()}, ARGS)
    seqs = ["MKVLAAGIL", "WYC"]
    x, lens = cs.batch_of(seqs, "cuda")
    cand = torch.tensor([[0, 2], [1, 0]], device="cuda")
    cmask = torch.tensor([[True, True], [True, False]], device="cuda")
    tables = cs.prepare(lex, cand, cmask)
    X = x.clamp(max=19).to(torch.int32).contiguous()
    Z = launch_forward(tables, X, lens.to(torch.int32).contiguous(), maxmode)[0].cpu()
    want = [
        _reference(lex, cs.tokens(seqs[0]).tolist(), [0, 2], maxmode),
        _reference(lex, cs.tokens(seqs[1]).tolist(), [1], maxmode),
    ]
    assert Z.tolist() == pytest.approx(want, abs=1e-3)


@needs_card
def test_the_cut_covers_every_residue_and_the_mass_is_a_share() -> None:
    from protea.core.lexicon.decode import Budget, cut, theme_mass

    lex = lexicon_from_state({k: v.cuda() for k, v in _state(seed=5).items()}, ARGS)
    seqs = sorted(["MKVLAAGILWW", "ACDEFGHIK", "W"], key=len)
    out = cut(lex, seqs, "cuda", Budget(states_per_residue=1e6, chunk_gb=0.1))
    for seq, rec in zip(seqs, out, strict=True):
        assert len(rec["columns"]) == len(seq)
        for start, end, theme in rec["segments"]:
            assert 0 <= start < end <= len(seq) and 0 <= theme < K
        assert math.isfinite(rec["best_bits"])
    mass = theme_mass(lex, seqs, "cuda", Budget(states_per_residue=1e6, chunk_gb=0.1))
    for themes, shares in mass:
        assert np.all(shares >= 0) and float(shares.astype(np.float32).sum()) <= 1.0 + 1e-2
        assert np.all(np.diff(themes) > 0)
