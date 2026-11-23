"""The candidates' filter: the best ungapped local score of every (protein, theme).

Ported from the laboratory's ``segmental_triton`` (``_ungapped``, ``_ungapped_split``,
``ungapped``, ``ungapped_regions_of`` and ``ungapped_two_pass``), with the same sums
in the same order. One program per (protein, theme): lane s holds, at residue i,
column (i - s) mod BLOCK, so the next residue moves every diagonal one column on
with no shuffle. A letter outside the 20 resets every lane.

The split kernel serves the themes longer than half their block without idle
lanes: the first MAIN columns rotate over MAIN lanes and the rest over a short
tail. Its scores are those of the plain kernel, bit for bit. The number of warps
changes the speed, never the result.

Needs Triton and a card.
"""

from __future__ import annotations

import math

import torch
import triton
import triton.language as tl

from protea.core.lexicon.model import Lexicon

#: The split kernel's warps (the laboratory's default after measuring).
SPLIT_WARPS = 1


def block_of(states: int) -> int:
    """A kernel's block for a theme or a protein with this many states."""
    return max(16, triton.next_power_of_2(states + 1))


def warps_of(block: int) -> int:
    return 4 if block <= 1024 else 8


# The kernels' argument lists stay compact: one name per line would push them past the
# platform's length limit without making them clearer.
# fmt: off
@triton.jit
def _ungapped(
    X, LENS, LO, TH, TLEN, OUT, OS, OE, x_b, o_b, KS, M, lo_k, PB, PT, BLOCK: tl.constexpr,
    REGION: tl.constexpr, PAIRS: tl.constexpr,
):
    """Best ungapped score of a theme over a protein, and with REGION its best run's ends."""
    pid = tl.program_id(0).to(tl.int64)
    if PAIRS:
        b = tl.load(PB + pid).to(tl.int64)
        t = tl.load(PT + pid).to(tl.int64)
    else:
        b = pid // KS
        t = pid % KS
    th = tl.load(TH + t)
    tlen = tl.load(TLEN + t)
    n = tl.load(LENS + b)
    s = tl.arange(0, BLOCK)
    run = tl.zeros([BLOCK], dtype=tl.float32)
    best = tl.zeros([BLOCK], dtype=tl.float32)
    rs = tl.zeros([BLOCK], dtype=tl.int32)
    bs = tl.zeros([BLOCK], dtype=tl.int32)
    be = tl.zeros([BLOCK], dtype=tl.int32)
    base = LO + th * lo_k
    for i in range(0, n):
        xi = tl.load(X + b * x_b + i)
        j = (i + BLOCK - s) % BLOCK
        valid = (j < tlen) & (xi < 20)
        sc = tl.load(base + xi * M + j, mask=valid, other=0.0).to(tl.float32)
        if REGION:
            rs = tl.where(run <= 0.0, i.to(tl.int32), rs)  # type: ignore[attr-defined]  # a Triton scalar
        run = tl.where(valid, tl.maximum(run + sc, 0.0), 0.0)
        if REGION:
            better = run > best
            bs = tl.where(better, rs, bs)
            be = tl.where(better, i.to(tl.int32), be)  # type: ignore[attr-defined]  # a Triton scalar
        best = tl.maximum(best, run)
    top = tl.max(best, 0)
    o = pid if PAIRS else b * o_b + t
    tl.store(OUT + o, top)
    if REGION:
        lane = tl.argmax(best, 0)
        tl.store(OS + o, tl.sum(tl.where(s == lane, bs, 0), 0))
        tl.store(OE + o, tl.sum(tl.where(s == lane, be, 0), 0))


@triton.jit
def _ungapped_split(
    X, LENS, LO, TH, TLEN, OUT, x_b, o_b, KS, M, lo_k, MAIN: tl.constexpr, TAILP: tl.constexpr
):
    """``_ungapped``'s best score for a theme longer than MAIN columns, without idle lanes."""
    pid = tl.program_id(0).to(tl.int64)
    b = pid // KS
    t = pid % KS
    th = tl.load(TH + t)
    tlen = tl.load(TLEN + t)
    n = tl.load(LENS + b)
    s = tl.arange(0, MAIN)
    u = tl.arange(0, TAILP)
    run = tl.zeros([MAIN], dtype=tl.float32)
    best = tl.zeros([MAIN], dtype=tl.float32)
    trun = tl.zeros([TAILP], dtype=tl.float32)
    tbest = tl.zeros([TAILP], dtype=tl.float32)
    tail = tlen - MAIN
    base = LO + th * lo_k
    last = tl.sum(tl.zeros([MAIN], dtype=tl.float32), 0)
    for i in range(0, n):
        xi = tl.load(X + b * x_b + i)
        ok = xi < 20
        row = base + tl.minimum(xi, 19) * M
        j = (i + MAIN - s) % MAIN
        sc = tl.load(row + j).to(tl.float32)
        jt = (i + TAILP - u) % TAILP
        tvalid = ok & (jt < tail)
        tsc = tl.load(row + MAIN + jt, mask=tvalid, other=0.0).to(tl.float32)
        prev = tl.where(j == 0, 0.0, run)
        run = tl.where(ok, tl.maximum(prev + sc, 0.0), 0.0)
        best = tl.maximum(best, run)
        tprev = tl.where(jt == 0, last, trun)
        trun = tl.where(tvalid, tl.maximum(tprev + tsc, 0.0), 0.0)
        tbest = tl.maximum(tbest, trun)
        last = tl.sum(tl.where(j == MAIN - 1, run, 0.0), 0)
    top = tl.maximum(tl.max(best, 0), tl.max(tbest, 0))
    tl.store(OUT + b * o_b + t, top)


def log_odds_bits(lex: Lexicon) -> torch.Tensor:
    """(K, 20, M) the emissions against the background, in bits, one letter's row contiguous."""
    lo = (lex.log_emissions - lex.logq[None, None]) / math.log(2)
    return lo.transpose(1, 2).contiguous().float()


def _split_tail(bs: int, longest: int) -> int:
    return max(16, triton.next_power_of_2(max(longest - bs // 2, 1)))


def _scores_of_block(
    lex: Lexicon, x: torch.Tensor, lens: torch.Tensor, lo: torch.Tensor, pos: torch.Tensor
):
    """The best scores of the themes ``pos`` (all of one block) over the batch."""
    B = x.shape[0]
    th, tl_ = pos.contiguous(), lex.length[pos].to(torch.int64).contiguous()
    bs = block_of(int(tl_.max()))
    sub = torch.empty(B, pos.numel(), device=x.device, dtype=torch.float32)
    grid = (B * pos.numel(),)
    tailp = _split_tail(bs, int(tl_.max()))
    if bs >= 64 and tailp < bs // 2:
        _ungapped_split[grid](
            x,
            lens,
            lo,
            th,
            tl_,
            sub,
            x.stride(0),
            sub.stride(0),
            pos.numel(),
            lex.M,
            lo.stride(0),
            MAIN=bs // 2,
            TAILP=tailp,
            num_warps=SPLIT_WARPS,
        )
    else:
        _ungapped[grid](
            x,
            lens,
            lo,
            th,
            tl_,
            sub,
            sub,
            sub,
            x.stride(0),
            sub.stride(0),
            pos.numel(),
            lex.M,
            lo.stride(0),
            x,
            x,
            BLOCK=bs,
            REGION=False,
            PAIRS=False,
            num_warps=warps_of(bs),
        )
    return sub


@torch.no_grad()
def ungapped_scores(lex: Lexicon, x: torch.Tensor, lens: torch.Tensor) -> torch.Tensor:
    """(B, K) the best ungapped score of every theme over every protein, in bits."""
    lo = log_odds_bits(lex)
    out = torch.empty(x.shape[0], lex.K, device=x.device, dtype=torch.float32)
    if x.shape[0] == 0:
        return out
    blocks = torch.tensor([block_of(int(v)) for v in lex.length.tolist()], device=x.device)
    X, L = x.contiguous(), lens.contiguous()
    for bs in sorted(set(blocks.tolist())):
        pos = (blocks == bs).nonzero(as_tuple=True)[0]
        out[:, pos] = _scores_of_block(lex, X, L, lo, pos)
    return out


@torch.no_grad()
def ungapped_regions_of(lex: Lexicon, x: torch.Tensor, lens: torch.Tensor, pairs: torch.Tensor):
    """``(scores, starts, ends)`` of the listed (protein, theme) pairs, (P,) each."""
    P, dev = pairs.shape[0], x.device
    sc = torch.empty(P, device=dev, dtype=torch.float32)
    st = torch.zeros(P, device=dev, dtype=torch.int32)
    en = torch.zeros(P, device=dev, dtype=torch.int32)
    if P == 0:
        return sc, st, en
    lo = log_odds_bits(lex)
    th = torch.arange(lex.K, device=dev, dtype=torch.int64)
    tl_ = lex.length.to(torch.int64).contiguous()
    X, L = x.contiguous(), lens.contiguous()
    pb, pt = pairs[:, 0].contiguous(), pairs[:, 1].contiguous()
    blocks = torch.tensor([block_of(int(v)) for v in tl_.tolist()], device=dev)[pt]
    for bs in sorted(set(blocks.tolist())):
        sel = (blocks == bs).nonzero(as_tuple=True)[0]
        s_sc = torch.empty(sel.numel(), device=dev, dtype=torch.float32)
        s_st = torch.zeros(sel.numel(), device=dev, dtype=torch.int32)
        s_en = torch.zeros(sel.numel(), device=dev, dtype=torch.int32)
        _ungapped[(sel.numel(),)](
            X,
            L,
            lo,
            th,
            tl_,
            s_sc,
            s_st,
            s_en,
            X.stride(0),
            0,
            lex.K,
            lex.M,
            lo.stride(0),
            pb[sel].contiguous(),
            pt[sel].contiguous(),
            BLOCK=bs,
            REGION=True,
            PAIRS=True,
            num_warps=warps_of(bs),
        )
        sc[sel], st[sel], en[sel] = s_sc, s_st, s_en
    return sc, st, en


@torch.no_grad()
def ungapped_two_pass(lex: Lexicon, x: torch.Tensor, lens: torch.Tensor, cost: torch.Tensor):
    """``(scores (B, K), starts, ends)``: every pair's score, then the regions of the
    pairs whose gain over the theme's hit cost is not negative (the only ones the
    region competition reads); the other pairs' starts and ends are 0."""
    if lex.args.get("comp_bias"):
        raise ValueError("a lexicon trained with the composition correction is not supported")
    sc = ungapped_scores(lex, x, lens)
    pairs = ((sc - cost[None, :]) >= 0).nonzero()
    st = torch.zeros(sc.shape, device=sc.device, dtype=torch.int32)
    en = torch.zeros(sc.shape, device=sc.device, dtype=torch.int32)
    if pairs.shape[0]:
        _, s_, e_ = ungapped_regions_of(lex, x, lens, pairs)
        st[pairs[:, 0], pairs[:, 1]] = s_
        en[pairs[:, 0], pairs[:, 1]] = e_
    return sc, st, en
# fmt: on
