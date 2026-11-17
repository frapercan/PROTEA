"""The best path's traceback over the stored alphas: every residue's theme and column.

Ported from the laboratory's ``cortes_viterbi`` (``_end_argmax``, ``_traceback_kernel``
and ``traceback_triton``): one program per protein walks from the last residue
back, with the same comparisons on the same stored alphas as the forward in the
max semiring left them. The kernel is split into jitted pieces that Triton
inlines; ties resolve as the laboratory's do.

A residue's label is its theme, or K for the background. Its column on the path
is the column for a match, -2 - the column for an insert, and -1 for the
background. A hit start marks where the path entered a theme from Begin.
"""

from __future__ import annotations

import torch
import triton
import triton.language as tl

#: Steps a walk may take per residue before it is stopped (a tie walking in circles).
_MAX_STEP = 300


# The kernels' argument lists stay compact: one name per line would push them past the
# platform's length limit without making them clearer.
# fmt: off
@triton.jit
def _end_argmax(row, me, cms, TILE: tl.constexpr):
    """The state k < cms that maximises row[k] + me[k] (the first one on ties)."""
    kk = tl.arange(0, TILE)
    best = tl.full([], -3.0e38, tl.float32)
    bk = tl.zeros([], tl.int64)
    for t in range(0, tl.cdiv(cms, TILE)):
        ks = t * TILE + kk
        ok = ks < cms
        v = tl.load(row + ks, mask=ok, other=0.0) + tl.load(me + ks, mask=ok, other=0.0)
        v = tl.where(ok, v, -3.0e38)
        m = tl.max(v, 0)
        a = tl.argmax(v, 0)
        upd = m > best
        bk = tl.where(upd, (t * TILE + a).to(tl.int64), bk)
        best = tl.where(upd, m, best)
    return bk


@triton.jit
def _emit(lab, col, th, cj, i, k, typ, K):
    """The residue's label and column on the path, when the state emits it."""
    isG = typ == 0
    isM = typ == 1
    labv = tl.where(isG, K, tl.load(th + k).to(tl.int32))
    tl.store(lab + i, labv, mask=typ <= 2)
    cv = tl.load(cj + k).to(tl.int32)
    tl.store(col + i, tl.where(isG, -1, tl.where(isM, cv, -2 - cv)), mask=typ <= 2)


@triton.jit
def _moves(am, ai, ad, age, tr, entw, colf, a_r, t_r, i, k, cms, g1, e0):
    """The scores of every move into the current state, from the stored alphas."""
    first = i == 0
    later = i > 0
    im1 = tl.maximum(i - 1, 0)
    km1 = tl.maximum(k - 1, 0)
    Gp = tl.where(first, 0.0, tl.load(age + im1 * 2))
    Ep = tl.where(first, -1.0e9, tl.load(age + im1 * 2 + 1))
    nf = ((tl.load(colf + k) & 1) == 0) & (k < cms)
    prev_ok = later & nf
    a_g1 = Gp + g1
    a_e0 = Ep + e0
    begin_from_G = a_g1 >= a_e0
    enter = tl.maximum(a_g1, a_e0) + tl.load(entw + k)
    Mprev = tl.where(prev_ok, tl.load(am + im1 * a_r + km1) + tl.load(tr + 0 * t_r + km1), -1.0e9)
    Iprev = tl.where(prev_ok, tl.load(ai + im1 * a_r + km1) + tl.load(tr + 5 * t_r + km1), -1.0e9)
    Dprev = tl.where(prev_ok, tl.load(ad + im1 * a_r + km1) + tl.load(tr + 7 * t_r + km1), -1.0e9)
    m_enter = enter >= tl.maximum(tl.maximum(Mprev, Iprev), Dprev)
    I_M = tl.where(later, tl.load(am + im1 * a_r + k) + tl.load(tr + 1 * t_r + k), -1.0e9)
    I_I = tl.where(later, tl.load(ai + im1 * a_r + k) + tl.load(tr + 4 * t_r + k), -1.0e9)
    D_M = tl.where(nf, tl.load(am + i * a_r + km1) + tl.load(tr + 2 * t_r + km1), -1.0e9)
    D_D = tl.where(nf, tl.load(ad + i * a_r + km1) + tl.load(tr + 6 * t_r + km1), -1.0e9)
    return Gp, Ep, begin_from_G, m_enter, Mprev, Iprev, Dprev, I_M, I_I, D_M, D_D


@triton.jit
def _next_state(
    typ, k, i, kend, g_to_g, begin_from_G, m_enter, Mprev, Iprev, Dprev, I_M, I_I, D_M, D_D
):
    """The state the walk moves to: (type, state, residue); type 4 ends it."""
    first = i == 0
    isG = typ == 0
    isM = typ == 1
    isI = typ == 2
    is_m = (Mprev >= Iprev) & (Mprev >= Dprev)
    is_i = (is_m == 0) & (Iprev >= Dprev)
    nG_t = tl.where(first, 4, tl.where(g_to_g, 0, 1))
    nG_k = tl.where(g_to_g, k, kend)
    nE_t = tl.where(first, 4, tl.where(begin_from_G, 0, 1))
    nE_k = tl.where(begin_from_G, k, kend)
    nC_t = tl.where(is_m, 1, tl.where(is_i, 2, 3))
    nI_t = tl.where(I_M >= I_I, 1, 2)
    nD_t = tl.where(D_M >= D_D, 1, 3)
    nt = tl.where(
        isG, nG_t, tl.where(isM, tl.where(m_enter, nE_t, nC_t), tl.where(isI, nI_t, nD_t))
    )
    nk = tl.where(isG, nG_k, tl.where(isM, tl.where(m_enter, nE_k, k - 1), tl.where(isI, k, k - 1)))
    ni = tl.where(isG | isM | isI, i - 1, i)
    bad = (nt != 4) & ((ni < 0) | (nk < 0))
    return tl.where(bad, 4, nt).to(tl.int32), tl.maximum(nk, 0), tl.maximum(ni, 0)


@triton.jit
def traceback_kernel(
    AM, AI, AD, AGE, TR, ENTW, COLF, CMS, THEME, COLJ, GE, LENS, LAB, STA, COL, a_b, a_r, g_b, t_b,
    t_r, w_b, c_b, h_b, l_b, K, MAXSTEP, TILE: tl.constexpr,
):
    """The traceback of one protein per program."""
    b = tl.program_id(0).to(tl.int64)
    n = tl.load(LENS + b).to(tl.int64)
    cms = tl.load(CMS + b).to(tl.int64)
    g0 = tl.load(GE + 0)
    g1 = tl.load(GE + 1)
    e0 = tl.load(GE + 2)
    e1 = tl.load(GE + 3)
    am, ai, ad = AM + b * a_b, AI + b * a_b, AD + b * a_b
    age, tr = AGE + b * g_b, TR + b * t_b
    i = n - 1
    Gl = tl.load(age + i * 2)
    El = tl.load(age + i * 2 + 1)
    typ = tl.where(Gl >= El, 0, 1).to(tl.int32)
    k = tl.zeros([], tl.int64)
    if El > Gl:
        k = _end_argmax(am + i * a_r, tr + 3 * t_r, cms, TILE)
    steps = tl.zeros([], tl.int64)
    maxsteps = n * MAXSTEP + 16
    while (typ != 4) & (steps < maxsteps):
        steps += 1
        _emit(LAB + b * l_b, COL + b * l_b, THEME + b * h_b, COLJ + b * h_b, i, k, typ, K)
        Gp, Ep, from_G, m_enter, Mprev, Iprev, Dprev, I_M, I_I, D_M, D_D = _moves(
            am, ai, ad, age, tr, ENTW + b * w_b, COLF + b * c_b, a_r, t_r, i, k, cms, g1, e0
        )
        g_to_g = (Gp + g0) >= (Ep + e1)
        need_end = (i > 0) & (((typ == 0) & (g_to_g == 0)) | ((typ == 1) & m_enter & (from_G == 0)))
        kend = tl.zeros([], tl.int64)
        if need_end:
            kend = _end_argmax(am + tl.maximum(i - 1, 0) * a_r, tr + 3 * t_r, cms, TILE)
        tl.store(STA + b * l_b + i, 1, mask=(typ == 1) & m_enter)
        typ, k, i = _next_state(
            typ, k, i, kend, g_to_g, from_G, m_enter, Mprev, Iprev, Dprev, I_M, I_I, D_M, D_D
        )


@torch.no_grad()
def traceback(
    alphas: tuple[torch.Tensor, ...], tables: dict[str, torch.Tensor], lens: torch.Tensor, K: int
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """``(labels int32, hit starts bool, columns int16)``, (B, ROWS) each, along the best path."""
    AM, AI, AD, AGE = alphas
    B, ROWS, _ = AM.shape
    TR, ENTW, COLF = tables["TR"], tables["ENTW"], tables["COLF"]
    LAB = torch.full((B, ROWS), K, dtype=torch.int32, device=AM.device)
    STA = torch.zeros((B, ROWS), dtype=torch.int8, device=AM.device)
    COL = torch.full((B, ROWS), -1, dtype=torch.int32, device=AM.device)
    TH = tables["theme"].contiguous()
    CJ = tables["colj"].contiguous()
    traceback_kernel[(B,)](
        AM,
        AI,
        AD,
        AGE,
        TR,
        ENTW,
        COLF,
        tables["CMS"],
        TH,
        CJ,
        tables["GE"],
        lens.to(torch.int32).contiguous(),
        LAB,
        STA,
        COL,
        AM.stride(0),
        AM.stride(1),
        AGE.stride(0),
        TR.stride(0),
        TR.stride(1),
        ENTW.stride(0),
        COLF.stride(0),
        TH.stride(0),
        LAB.stride(0),
        K,
        _MAX_STEP,
        TILE=1024,
        num_warps=4,
    )
    return LAB, STA.bool(), COL.to(torch.int16)
# fmt: on
