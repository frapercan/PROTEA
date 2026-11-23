"""The exact passes over each protein's candidate states, tiled, on Triton.

Ported from the laboratory's ``segmental_triton._seg_fwd_t`` and ``_seg_bwd_t``. Per
residue i, with op = logaddexp (the sum over paths) or max (the best path), and
states M, I, D per (theme, column):

    Begin = op(G + g1, End + e0);   G' = q[x_i] + op(G + g0, End + e1)
    M'[c, j] = E[c, j, x_i] + op(Begin + enter[c, j], op(M[c, j-1] + mm, I[c, j-1] + im, D[c, j-1] + dm))
    I'[c, j] = q[x_i] + op(M[c, j] + mi, I[c, j] + ii)
    D'[c, j] = op(D'[c, j-1] + dd[c, j-1], M'[c, j-1] + md[c, j-1])   (a scan along the columns)
    End' = op over (c, j) of M'[c, j] + me[c, j];     log P(x) = op(G, End) at the last residue

A protein's states are its candidates' active columns end to end, visited in
tiles of TILE states held in two per-protein buffers; the delete chain carries
across a tile's edge. The forward stores every residue's alphas when asked (the
traceback and the backward read them).

The backward here keeps only what the theme mass needs: the betas and the
expected count of every match state at every letter (``DE``, the laboratory's
gradient with respect to the emissions). It skips the transition, entry and
background counts the laboratory's training needs. The kernels are split into
jitted pieces that Triton inlines; the arithmetic and its order are the
laboratory's.
"""

from __future__ import annotations

import torch
import triton
import triton.language as tl

TILE_STATES = 2048
_NEG = tl.constexpr(-1.0e9)


# The kernels' argument lists stay compact: one name per line would push them past the
# platform's length limit without making them clearer.
# fmt: off
@triton.jit
def _op(a, b, MAXMODE: tl.constexpr):
    if MAXMODE:
        return tl.maximum(a, b)
    m = tl.maximum(a, b)
    return m + tl.log(1.0 + tl.exp(-tl.abs(a - b)))


@triton.jit
def _cmp_log(a1, c1, a2, c2):
    m = tl.maximum(a2, a1 + c2)
    return m + tl.log(1.0 + tl.exp(-tl.abs(a2 - (a1 + c2)))), c1 + c2


@triton.jit
def _cmp_max(a1, c1, a2, c2):
    return tl.maximum(a2, a1 + c2), c1 + c2


@triton.jit
def _cmp_log_rev(a1, c1, a2, c2):
    m = tl.maximum(a2, a1 + c2)
    return m + tl.log(1.0 + tl.exp(-tl.abs(a2 - (a1 + c2)))), c1 + c2


@triton.jit
def _fill(base, SMAX, cmb, copies, value, TILE: tl.constexpr):
    """``copies`` buffers of SMAX states from ``base``, every real state set to ``value``."""
    kk = tl.arange(0, TILE)
    for t in range(0, (cmb + TILE - 1) // TILE):
        k = t * TILE + kk
        ok = k < cmb
        for c in range(0, copies):
            tl.store(base + c * SMAX + k, tl.zeros([TILE], dtype=tl.float32) + value, mask=ok)


@triton.jit
def _match_insert(trb, t_r, prv, SMAX, k, ok, notfirst, em, entw, Begin, eq, MAXMODE: tl.constexpr):
    """The match and insert states of one tile at this residue, from the previous residue's."""
    mm_s = tl.load(trb + 0 * t_r + k - 1, mask=notfirst, other=_NEG)
    mi = tl.load(trb + 1 * t_r + k, mask=ok, other=_NEG)
    ii = tl.load(trb + 4 * t_r + k, mask=ok, other=_NEG)
    im_s = tl.load(trb + 5 * t_r + k - 1, mask=notfirst, other=_NEG)
    dm_s = tl.load(trb + 7 * t_r + k - 1, mask=notfirst, other=_NEG)
    Mv = tl.load(prv + k, mask=ok, other=_NEG)
    Iv = tl.load(prv + SMAX + k, mask=ok, other=_NEG)
    Mps = tl.load(prv + k - 1, mask=notfirst, other=_NEG)
    Ips = tl.load(prv + SMAX + k - 1, mask=notfirst, other=_NEG)
    Dps = tl.load(prv + 2 * SMAX + k - 1, mask=notfirst, other=_NEG)
    fp = _op(_op(Mps + mm_s, Ips + im_s, MAXMODE), Dps + dm_s, MAXMODE)
    Mn = em + _op(Begin + entw, fp, MAXMODE)
    Mn = tl.where(ok, Mn, _NEG)
    In = eq + _op(Mv + mi, Iv + ii, MAXMODE)
    In = tl.where(ok, In, _NEG)
    return Mn, In


@triton.jit
def _delete_chain(trb, t_r, nxt, SMAX, k, kk, ok, notfirst, MAXMODE: tl.constexpr):
    """The delete states of one tile, a scan along the columns carried across the tile's edge."""
    md_s = tl.load(trb + 2 * t_r + k - 1, mask=notfirst, other=_NEG)
    dd_s = tl.load(trb + 6 * t_r + k - 1, mask=notfirst, other=_NEG)
    Mn_s = tl.load(nxt + k - 1, mask=notfirst, other=_NEG)
    base = tl.where(notfirst, Mn_s + md_s, _NEG)
    cc = tl.where(notfirst, dd_s, _NEG)
    edge = (kk == 0) & notfirst
    Dcar = tl.load(nxt + 2 * SMAX + k - 1, mask=edge, other=_NEG)
    base = tl.where(edge, _op(base, Dcar + dd_s, MAXMODE), base)
    if MAXMODE:
        Dn, _ = tl.associative_scan((base, cc), 0, _cmp_max)
    else:
        Dn, _ = tl.associative_scan((base, cc), 0, _cmp_log)
    return tl.where(ok, Dn, _NEG)


@triton.jit
def _end_step(emx, esum, Mn, me, ok, MAXMODE: tl.constexpr):
    """The running End over the tiles: a maximum, or a rescaled sum of exponentials."""
    ae = tl.where(ok, Mn + me, _NEG)
    if MAXMODE:
        emx = tl.maximum(emx, ae)
    else:
        nmx = tl.maximum(emx, ae)
        esum = esum * tl.exp(emx - nmx) + tl.exp(ae - nmx)
        emx = nmx
    return emx, esum


@triton.jit
def _fwd_tile(
    X_i, E, e_b, e_r, trb, t_r, ENTW, w_b, COLF, c_b, b, prv, nxt, SMAX, t, kk, cmb, Begin, eq,
    emx, esum, AM, AI, AD, ROWS, i, TILE: tl.constexpr, MAXMODE: tl.constexpr, STORE: tl.constexpr,
    STORE_ID: tl.constexpr,
):
    """One tile of one residue of the forward: its M, I and D, the End, and the stored alphas."""
    k = t * TILE + kk
    ok = k < cmb
    fl = tl.load(COLF + b * c_b + k, mask=ok, other=3)
    notfirst = ok & ((fl & 1) == 0)
    me = tl.load(trb + 3 * t_r + k, mask=ok, other=_NEG)
    entw = tl.load(ENTW + b * w_b + k, mask=ok, other=_NEG)
    em = tl.load(E + b * e_b + X_i * e_r + k, mask=ok, other=_NEG)
    Mn, In = _match_insert(trb, t_r, prv, SMAX, k, ok, notfirst, em, entw, Begin, eq, MAXMODE)
    tl.store(nxt + k, Mn, mask=ok)
    tl.debug_barrier()
    Dn = _delete_chain(trb, t_r, nxt, SMAX, k, kk, ok, notfirst, MAXMODE)
    tl.store(nxt + SMAX + k, In, mask=ok)
    tl.store(nxt + 2 * SMAX + k, Dn, mask=ok)
    emx, esum = _end_step(emx, esum, Mn, me, ok, MAXMODE)
    if STORE:
        ra = (b * ROWS + i) * SMAX
        tl.store(AM + ra + k, Mn, mask=ok)
        if STORE_ID:
            tl.store(AI + ra + k, In, mask=ok)
            tl.store(AD + ra + k, Dn, mask=ok)
    tl.debug_barrier()
    return emx, esum


@triton.jit
def _end_of(emx, esum, MAXMODE: tl.constexpr):
    if MAXMODE:
        return tl.max(emx, 0)
    gmx = tl.max(emx, 0)
    return gmx + tl.log(tl.sum(esum * tl.exp(emx - gmx), 0))


@triton.jit
def seg_fwd_t(
    X, LENS, E, TR, ENTW, COLF, CMS, GE, LQ, ST, Z, AM, AI, AD, AGE, x_b, e_b, e_r, t_b, t_r, w_b,
    c_b, s_b, SMAX, ROWS, TILE: tl.constexpr, MAXMODE: tl.constexpr, STORE: tl.constexpr,
    STORE_ID: tl.constexpr,
):
    """The forward of one protein per program: log P(x), or the best path's score.

    STORE keeps every residue's match alphas and its background and End; STORE_ID
    the insert and delete alphas too (the traceback reads them, the backward not).
    """
    b = tl.program_id(0).to(tl.int64)
    n = tl.load(LENS + b)
    cmb = tl.load(CMS + b)
    nt = (cmb + TILE - 1) // TILE
    kk = tl.arange(0, TILE)
    g0 = tl.load(GE + 0)
    g1 = tl.load(GE + 1)
    e0 = tl.load(GE + 2)
    e1 = tl.load(GE + 3)
    trb = TR + b * t_b
    stb = ST + b * s_b
    _fill(stb, SMAX, cmb, 6, -1.0e9, TILE)
    tl.debug_barrier()
    G = tl.full([], 0.0, tl.float32)
    End = tl.full([], -1.0e9, tl.float32)
    for i in range(0, n):
        prv = stb + (i % 2) * 3 * SMAX
        nxt = stb + ((i + 1) % 2) * 3 * SMAX
        xi = tl.load(X + b * x_b + i)
        eq = tl.load(LQ + xi)
        Begin = _op(G + g1, End + e0, MAXMODE)
        Gn = eq + _op(G + g0, End + e1, MAXMODE)
        emx = tl.zeros([TILE], dtype=tl.float32) + _NEG
        esum = tl.zeros([TILE], dtype=tl.float32)
        for t in range(0, nt):
            emx, esum = _fwd_tile(
                xi, E, e_b, e_r, trb, t_r, ENTW, w_b, COLF, c_b, b, prv, nxt, SMAX, t, kk, cmb,
                Begin, eq, emx, esum, AM, AI, AD, ROWS, i, TILE, MAXMODE, STORE, STORE_ID,
            )
        End = _end_of(emx, esum, MAXMODE)
        G = Gn
        if STORE:
            tl.store(AGE + (b * ROWS + i) * 2, G)
            tl.store(AGE + (b * ROWS + i) * 2 + 1, End)
    tl.store(Z + b, _op(G, End, MAXMODE))


@triton.jit
def _begin_of_next(E, e_b, e_r, ENTW, w_b, b, prv, Qp, x1, last, cmb, nt, kk, TILE: tl.constexpr):
    """Pass 1 of a residue: Q = E(i+1) + bM(i+1) for every state, and the Begin sum of residue i+1."""
    bmx = tl.zeros([TILE], dtype=tl.float32) + _NEG
    bsum = tl.zeros([TILE], dtype=tl.float32)
    for t in range(0, nt):
        k = t * TILE + kk
        ok = k < cmb
        em1 = tl.load(E + b * e_b + x1 * e_r + k, mask=ok, other=_NEG)
        bM1 = tl.load(prv + k, mask=ok, other=_NEG)
        Q = tl.where(last, _NEG, em1 + bM1)
        Q = tl.where(ok, Q, _NEG)
        tl.store(Qp + k, Q, mask=ok)
        entw = tl.load(ENTW + b * w_b + k, mask=ok, other=_NEG)
        v = tl.where(ok, entw + Q, _NEG)
        nmx = tl.maximum(bmx, v)
        bsum = bsum * tl.exp(bmx - nmx) + tl.exp(v - nmx)
        bmx = nmx
    tl.debug_barrier()
    gmx = tl.max(bmx, 0)
    return tl.where(last, -1.0e9, gmx + tl.log(tl.sum(bsum * tl.exp(bmx - gmx), 0)))


@triton.jit
def _bwd_delete(trb, t_r, Qp, bDp, k, kk, ok, notlast, TILE: tl.constexpr):
    """The delete betas of one tile, a reversed scan whose suffix carries back from the next tile."""
    dd = tl.load(trb + 6 * t_r + k, mask=ok, other=_NEG)
    dm = tl.load(trb + 7 * t_r + k, mask=ok, other=_NEG)
    Qs = tl.load(Qp + k + 1, mask=notlast, other=_NEG)
    base = tl.where(ok, dm + Qs, _NEG)
    cc = tl.where(notlast, dd, _NEG)
    edge = (kk == TILE - 1) & notlast
    bDcar = tl.load(bDp + k + 1, mask=edge, other=_NEG)
    base = tl.where(edge, _op(base, dd + bDcar, False), base)
    bD, _ = tl.associative_scan((base, cc), 0, _cmp_log_rev, reverse=True)
    bD = tl.where(ok, bD, _NEG)
    tl.store(bDp + k, bD, mask=ok)
    tl.debug_barrier()
    return tl.load(bDp + k + 1, mask=notlast, other=_NEG), Qs


@triton.jit
def _bwd_tile(
    TR_b, t_r, COLF_b, Qp, bDp, prv, nxt, SMAX, AM_r, DE_x, t, kk, cmb, bE, eq1, last, zz,
    TILE: tl.constexpr,
):
    """Pass 2 of one tile: the match and insert betas of residue i and its match states' expected counts."""
    k = t * TILE + kk
    ok = k < cmb
    fl = tl.load(COLF_b + k, mask=ok, other=3)
    notlast = ok & ((fl & 2) == 0)
    bDs, Qs = _bwd_delete(TR_b, t_r, Qp, bDp, k, kk, ok, notlast, TILE)
    mm = tl.load(TR_b + 0 * t_r + k, mask=ok, other=_NEG)
    mi = tl.load(TR_b + 1 * t_r + k, mask=ok, other=_NEG)
    md = tl.load(TR_b + 2 * t_r + k, mask=ok, other=_NEG)
    me = tl.load(TR_b + 3 * t_r + k, mask=ok, other=_NEG)
    ii = tl.load(TR_b + 4 * t_r + k, mask=ok, other=_NEG)
    im = tl.load(TR_b + 5 * t_r + k, mask=ok, other=_NEG)
    bI1 = tl.load(prv + SMAX + k, mask=ok, other=_NEG)
    bI1x = tl.where(last, _NEG, eq1 + bI1)
    bI = tl.where(ok, _op(im + Qs, ii + bI1x, False), _NEG)
    bM = _op(_op(me + bE, md + bDs, False), _op(mm + Qs, mi + bI1x, False), False)
    bM = tl.where(ok, bM, _NEG)
    aM = tl.load(AM_r + k, mask=ok, other=_NEG)
    gM = tl.where(ok, tl.exp(tl.minimum(aM + bM - zz, 0.0)), 0.0)
    tl.store(DE_x + k, tl.load(DE_x + k, mask=ok, other=0.0) + gM, mask=ok)
    tl.store(nxt + k, bM, mask=ok)
    tl.store(nxt + SMAX + k, bI, mask=ok)
    tl.debug_barrier()


@triton.jit
def seg_bwd_match_t(
    X, LENS, E, TR, ENTW, COLF, CMS, GE, LQ, Z, AM, BT, DE, x_b, e_b, e_r, t_b, t_r, w_b, c_b, s_b,
    de_b, SMAX, ROWS, TILE: tl.constexpr,
):
    """The betas from the last residue down to the first, and every match state's expected count per letter."""
    b = tl.program_id(0).to(tl.int64)
    n = tl.load(LENS + b)
    cmb = tl.load(CMS + b)
    nt = (cmb + TILE - 1) // TILE
    kk = tl.arange(0, TILE)
    g0 = tl.load(GE + 0)
    g1 = tl.load(GE + 1)
    e0 = tl.load(GE + 2)
    e1 = tl.load(GE + 3)
    zz = tl.load(Z + b)
    btb = BT + b * s_b
    _fill(btb, SMAX, cmb, 2, -1.0e9, TILE)
    tl.debug_barrier()
    bG1 = tl.full([], 0.0, tl.float32)
    for r in range(0, n):
        i = n - 1 - r
        last = i == n - 1
        prv = btb + (r % 2) * 2 * SMAX
        nxt = btb + ((r + 1) % 2) * 2 * SMAX
        xi = tl.load(X + b * x_b + i)
        x1 = tl.load(X + b * x_b + i + 1, mask=i + 1 < n, other=0)
        eq1 = tl.load(LQ + x1)
        bBeg1 = _begin_of_next(
            E, e_b, e_r, ENTW, w_b, b, prv, btb + 4 * SMAX, x1, last, cmb, nt, kk, TILE
        )
        bG1x = tl.where(last, -1.0e9, eq1 + bG1)
        bG = tl.where(last, 0.0, _op(g0 + bG1x, g1 + bBeg1, False))
        bE = tl.where(last, 0.0, _op(e1 + bG1x, e0 + bBeg1, False))
        ra = (b * ROWS + i) * SMAX
        for tt in range(0, nt):
            _bwd_tile(
                TR + b * t_b, t_r, COLF + b * c_b, btb + 4 * SMAX, btb + 5 * SMAX, prv, nxt, SMAX,
                AM + ra, DE + b * de_b + xi * e_r, nt - 1 - tt, kk, cmb, bE, eq1, last, zz, TILE,
            )
        bG1 = bG


#: The forward's results: Z, the match, insert and delete alphas, the background and End, ROWS.
Forward = tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor, int]


def launch_forward(
    tables: dict[str, torch.Tensor], X: torch.Tensor, lens: torch.Tensor, maxmode: bool
) -> Forward:
    """``seg_fwd_t`` with every residue's alphas stored: Z, AM, AI, AD, AGE and ROWS.

    The insert and delete alphas are kept for the best path only (its traceback
    reads them); in the sum semiring they are empty.
    """
    E, TR, ENTW, COLF, CMS = (tables[k] for k in ("E", "TR", "ENTW", "COLF", "CMS"))
    B, S = ENTW.shape
    dev = X.device
    ROWS = max(16, -(-int(lens.max()) // 16) * 16)
    ST = torch.empty(B, 6 * S, device=dev, dtype=torch.float32)
    Z = torch.empty(B, device=dev, dtype=torch.float32)
    AM = torch.empty(B, ROWS, S, device=dev, dtype=torch.float32)
    if maxmode:
        AI, AD = torch.empty_like(AM), torch.empty_like(AM)
    else:
        AI = AD = torch.empty(1, device=dev, dtype=torch.float32)
    AGE = torch.empty(B, ROWS, 2, device=dev, dtype=torch.float32)
    seg_fwd_t[(B,)](
        X,
        lens,
        E,
        TR,
        ENTW,
        COLF,
        CMS,
        tables["GE"],
        tables["LQ"],
        ST,
        Z,
        AM,
        AI,
        AD,
        AGE,
        X.stride(0),
        E.stride(0),
        E.stride(1),
        TR.stride(0),
        TR.stride(1),
        ENTW.stride(0),
        COLF.stride(0),
        ST.stride(0),
        S,
        ROWS,
        TILE=TILE_STATES,
        MAXMODE=maxmode,
        STORE=True,
        STORE_ID=maxmode,
        num_warps=8,
    )
    return Z, AM, AI, AD, AGE, ROWS


def match_counts(
    tables: dict[str, torch.Tensor], X: torch.Tensor, lens: torch.Tensor
) -> torch.Tensor:
    """(B, 20, S) every match state's expected count at every letter: the laboratory's dZ/dE."""
    Z, AM, _, _, _, ROWS = launch_forward(tables, X, lens, maxmode=False)
    E, TR, ENTW, COLF, CMS = (tables[k] for k in ("E", "TR", "ENTW", "COLF", "CMS"))
    B, S = ENTW.shape
    BT = torch.empty(B, 6 * S, device=X.device, dtype=torch.float32)
    DE = torch.zeros(B, 20, S, device=X.device, dtype=torch.float32)
    seg_bwd_match_t[(B,)](
        X,
        lens,
        E,
        TR,
        ENTW,
        COLF,
        CMS,
        tables["GE"],
        tables["LQ"],
        Z,
        AM,
        BT,
        DE,
        X.stride(0),
        E.stride(0),
        E.stride(1),
        TR.stride(0),
        TR.stride(1),
        ENTW.stride(0),
        COLF.stride(0),
        BT.stride(0),
        DE.stride(0),
        S,
        ROWS,
        TILE=TILE_STATES,
        num_warps=8,
    )
    return DE
# fmt: on
