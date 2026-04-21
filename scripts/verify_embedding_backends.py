"""End-to-end verification of all embedding backends with real weights.

Loads each backend + model on GPU, runs a forward pass on a small fixed set
of protein sequences, and checks that the resulting embeddings are:

  * the right shape and dtype (float32 after pooling)
  * L2-normalised when ``normalize=True`` (norm ~ 1.0 within FP16 tolerance)
  * not collapsed (high fraction of non-zero dimensions)
  * deterministic (same sequence → same vector across calls)
  * distinguishable (different sequences → cosine similarity < 0.999)
  * batch-stable (a sequence embedded alone matches its slot in a batch)

For the Ankh backend the current space-joined tokenisation is also compared
against the documented ``is_split_into_words=True`` path, to confirm we are
not silently producing degenerate vectors through ``<unk>`` substitution.

Run with:
    poetry run python scripts/verify_embedding_backends.py [--only BACKEND]

This script bypasses the job queue and calls ``_load_model`` / ``_embed_*``
directly.  It is safe to run while the stack is up as long as no
``compute_embeddings`` job is currently consuming the GPU.
"""
from __future__ import annotations

import argparse
import gc
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable
from unittest.mock import MagicMock

import numpy as np

# --- Fixed test sequences --------------------------------------------------
# Two real-ish short sequences (~30 AA each) + one ambiguous-residue sequence
# to exercise the U/Z/O/B → X substitution path.
SEQ_SHORT = "MKTAYIAKQRQISFVKSHFSRQLEERLGLIEVQ"
SEQ_OTHER = "MSEQNNTEMTFQIQRIYTKDISFEAPNAPHVFQ"
SEQ_AMBIG = "MKTAYIAKQUZOBFVKSHFSRQLEERLGLIEVQ"


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""

    def __str__(self) -> str:
        icon = "OK " if self.ok else "FAIL"
        return f"  [{icon}] {self.name}{(' — ' + self.detail) if self.detail else ''}"


@dataclass
class BackendResult:
    backend: str
    model_name: str
    loaded_ms: float
    forward_ms: float
    dim: int
    checks: list[Check]

    @property
    def all_ok(self) -> bool:
        return all(c.ok for c in self.checks)


def _make_config(
    backend: str,
    model_name: str,
    layer_indices: list[int] | None = None,
    layer_agg: str = "mean",
    pooling: str = "mean",
    normalize: bool = True,
    normalize_residues: bool = False,
    max_length: int = 512,
    use_chunking: bool = False,
    chunk_size: int = 256,
    chunk_overlap: int = 0,
) -> Any:
    cfg = MagicMock()
    cfg.model_backend = backend
    cfg.model_name = model_name
    cfg.layer_indices = layer_indices or [0]
    cfg.layer_agg = layer_agg
    cfg.pooling = pooling
    cfg.normalize = normalize
    cfg.normalize_residues = normalize_residues
    cfg.max_length = max_length
    cfg.use_chunking = use_chunking
    cfg.chunk_size = chunk_size
    cfg.chunk_overlap = chunk_overlap
    return cfg


def _silent_emit(*_args: Any, **_kwargs: Any) -> None:
    pass


def _free_gpu() -> None:
    import torch

    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
        torch.cuda.synchronize()


def _cosine(u: np.ndarray, v: np.ndarray) -> float:
    nu = float(np.linalg.norm(u))
    nv = float(np.linalg.norm(v))
    if nu == 0 or nv == 0:
        return 0.0
    return float(np.dot(u, v) / (nu * nv))


def _run_backend(
    backend: str,
    model_name: str,
    embed_fn: Callable[..., Any],
    *,
    device: str = "cuda",
    layer_indices: list[int] | None = None,
) -> BackendResult:
    """Load the model, run the three fixed sequences, and sanity-check the output."""
    from protea.core.operations.compute_embeddings import _load_model

    cfg = _make_config(backend, model_name, layer_indices=layer_indices)

    print(f"\n== {backend:<6} {model_name}")
    t0 = time.perf_counter()
    model, tokenizer = _load_model(cfg, device, _silent_emit)
    t_load = (time.perf_counter() - t0) * 1000.0
    print(f"   loaded in {t_load:.0f} ms")

    checks: list[Check] = []

    # --- Batch of 2 forward pass ------------------------------------------
    t0 = time.perf_counter()
    batch_out = embed_fn(model, tokenizer, [SEQ_SHORT, SEQ_OTHER], cfg, device)
    t_forward = (time.perf_counter() - t0) * 1000.0
    print(f"   forward (2 seqs) in {t_forward:.0f} ms")

    checks.append(Check("batch returns 2 entries", len(batch_out) == 2,
                        f"got {len(batch_out)}"))
    vec_short = batch_out[0][0].vector
    vec_other = batch_out[1][0].vector
    dim = vec_short.shape[0]

    checks.append(Check("dtype is float32", vec_short.dtype == np.float32,
                        f"dtype={vec_short.dtype}"))
    checks.append(Check("shape is (D,)", vec_short.ndim == 1,
                        f"ndim={vec_short.ndim}"))
    checks.append(Check("dim > 0", dim > 0, f"dim={dim}"))

    # L2 norm ~ 1 (normalize=True, but FP16 forward → some drift expected)
    norm_short = float(np.linalg.norm(vec_short))
    norm_other = float(np.linalg.norm(vec_other))
    norm_ok = abs(norm_short - 1.0) < 1e-3 and abs(norm_other - 1.0) < 1e-3
    checks.append(Check(
        "L2 norm ~ 1",
        norm_ok,
        f"short={norm_short:.5f} other={norm_other:.5f}",
    ))

    # Non-zero fraction (detect degenerate <unk> collapse)
    nz_short = float((np.abs(vec_short) > 1e-6).sum() / dim)
    nz_other = float((np.abs(vec_other) > 1e-6).sum() / dim)
    checks.append(Check(
        "non-zero fraction > 90%",
        nz_short > 0.9 and nz_other > 0.9,
        f"short={nz_short:.2%} other={nz_other:.2%}",
    ))

    # Distinguishable
    cos_distinct = _cosine(vec_short, vec_other)
    checks.append(Check(
        "different seqs produce different vectors",
        cos_distinct < 0.999,
        f"cos(short, other)={cos_distinct:.4f}",
    ))

    # Determinism: re-embed the first sequence alone and compare
    single_out = embed_fn(model, tokenizer, [SEQ_SHORT], cfg, device)
    vec_short_again = single_out[0][0].vector
    cos_determ = _cosine(vec_short, vec_short_again)
    # batch==1 vs batch==2 for the same seq: allow small FP16 drift
    checks.append(Check(
        "batch-stable (same seq alone ≈ in batch)",
        cos_determ > 0.999,
        f"cos={cos_determ:.6f}",
    ))

    # Re-embed twice alone: must be bit-identical
    single_out_2 = embed_fn(model, tokenizer, [SEQ_SHORT], cfg, device)
    cos_twice = _cosine(single_out[0][0].vector, single_out_2[0][0].vector)
    checks.append(Check(
        "deterministic (same call twice)",
        cos_twice > 0.99999,
        f"cos={cos_twice:.6f}",
    ))

    # Ambiguous residues (UZOB) must not crash and must stay normalised
    try:
        ambig_out = embed_fn(model, tokenizer, [SEQ_AMBIG], cfg, device)
        vec_ambig = ambig_out[0][0].vector
        ambig_ok = abs(float(np.linalg.norm(vec_ambig)) - 1.0) < 1e-3
        checks.append(Check(
            "handles ambiguous residues (UZOB)",
            ambig_ok,
            f"norm={float(np.linalg.norm(vec_ambig)):.5f}",
        ))
    except Exception as e:  # noqa: BLE001
        checks.append(Check("handles ambiguous residues (UZOB)", False, repr(e)))

    # Free weights before the next backend
    del model, tokenizer
    _free_gpu()

    for c in checks:
        print(c)

    return BackendResult(backend, model_name, t_load, t_forward, dim, checks)


def _run_backend_chunking(
    backend: str,
    model_name: str,
    embed_fn: Callable[..., Any],
    *,
    device: str = "cuda",
    layer_indices: list[int] | None = None,
) -> BackendResult:
    """Verify chunking over a long sequence.

    Builds a ~704 AA sequence, runs the backend with ``use_chunking=True``,
    ``chunk_size=300``, ``chunk_overlap=50``, and validates:

      * correct number of chunks and chunk start indices (0, 250, 500)
      * final chunk ends at the residue length (704 for ESM which strips
        CLS/EOS, 705 for T5/Ankh which keeps EOS)
      * every chunk is finite, L2-normalised, and has a high non-zero fraction
      * distant chunks (first vs last) are distinguishable — pooling cannot be
        collapsing every chunk to the same degenerate vector
    """
    from protea.core.operations.compute_embeddings import _load_model

    chunk_size = 300
    chunk_overlap = 50
    seq_len = 704
    # Alternate two distinct short motifs so that the first and last chunks
    # see genuinely different content (necessary for the distinguishability
    # check; a repeated single motif would make all chunks near-identical).
    long_seq = ((SEQ_SHORT + SEQ_OTHER) * 12)[:seq_len]

    cfg = _make_config(
        backend,
        model_name,
        layer_indices=layer_indices,
        max_length=1024,
        use_chunking=True,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
    )

    print(f"\n== [chunk] {backend:<6} {model_name}")
    print(f"   seq_len={seq_len}, chunk_size={chunk_size}, chunk_overlap={chunk_overlap}")

    t0 = time.perf_counter()
    model, tokenizer = _load_model(cfg, device, _silent_emit)
    t_load = (time.perf_counter() - t0) * 1000.0
    print(f"   loaded in {t_load:.0f} ms")

    checks: list[Check] = []

    t0 = time.perf_counter()
    out = embed_fn(model, tokenizer, [long_seq], cfg, device)
    t_forward = (time.perf_counter() - t0) * 1000.0
    print(f"   forward (1 long seq) in {t_forward:.0f} ms")

    chunks = out[0]
    dim = chunks[0].vector.shape[0] if chunks else 0
    print(f"   produced {len(chunks)} chunks, dim={dim}")
    for i, ck in enumerate(chunks):
        print(f"     chunk[{i}] span=[{ck.chunk_index_s}, {ck.chunk_index_e})")

    # 1. Exactly 3 chunks: (0,300), (250,550), (500,~seq_len)
    checks.append(Check(
        "chunk count == 3",
        len(chunks) == 3,
        f"got {len(chunks)}",
    ))

    # 2. chunk_index_s values match expected starts
    expected_starts = [0, 250, 500]
    got_starts = [ck.chunk_index_s for ck in chunks]
    checks.append(Check(
        "chunk starts == [0, 250, 500]",
        got_starts == expected_starts,
        f"got {got_starts}",
    ))

    # 3. Final chunk must end at the amino-acid count.  Every backend now
    # strips all special tokens (CLS/BOS/EOS and ProstT5's <AA2fold>) so the
    # residue tensor length equals the amino-acid count — meaning
    # chunk_index_s/e are amino-acid positions on every backend.
    last_end = chunks[-1].chunk_index_e if chunks else None
    checks.append(Check(
        "final chunk ends at AA count (704)",
        last_end == seq_len,
        f"chunk_index_e={last_end}",
    ))

    # 4. Every chunk has a valid vector (finite, normalised, non-degenerate)
    all_finite = True
    all_norm_ok = True
    all_nz_ok = True
    for ck in chunks:
        v = ck.vector
        if not np.all(np.isfinite(v)):
            all_finite = False
        if abs(float(np.linalg.norm(v)) - 1.0) > 1e-3:
            all_norm_ok = False
        nz = float((np.abs(v) > 1e-6).sum() / max(v.shape[0], 1))
        if nz <= 0.9:
            all_nz_ok = False

    checks.append(Check("all chunks finite (no NaN/Inf)", all_finite))
    checks.append(Check("all chunks L2 norm ~ 1", all_norm_ok))
    checks.append(Check("all chunks non-zero fraction > 90%", all_nz_ok))

    # 5. First and last chunks must differ — position-sensitive content
    if len(chunks) >= 2:
        cos_first_last = _cosine(chunks[0].vector, chunks[-1].vector)
        checks.append(Check(
            "first vs last chunk distinguishable",
            cos_first_last < 0.9999,
            f"cos(first, last)={cos_first_last:.4f}",
        ))

    # 6. Deterministic across repeated calls
    out2 = embed_fn(model, tokenizer, [long_seq], cfg, device)
    chunks2 = out2[0]
    determ_ok = (
        len(chunks2) == len(chunks)
        and all(
            _cosine(a.vector, b.vector) > 0.99999
            for a, b in zip(chunks, chunks2, strict=False)
        )
    )
    checks.append(Check("chunking is deterministic", determ_ok))

    del model, tokenizer
    _free_gpu()

    for c in checks:
        print(c)

    return BackendResult(backend, model_name, t_load, t_forward, dim, checks)


def _embed_dispatch(backend: str):
    """Return the right _embed_* function for this backend."""
    from protea.core.operations import compute_embeddings as ce

    if backend == "esm":
        return ce._embed_esm
    if backend == "esm3c":
        def _adapter(model, tokenizer, sequences, cfg, device):  # noqa: ARG001
            return ce._embed_esm3c(model, sequences, cfg, device)
        return _adapter
    if backend == "t5":
        return ce._embed_t5
    if backend == "ankh":
        return ce._embed_ankh
    raise ValueError(f"unknown backend: {backend}")


def run_ankh_tokenizer_comparison() -> Check:
    """Compare current space-joined path vs is_split_into_words path for Ankh.

    The current PROTEA Ankh code reuses _embed_t5, which space-joins amino
    acids ("A C D E F"). The Ankh paper and README instead recommend passing
    a list of chars with is_split_into_words=True. This check loads ankh-base
    once, runs both tokenisation strategies, and reports the cosine between
    the resulting embeddings. If they disagree severely (cos < 0.99), the
    current path is likely producing degenerate vectors via <unk>.
    """
    import re

    import torch
    import torch.nn.functional as F
    from transformers import AutoTokenizer, T5EncoderModel

    print("\n== Ankh tokenizer comparison (space-joined vs is_split_into_words)")
    device = "cuda"
    # Ankh overflows in FP16 (T5 LayerNorm) — use bfloat16 to match the
    # production loader.
    dtype = torch.bfloat16

    tok = AutoTokenizer.from_pretrained("ElnaggarLab/ankh-base")
    model = T5EncoderModel.from_pretrained(
        "ElnaggarLab/ankh-base",
        output_hidden_states=True,
        torch_dtype=dtype,
    )
    model.eval().to(device)

    clean = re.sub(r"[UZOB]", "X", SEQ_SHORT)

    # Path A — current PROTEA behaviour (string, characters separated by spaces)
    inputs_a = tok.batch_encode_plus(
        [" ".join(clean)],
        padding="longest",
        return_tensors="pt",
        add_special_tokens=True,
    )
    inputs_a = {k: v.to(device) for k, v in inputs_a.items()}

    # Path B — Ankh README (list of chars, is_split_into_words=True)
    inputs_b = tok.batch_encode_plus(
        [list(clean)],
        padding="longest",
        is_split_into_words=True,
        return_tensors="pt",
        add_special_tokens=True,
    )
    inputs_b = {k: v.to(device) for k, v in inputs_b.items()}

    # Count <unk> tokens (tok.unk_token_id) in each path
    unk_id = tok.unk_token_id
    unk_a = int((inputs_a["input_ids"] == unk_id).sum().item()) if unk_id is not None else -1
    unk_b = int((inputs_b["input_ids"] == unk_id).sum().item()) if unk_id is not None else -1
    len_a = int(inputs_a["attention_mask"].sum().item())
    len_b = int(inputs_b["attention_mask"].sum().item())
    print(f"   path A (space-joined)     : tokens={len_a} unk={unk_a}")
    print(f"   path B (is_split_into_w)  : tokens={len_b} unk={unk_b}")

    with torch.no_grad():
        out_a = model(**inputs_a, output_hidden_states=True)
        out_b = model(**inputs_b, output_hidden_states=True)

    # Mean-pool over valid positions (include EOS — T5 convention)
    def _pool(out: Any, mask: Any) -> np.ndarray:
        h = out.hidden_states[-1][0]  # (L, D)
        actual = int(mask[0].sum().item())
        residues = h[:actual, :].float()
        pooled = residues.mean(dim=0)
        pooled = F.normalize(pooled.unsqueeze(0), p=2, dim=1).squeeze(0)
        return pooled.cpu().numpy()

    vec_a = _pool(out_a, inputs_a["attention_mask"])
    vec_b = _pool(out_b, inputs_b["attention_mask"])
    cos = _cosine(vec_a, vec_b)
    print(f"   cos(vec_A, vec_B) = {cos:.6f}")
    print(f"   tokens_A first 10: {inputs_a['input_ids'][0][:10].tolist()}")
    print(f"   tokens_B first 10: {inputs_b['input_ids'][0][:10].tolist()}")

    del model, tok
    _free_gpu()

    # The production path (B, is_split_into_words=True) must emit 0 <unk>.
    # Path A (space-joined, used for ProstT5) is expected to diverge — this
    # comparison exists precisely to document that divergence and justify
    # the backend-specific tokenisation.
    ok = unk_b == 0
    detail = (
        f"production_path_unk={unk_b} "
        f"(legacy space-joined: unk={unk_a}, cos_vs_production={cos:.4f})"
    )
    return Check("Ankh production tokenisation (is_split_into_words) emits 0 <unk>", ok, detail)


BACKENDS_TO_TEST: list[tuple[str, str, list[int] | None]] = [
    # (backend, model_name, layer_indices)
    ("esm",   "facebook/esm2_t6_8M_UR50D",            [0]),
    ("esm",   "facebook/esm2_t33_650M_UR50D",         [0]),
    ("esm3c", "esmc_300m",                            [0]),
    ("t5",    "Rostlab/prot_t5_xl_half_uniref50-enc", [0]),
    ("t5",    "Rostlab/ProstT5",                      [0]),
    ("ankh",  "ElnaggarLab/ankh-base",                [0]),
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", help="Restrict to one backend tag (esm, esm3c, t5, ankh)")
    parser.add_argument("--skip-tokenizer-compare", action="store_true",
                        help="Skip the Ankh tokenizer comparison (saves one model load)")
    parser.add_argument("--chunking-only", action="store_true",
                        help="Skip the short-sequence sweep and only verify the "
                             "chunking path on each backend")
    parser.add_argument("--skip-chunking", action="store_true",
                        help="Skip the chunking verification pass entirely")
    args = parser.parse_args()

    targets = BACKENDS_TO_TEST
    if args.only:
        targets = [t for t in targets if t[0] == args.only]
        if not targets:
            print(f"No backends match --only {args.only}")
            return 2

    results: list[BackendResult] = []
    chunking_results: list[BackendResult] = []
    tokenizer_check: Check | None = None

    if not args.chunking_only:
        for backend, model_name, layer_indices in targets:
            try:
                res = _run_backend(
                    backend,
                    model_name,
                    _embed_dispatch(backend),
                    layer_indices=layer_indices,
                )
                results.append(res)
            except Exception as e:  # noqa: BLE001
                print(f"   FAIL: {type(e).__name__}: {e}")
                results.append(BackendResult(
                    backend, model_name, 0.0, 0.0, 0,
                    [Check("model loaded + forward", False, f"{type(e).__name__}: {e}")],
                ))
                _free_gpu()

        if (not args.skip_tokenizer_compare
                and (not args.only or args.only == "ankh")):
            try:
                tokenizer_check = run_ankh_tokenizer_comparison()
                print(tokenizer_check)
            except Exception as e:  # noqa: BLE001
                tokenizer_check = Check("Ankh tokenizer comparison", False, repr(e))
                print(tokenizer_check)

    if not args.skip_chunking:
        for backend, model_name, layer_indices in targets:
            try:
                res = _run_backend_chunking(
                    backend,
                    model_name,
                    _embed_dispatch(backend),
                    layer_indices=layer_indices,
                )
                chunking_results.append(res)
            except Exception as e:  # noqa: BLE001
                print(f"   FAIL: {type(e).__name__}: {e}")
                chunking_results.append(BackendResult(
                    backend, model_name, 0.0, 0.0, 0,
                    [Check("chunking forward", False, f"{type(e).__name__}: {e}")],
                ))
                _free_gpu()

    # --- Summary ----------------------------------------------------------
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    n_ok = 0
    if results:
        print("  -- short-sequence sweep --")
    for r in results:
        status = "PASS" if r.all_ok else "FAIL"
        n_ok += int(r.all_ok)
        print(f"  [{status}] {r.backend:<6} {r.model_name:<42} "
              f"dim={r.dim:<5} load={r.loaded_ms:>6.0f}ms fwd={r.forward_ms:>6.0f}ms")
        for c in r.checks:
            if not c.ok:
                print(f"         ↳ {c}")
    if tokenizer_check is not None:
        status = "PASS" if tokenizer_check.ok else "FAIL"
        print(f"  [{status}] ankh tokeniser comparison  {tokenizer_check.detail}")

    n_chunk_ok = 0
    if chunking_results:
        print("  -- chunking sweep --")
    for r in chunking_results:
        status = "PASS" if r.all_ok else "FAIL"
        n_chunk_ok += int(r.all_ok)
        print(f"  [{status}] {r.backend:<6} {r.model_name:<42} "
              f"dim={r.dim:<5} fwd={r.forward_ms:>6.0f}ms")
        for c in r.checks:
            if not c.ok:
                print(f"         ↳ {c}")

    print(f"\nshort-seq: {n_ok}/{len(results)}  chunking: {n_chunk_ok}/{len(chunking_results)}")
    all_ok = (
        n_ok == len(results)
        and (tokenizer_check is None or tokenizer_check.ok)
        and n_chunk_ok == len(chunking_results)
    )
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
