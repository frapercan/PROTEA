# Stage-1 compute-node regime and its fingerprint (ADR-D48, section 4)

The node's software regime moves stored coordinates without moving any
embedding-config id. That makes it part of the result, so it is written here,
where both machines read it, and checked by a fingerprint rather than asserted.

## The regime (desktop compute node, rebuilt 2026-10-05)

| What | Value | Source |
|---|---|---|
| GPU | NVIDIA GeForce RTX 3060, 12 GB, driver 595.91.07 | hardware |
| Python | 3.14.4 (the server runs 3.12.13) | node |
| PROTEA coordinator | `9d98b6d98ca7` | declaration |
| protea-backends | `9b76719a14d7` (plus the 6 other git siblings at their lock revs) | lock, via node-sync |
| transformers / tokenizers | 4.48.1 / 0.21.4 | lock, pinned by hand |
| esm | 3.2.1.post1 | lock, pinned by hand |
| torch | 2.11.0+cu130 | pinned by hand; the lock has no CUDA build |
| attention | eager (ESM, T5: no SDPA or flash path in 4.48.1); no `flash_attn` | read in the installed code |
| dtype on CUDA | fp16 (esm, t5, esm3c), bf16 (ankh) | backends |

Accepted divergences:

- esm 3.2.1.post1 declares `torchtext`. The lock's 0.18.0 has no wheel for
  Python 3.14, and esm never imports it.
- numpy (2.5.3 against the lock's 2.4.6) and sentencepiece (0.2.2 against
  0.2.1) are left as installed, because nothing in the numeric path requires
  the lock's version.

## The fingerprint

`fingerprint-sequences.fasta` holds 17 fixed sequences, sha256
`2a9ddd4e81a326611bf3c7a137cbd902b0c44d7e1f628698a055f2b84a25f9e3`, chosen by a
deterministic rule:

- 14 sequences by target length (30 to 2500 residues, including the 1020 and
  1023 boundary around `max_length=1022`). Each is the closest length using only
  the 20 standard residues, with ties broken by sequence hash.
- The longest sequence in the store (35,991 residues).
- One sequence containing `X` and one containing `U`.

`fingerprint.py` embeds each sequence with each of the eight stage-1 configs,
**one sequence per forward pass** (batch 1, no padding), through the same
plugin path `compute_embeddings` uses. It records shape, dtype, the sha256 of
the float32 vector the backend returns, and the sha256 of its float16 cast,
which is what the `halfvec` column stores.

### Is byte equality attainable on this GPU? Yes

Measured 2026-10-05: two runs in **separate processes** agree byte for byte on
**136 of 136** vectors (8 configs by 17 sequences), in float32 and in float16.
Every vector is finite, L2-normalised and of shape `(1, dim)`. The 35,991-residue
sequence is truncated, not chunked. The reference output is
`fingerprint-2026-10-05.json` (sha256
`6ba8704aab61e87371e6b420c1109a7f04bc801add2ba7f184e05b78550e6381`).

Because equality is exact, the regime check compares hashes. No numeric
threshold is needed.

### When to run it

- Before the first stage-1 pass and after the last. Any differing hash names
  the config whose numbers moved, even if `pip freeze` looks the same.
- After any change to the node's environment.

```bash
cd <PROTEA checkout at the declared coordinator>
HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 PYTHONPATH=$PWD \
  .venv/bin/python docs/campaign/stage1/fingerprint.py /tmp/fp
# then compare the sha256_f32 / sha256_f16 fields of /tmp/fp.json
# against fingerprint-2026-10-05.json
```

Production passes also run at `batch_size=1`, so a stored vector does not
depend on which sequences shared its batch.
