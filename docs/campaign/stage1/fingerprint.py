"""Regime fingerprint for the stage-1 embedding configs (ADR-D48, section 4).

Embeds every sequence of fingerprint-sequences.fasta with every stage-1 config,
ONE SEQUENCE PER FORWARD PASS (batch 1, no padding), through the same backend
plugin path compute_embeddings uses (plugin.load_model + plugin.embed_chunks).
For each vector it records shape, dtype, the sha256 of the float32 vector the
backend returns, and the sha256 of its float16 cast, which is what the halfvec
column stores. Writes <out>.json and <out>.npz.

Usage: python fingerprint.py <out_prefix>
Run it twice in separate processes and compare the two outputs.
"""

from __future__ import annotations

import gc
import hashlib
import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import torch

from protea.core.operations.compute_embeddings import _effective_device, _resolve_backend

HERE = Path(__file__).resolve().parent
FASTA = HERE / "fingerprint-sequences.fasta"

RECIPE = dict(
    layer_indices=[0], layer_agg="mean", pooling="mean", normalize=True,
    normalize_residues=False, max_length=1022, use_chunking=False,
    chunk_size=512, chunk_overlap=0, embedding_scale=1.0,
)
CONFIGS = [  # (key, embedding_config_id, model_name, backend), ADR-D48 order
    ("esm2_150m", "336a2bcb-d35e-50c5-ba7e-9ae35f1a1dcb", "facebook/esm2_t30_150M_UR50D", "esm"),
    ("ankh_base", "e7ba37da-c18d-5261-b5b4-49a3093d9d4d", "ElnaggarLab/ankh-base", "ankh"),
    ("esm2_650m", "9c4ea46b-c8c9-5cdb-af36-14df75605dd2", "facebook/esm2_t33_650M_UR50D", "esm"),
    ("esmc_600m", "38e079cd-0613-5ea7-b6dc-f70f0c419731", "esmc_600m", "esm3c"),
    ("prot_t5", "7d409774-ec37-5606-a90c-95646ce813ac", "Rostlab/prot_t5_xl_half_uniref50-enc", "t5"),
    ("prostt5", "cd00a743-8215-5ce9-8ad3-2846d9f590db", "Rostlab/ProstT5", "t5"),
    ("ankh_large", "2b27f22a-ed4c-50a8-9068-ec18be8d4a46", "ElnaggarLab/ankh-large", "ankh"),
    ("esm2_3b", "6fef8557-ca97-55c4-b75a-e9fca7d8dd93", "facebook/esm2_t36_3B_UR50D", "esm"),
]


def _read_fasta(path: Path) -> list[tuple[str, str]]:
    records, name, buf = [], None, []
    for line in path.read_text().splitlines():
        if line.startswith(">"):
            if name is not None:
                records.append((name, "".join(buf)))
            name, buf = line[1:].split()[0], []
        else:
            buf.append(line.strip())
    if name is not None:
        records.append((name, "".join(buf)))
    return records


def _noop_emit(*_args, **_kwargs) -> None:
    return None


def main(out_prefix: str) -> None:
    seqs = _read_fasta(FASTA)
    device = _effective_device("cuda")
    result = {
        "fasta_sha256": hashlib.sha256(FASTA.read_bytes()).hexdigest(),
        "torch": torch.__version__,
        "cuda": torch.version.cuda,
        "gpu": torch.cuda.get_device_name(0) if torch.cuda.is_available() else None,
        "device": device,
        "configs": {},
    }
    arrays: dict[str, np.ndarray] = {}
    for key, cid, model_name, backend in CONFIGS:
        cfg = SimpleNamespace(id=cid, model_name=model_name, model_backend=backend, **RECIPE)
        plugin = _resolve_backend(backend)
        t0 = time.time()
        model, tokenizer = plugin.load_model(model_name, device, _noop_emit)
        load_s = time.time() - t0
        per_seq = {}
        t1 = time.time()
        for seq_hash, seq in seqs:
            chunks = plugin.embed_chunks(model, tokenizer, [seq], cfg, device)
            assert len(chunks) == 1, f"{key}: expected one result per sequence"
            vecs = [np.asarray(c.vector, dtype=np.float32) for c in chunks[0]]
            v32 = np.stack(vecs)
            v16 = v32.astype(np.float16)
            arrays[f"{key}__{seq_hash}"] = v32
            per_seq[seq_hash] = {
                "n_chunks": int(v32.shape[0]),
                "shape": list(v32.shape),
                "dtype_backend": str(np.asarray(chunks[0][0].vector).dtype),
                "sha256_f32": hashlib.sha256(v32.tobytes()).hexdigest(),
                "sha256_f16": hashlib.sha256(v16.tobytes()).hexdigest(),
                "norm": float(np.linalg.norm(v32[0])),
                "finite": bool(np.isfinite(v32).all()),
            }
        result["configs"][key] = {
            "embedding_config_id": cid,
            "model_name": model_name,
            "backend": backend,
            "load_seconds": round(load_s, 1),
            "embed_seconds": round(time.time() - t1, 2),
            "sequences": per_seq,
        }
        print(f"{key}: load {load_s:.1f}s embed {time.time() - t1:.2f}s", flush=True)
        del model, tokenizer
        gc.collect()
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
    Path(f"{out_prefix}.json").write_text(json.dumps(result, indent=1, sort_keys=True))
    np.savez_compressed(f"{out_prefix}.npz", **arrays)
    print("written", out_prefix, flush=True)


if __name__ == "__main__":
    main(sys.argv[1])
