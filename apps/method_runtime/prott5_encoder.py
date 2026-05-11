"""Mean-pooled ProtT5 embedder for the protea-method-runtime container.

Vendored from ``apps/lafa_container/prott5_encoder.py``. Kept in the
method-runtime tree so the Tier 3 base image (ADR-D15) does not depend
on the LAFA v1 wrapper layout, which is scheduled for replacement once
the LAFA v2 containers rebase on top of this image (ADR-D23).
"""

from __future__ import annotations

import time
from collections.abc import Iterable

import numpy as np
import torch
from transformers import T5EncoderModel, T5Tokenizer

_MODEL_NAME = "Rostlab/prot_t5_xl_half_uniref50-enc"


def _load_model(cache_dir: str | None) -> tuple[T5EncoderModel, T5Tokenizer, torch.device]:
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    model = T5EncoderModel.from_pretrained(_MODEL_NAME, cache_dir=cache_dir)
    if device.type == "cpu":
        model = model.to(torch.float32)
    model = model.to(device).eval()
    tokenizer = T5Tokenizer.from_pretrained(_MODEL_NAME, do_lower_case=False, cache_dir=cache_dir)
    return model, tokenizer, device


def _prepare(seq: str) -> str:
    return " ".join(seq.replace("U", "X").replace("Z", "X").replace("O", "X"))


def embed_sequences(
    sequences: dict[str, str],
    *,
    cache_dir: str | None = None,
    max_residues: int = 4000,
    max_seq_len: int = 1000,
    max_batch: int = 100,
) -> dict[str, np.ndarray]:
    """Return one mean-pooled vector per accession.

    Sorts sequences by descending length so short tails batch efficiently;
    falls back to single-sequence processing for sequences > ``max_seq_len``.
    """
    if not sequences:
        return {}

    model, tokenizer, device = _load_model(cache_dir)

    items = sorted(sequences.items(), key=lambda kv: -len(kv[1]))
    embeddings: dict[str, np.ndarray] = {}

    start = time.time()
    batch: list[tuple[str, str, int]] = []
    for idx, (acc, seq) in enumerate(items, 1):
        prepared = _prepare(seq)
        seq_len = len(seq)
        batch.append((acc, prepared, seq_len))

        n_res = sum(s_len for _, _, s_len in batch) + seq_len
        flush = (
            len(batch) >= max_batch
            or n_res >= max_residues
            or idx == len(items)
            or seq_len > max_seq_len
        )
        if not flush:
            continue

        accs, seqs, lens = zip(*batch, strict=True)
        batch = []

        token_encoding = tokenizer.batch_encode_plus(
            list(seqs), add_special_tokens=True, padding="longest"
        )
        input_ids = torch.tensor(token_encoding["input_ids"]).to(device)
        attention_mask = torch.tensor(token_encoding["attention_mask"]).to(device)

        try:
            with torch.no_grad():
                hidden = model(input_ids, attention_mask=attention_mask).last_hidden_state
        except RuntimeError as exc:
            print(f"[prott5_encoder] OOM/error on batch with longest L={lens[0]}: {exc}")
            continue

        for b_idx, ident in enumerate(accs):
            s_len = lens[b_idx]
            vec = hidden[b_idx, :s_len].mean(dim=0).detach().cpu().numpy().astype(np.float32)
            embeddings[ident] = vec

    elapsed = time.time() - start
    print(
        f"[prott5_encoder] {len(embeddings)} embeddings in {elapsed:.1f}s "
        f"({elapsed / max(1, len(embeddings)):.3f}s/protein, device={device})"
    )
    return embeddings


def parse_fasta(path: str) -> dict[str, str]:
    """Read a FASTA file into ``{accession: sequence}``.

    Accession is the substring between the first two ``|`` if present
    (UniProt-style ``sp|P12345|name``), else the full id token.
    """
    seqs: dict[str, str] = {}
    current: str | None = None
    with open(path) as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            if line.startswith(">"):
                header = line[1:].split()[0]
                parts = header.split("|")
                current = parts[1] if len(parts) >= 2 else header
                seqs[current] = ""
            elif current is not None:
                seqs[current] += line.upper().replace("-", "")
    return seqs


def fasta_accessions(path: str) -> list[str]:
    """Return accessions in FASTA order (stable for output ordering)."""
    accs: list[str] = []
    with open(path) as handle:
        for raw in handle:
            if raw.startswith(">"):
                header = raw[1:].strip().split()[0]
                parts = header.split("|")
                accs.append(parts[1] if len(parts) >= 2 else header)
    return accs


def keys_as_array(seqs: Iterable[str]) -> list[str]:
    return list(seqs)
