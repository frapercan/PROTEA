"""Per-PLM mean-pool encoder registry for the protea-knn-8plm container.

Eight protein language models, one mean-pooled vector per query. Each
encoder loads weights lazily (HuggingFace cache or ESM SDK), runs
inference on CPU or CUDA depending on availability, and returns a
``dict[accession, np.ndarray]`` so the ensemble driver can call them
uniformly.

Design rules:

* No heavy imports at module load. ``torch`` and ``transformers`` are
  imported inside each loader so unit tests (and the entrypoint
  argparse surface) stay cheap.
* Each encoder is a small callable, registered in :data:`PLM_REGISTRY`.
  The container ships all eight; the driver iterates over the keys
  selected by ``--plms`` (default: every key).
* Mean-pool over the residue axis is the canonical pooling for the
  knn-v1 baseline. Keeping the same pooling here lets An Phan's
  evaluator compare the ensemble against the v1 submission on
  identical query-side conditions.
* The ProtT5 path delegates to ``prott5_encoder.embed_sequences`` from
  the base ``protea-method-runtime`` image so we do not fork the v1
  encoder.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np

#: Public type for a per-PLM encoder. ``cache_dir`` is the HuggingFace
#: cache root, ``device`` is a torch device string (``"cpu"`` /
#: ``"cuda:0"``). Encoders ignore options they do not need.
EncoderFn = Callable[[dict[str, str], str | None, str | None], dict[str, np.ndarray]]


@dataclass(frozen=True)
class PLMSpec:
    """Identity + checkpoint pointer for one ensemble member."""

    key: str
    family: str  # esm2 / prot_t5 / prostt5 / ankh / esmc
    checkpoint: str  # HF id or ESM SDK key
    note: str

    @property
    def display(self) -> str:
        return f"{self.key} ({self.family}, {self.checkpoint})"


def _device() -> str:
    """Pick the best available torch device without paying for the import early."""
    import torch  # local import: ``torch`` is heavy

    return "cuda:0" if torch.cuda.is_available() else "cpu"


def _prepare_t5_seq(seq: str) -> str:
    """Same residue rewriting + space separation used by the v1 ProtT5 encoder."""
    return " ".join(seq.replace("U", "X").replace("Z", "X").replace("O", "X"))


def _encode_prott5(
    sequences: dict[str, str],
    cache_dir: str | None,
    device: str | None,
) -> dict[str, np.ndarray]:
    """Wrap the v1 ProtT5 encoder shipped by ``protea-method-runtime``."""
    from prott5_encoder import embed_sequences  # type: ignore[import-not-found]

    return embed_sequences(sequences, cache_dir=cache_dir)


def _encode_t5_family(
    sequences: dict[str, str],
    cache_dir: str | None,
    device: str | None,
    *,
    checkpoint: str,
) -> dict[str, np.ndarray]:
    """Mean-pool a T5EncoderModel (ProtT5 backbone or ProstT5).

    Used for ``prostt5`` only; the canonical ProtT5 path goes through
    :func:`_encode_prott5` (vendored from the v1 wrapper). Splitting the
    two means the 8plm container reuses the v1 implementation byte-for-
    byte instead of risking a subtle reimplementation drift.
    """
    import torch
    from transformers import T5EncoderModel, T5Tokenizer

    if not sequences:
        return {}

    dev = torch.device(device or _device())
    tokenizer = T5Tokenizer.from_pretrained(checkpoint, do_lower_case=False, cache_dir=cache_dir)
    model = T5EncoderModel.from_pretrained(checkpoint, cache_dir=cache_dir)
    model = model.to(dev).eval()

    out: dict[str, np.ndarray] = {}
    items = sorted(sequences.items(), key=lambda kv: -len(kv[1]))
    with torch.no_grad():
        for acc, seq in items:
            prepared = _prepare_t5_seq(seq)
            enc = tokenizer.batch_encode_plus(
                [prepared], add_special_tokens=True, padding="longest"
            )
            ids = torch.tensor(enc["input_ids"]).to(dev)
            mask = torch.tensor(enc["attention_mask"]).to(dev)
            hidden = model(ids, attention_mask=mask).last_hidden_state
            vec = hidden[0, : len(seq)].mean(dim=0).detach().cpu().numpy().astype(np.float32)
            out[acc] = vec
    return out


def _encode_esm2(
    sequences: dict[str, str],
    cache_dir: str | None,
    device: str | None,
    *,
    checkpoint: str,
) -> dict[str, np.ndarray]:
    """Mean-pool an HuggingFace ``EsmModel`` (esm2 sizes 150M / 650M / 3B)."""
    import torch
    from transformers import AutoTokenizer, EsmModel

    if not sequences:
        return {}

    dev = torch.device(device or _device())
    tokenizer = AutoTokenizer.from_pretrained(checkpoint, cache_dir=cache_dir)
    model = EsmModel.from_pretrained(checkpoint, cache_dir=cache_dir).eval().to(dev)

    out: dict[str, np.ndarray] = {}
    with torch.no_grad():
        for acc, seq in sequences.items():
            tokens = tokenizer(seq, return_tensors="pt", truncation=True, add_special_tokens=True)
            tokens = {k: v.to(dev) for k, v in tokens.items()}
            hidden = model(**tokens).last_hidden_state
            # Strip CLS (pos 0) and EOS (last valid) to match the
            # protea-backends ESM convention (see esm/__init__.py).
            actual_len = int(tokens["attention_mask"].sum().item())
            residues = hidden[0, 1 : actual_len - 1, :]
            out[acc] = residues.mean(dim=0).detach().cpu().numpy().astype(np.float32)
    return out


def _encode_ankh(
    sequences: dict[str, str],
    cache_dir: str | None,
    device: str | None,
    *,
    checkpoint: str,
) -> dict[str, np.ndarray]:
    """Mean-pool the Ankh family (T5EncoderModel via AutoTokenizer)."""
    import torch
    from transformers import AutoTokenizer, T5EncoderModel

    if not sequences:
        return {}

    dev = torch.device(device or _device())
    tokenizer = AutoTokenizer.from_pretrained(checkpoint, cache_dir=cache_dir)
    model = T5EncoderModel.from_pretrained(checkpoint, cache_dir=cache_dir).eval().to(dev)

    out: dict[str, np.ndarray] = {}
    with torch.no_grad():
        for acc, seq in sequences.items():
            tokens = tokenizer(seq, return_tensors="pt", truncation=True, add_special_tokens=True)
            tokens = {k: v.to(dev) for k, v in tokens.items()}
            hidden = model(**tokens).last_hidden_state
            actual_len = int(tokens["attention_mask"].sum().item())
            residues = hidden[0, : max(1, actual_len - 1), :]
            out[acc] = residues.mean(dim=0).detach().cpu().numpy().astype(np.float32)
    return out


def _encode_esmc(
    sequences: dict[str, str],
    cache_dir: str | None,
    device: str | None,
    *,
    checkpoint: str,
) -> dict[str, np.ndarray]:
    """Mean-pool an ESM-C (Cambrian) model via the EvolutionaryScale SDK."""
    import torch
    from esm.models.esmc import ESMC  # type: ignore[import-not-found]
    from esm.sdk.api import ESMProtein, LogitsConfig  # type: ignore[import-not-found]

    if not sequences:
        return {}

    dev = device or _device()
    model = ESMC.from_pretrained(checkpoint).to(dev).eval()

    out: dict[str, np.ndarray] = {}
    with torch.no_grad():
        for acc, seq in sequences.items():
            protein = ESMProtein(sequence=seq)
            tensor = model.encode(protein)
            logits = model.logits(tensor, LogitsConfig(sequence=True, return_embeddings=True))
            embeddings = logits.embeddings  # (1, L+2, D)
            residues = embeddings[0, 1:-1, :]  # strip CLS + EOS
            out[acc] = residues.mean(dim=0).detach().cpu().numpy().astype(np.float32)
    return out


def _make_encoder(family: str, checkpoint: str) -> EncoderFn:
    """Bind a family-specific encoder with its checkpoint."""
    if family == "prot_t5":
        return _encode_prott5
    if family == "prostt5":
        return lambda seqs, cache, dev: _encode_t5_family(
            seqs, cache, dev, checkpoint=checkpoint
        )
    if family == "esm2":
        return lambda seqs, cache, dev: _encode_esm2(
            seqs, cache, dev, checkpoint=checkpoint
        )
    if family == "ankh":
        return lambda seqs, cache, dev: _encode_ankh(
            seqs, cache, dev, checkpoint=checkpoint
        )
    if family == "esmc":
        return lambda seqs, cache, dev: _encode_esmc(
            seqs, cache, dev, checkpoint=checkpoint
        )
    raise ValueError(f"unknown PLM family: {family}")


#: Canonical 8-PLM line-up for the ensemble. The key is the bundle
#: subdirectory name (``<bundle>/plms/<key>/reference_embeddings.parquet``)
#: and the CLI selector; the checkpoint is the HuggingFace / ESM SDK id.
PLM_SPECS: tuple[PLMSpec, ...] = (
    PLMSpec(
        key="esm2_150m",
        family="esm2",
        checkpoint="facebook/esm2_t30_150M_UR50D",
        note="ESM-2 150M (30 layers).",
    ),
    PLMSpec(
        key="esm2_650m",
        family="esm2",
        checkpoint="facebook/esm2_t33_650M_UR50D",
        note="ESM-2 650M (33 layers).",
    ),
    PLMSpec(
        key="esm2_3b",
        family="esm2",
        checkpoint="facebook/esm2_t36_3B_UR50D",
        note="ESM-2 3B (36 layers).",
    ),
    PLMSpec(
        key="prot_t5",
        family="prot_t5",
        checkpoint="Rostlab/prot_t5_xl_half_uniref50-enc",
        note="ProtT5-XL UniRef50 (half precision encoder).",
    ),
    PLMSpec(
        key="prostt5",
        family="prostt5",
        checkpoint="Rostlab/ProstT5",
        note="ProstT5 (T5 backbone, AA + 3Di joint vocab; AA-only inference).",
    ),
    PLMSpec(
        key="ankh_base",
        family="ankh",
        checkpoint="ElnaggarLab/ankh-base",
        note="Ankh-base (T5 backbone).",
    ),
    PLMSpec(
        key="ankh_large",
        family="ankh",
        checkpoint="ElnaggarLab/ankh-large",
        note="Ankh-large (T5 backbone).",
    ),
    PLMSpec(
        key="esmc_600m",
        family="esmc",
        checkpoint="esmc_600m",
        note="ESM-C 600M (Cambrian) via EvolutionaryScale SDK.",
    ),
)


def build_registry() -> dict[str, EncoderFn]:
    """Return ``{plm_key: encoder_fn}`` for the canonical 8-PLM ensemble.

    Callers can override single entries (e.g. for unit tests) by
    mutating the returned dict before passing it to the ensemble driver.
    """
    return {spec.key: _make_encoder(spec.family, spec.checkpoint) for spec in PLM_SPECS}


def list_plm_keys() -> list[str]:
    """Convenience: ordered list of the 8 ensemble member keys."""
    return [spec.key for spec in PLM_SPECS]


def spec_for(key: str) -> PLMSpec:
    """Return the :class:`PLMSpec` for ``key``; raises ``KeyError`` on miss."""
    for spec in PLM_SPECS:
        if spec.key == key:
            return spec
    raise KeyError(f"unknown PLM key: {key}")
