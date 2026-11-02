"""The code of a residue under a frozen residue vocabulary (ADR-D51).

A residue vocabulary is a sparse dictionary over the hidden states of a few layers of a
protein language model, read at a fixed number of active features, with every active value
quantised to one of a fixed number of levels. This module is the platform's implementation
of the code that the lab's reference encoder specifies (``compresion/scripts/
codificador_referencia.py`` in the sf-jepa repository): given the hidden states of one
sequence, it returns per residue the active features in ascending order and their levels,
and it reads them back.

EXACTNESS IS PART OF THE CONTRACT

Two implementations agree bit for bit only when they run the same operations in the same
order over the same memory layout, so the order below is the recipe and not a detail:

* each layer's states are standardised per dimension, ``(state - mu) / sd`` in single
  precision, rounded to half precision, and concatenated in the artifact's layer order;
* the pre-activation is ``(x - bd) @ We_t + be``, with ``We_t`` stored row major as
  ``(in_dim, dict_dim)``. The lab measured that the transposed layout changes the last bit
  of some products, which is enough to move a feature across its threshold;
* the threshold is ``exp(log_threshold)``, computed on the device that encodes;
* a feature is active when its pre-activation is STRICTLY above its threshold;
* the level is ``floor(p / range * levels)`` evaluated as written, division first, then
  clamped to ``0 .. levels - 1``.

Within one device and one call shape the result is deterministic. Another device, or the
same residues split into other calls, can change the last bit of a product, and a
pre-activation within a few units of the last place of a threshold or of a level edge can
then land on the other side. The lab measured 165 differing entries out of 309 million
between its CPU and its GPU.

torch is imported when a vocabulary is built, not when this module is imported, so the
shard and read paths stay usable on a host without it.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np

#: The artifact kind this module reads. Any other kind is refused.
KIND = "residue-vocabulary"

#: What the artifact's meta must declare. Absent any of them the artifact is refused rather
#: than defaulted, because a default would produce a complete code computed from something
#: else.
REQUIRED_META = (
    "kind",
    "hidden_state_indices",
    "layer_indices",
    "d_model",
    "dict_dim",
    "levels",
    "actives",
    "training_release",
)

#: The arrays the artifact must carry, with their shapes in terms of the meta.
REQUIRED_ARRAYS = (
    "encoder_weight_t",
    "encoder_bias",
    "decoder_weight",
    "decoder_bias",
    "log_threshold",
    "level_range",
    "level_means",
    "mu",
    "sd",
)


@dataclass(frozen=True)
class ResidueCode:
    """The code of one sequence.

    Residue ``i``'s active features are ``features[offsets[i]:offsets[i + 1]]`` in ascending
    order, and their levels (``0 .. levels - 1``) sit at the same positions of ``levels``.
    """

    features: np.ndarray
    levels: np.ndarray
    offsets: np.ndarray

    @property
    def residues(self) -> int:
        """Number of residues the code covers."""
        return int(self.offsets.shape[0]) - 1

    @property
    def entries(self) -> int:
        """Number of active features over all residues."""
        return int(self.features.shape[0])

    def symbols(self, dict_dim: int) -> np.ndarray:
        """Dense level symbols ``(residues, dict_dim)``: 0 when inactive, level + 1 when active."""
        out = np.zeros((self.residues, dict_dim), dtype=np.int16)
        rows = np.repeat(np.arange(self.residues), np.diff(self.offsets))
        out[rows, self.features.astype(np.int64)] = self.levels.astype(np.int16) + 1
        return out


def _check_artifact(arrays: dict[str, np.ndarray], meta: dict[str, Any]) -> None:
    """Refuse an artifact whose meta or arrays do not describe one residue vocabulary.

    Parameters
    ----------
    arrays : dict of str to ndarray
        The artifact's arrays, keyed as in :data:`REQUIRED_ARRAYS`.
    meta : dict
        The artifact's meta, keyed as in :data:`REQUIRED_META`.

    Raises
    ------
    ValueError
        When a field or an array is missing, the kind is not :data:`KIND`, a shape does not
        follow from the meta, or the inactive level does not read back as zero.
    """
    missing = [k for k in REQUIRED_META if k not in meta]
    if missing:
        raise ValueError(f"the vocabulary's meta declares no {missing}")
    if meta["kind"] != KIND:
        raise ValueError(f"artifact kind {meta['kind']!r} is not {KIND!r}")
    absent = [k for k in REQUIRED_ARRAYS if k not in arrays]
    if absent:
        raise ValueError(f"the vocabulary carries no {absent}")
    n_layers = len(meta["layer_indices"])
    if n_layers != len(meta["hidden_state_indices"]):
        raise ValueError("layer_indices and hidden_state_indices name different numbers of layers")
    d_model, dict_dim, levels = int(meta["d_model"]), int(meta["dict_dim"]), int(meta["levels"])
    in_dim = n_layers * d_model
    expected = {
        "encoder_weight_t": (in_dim, dict_dim),
        "encoder_bias": (dict_dim,),
        "decoder_weight": (in_dim, dict_dim),
        "decoder_bias": (in_dim,),
        "log_threshold": (dict_dim,),
        "level_range": (dict_dim,),
        "level_means": (dict_dim, levels + 1),
        "mu": (n_layers, d_model),
        "sd": (n_layers, d_model),
    }
    wrong = {k: (tuple(arrays[k].shape), v) for k, v in expected.items() if arrays[k].shape != v}
    if wrong:
        raise ValueError(f"arrays of the wrong shape (got, expected): {wrong}")
    if not 1 <= levels <= 256:
        raise ValueError(f"{levels} levels do not fit in one byte")
    if dict_dim > 32768:
        raise ValueError(f"{dict_dim} features do not fit in int16 indices")
    if np.any(arrays["level_means"][:, 0] != 0):
        raise ValueError("the inactive symbol must read back as 0")


class ResidueVocabulary:
    """A frozen residue vocabulary, ready to encode on one device.

    Parameters
    ----------
    arrays : dict of str to ndarray
        The artifact's arrays (:data:`REQUIRED_ARRAYS`).
    meta : dict
        The artifact's meta (:data:`REQUIRED_META`).
    device : str
        Where the products run, ``"cpu"`` or a CUDA device.
    """

    def __init__(self, arrays: dict[str, np.ndarray], meta: dict[str, Any], device: str = "cpu"):
        import torch

        _check_artifact(arrays, meta)
        self.meta = dict(meta)
        self.dict_dim = int(meta["dict_dim"])
        self.n_levels = int(meta["levels"])
        self.n_layers = len(meta["layer_indices"])
        self.d_model = int(meta["d_model"])
        self.in_dim = self.n_layers * self.d_model
        dev = torch.device(device)
        self.device = dev

        def tensor(name: str) -> torch.Tensor:
            return torch.as_tensor(np.ascontiguousarray(arrays[name], dtype=np.float32)).to(dev)

        # row major (in_dim, dict_dim), whatever layout the arrays arrived in
        self._we_t = tensor("encoder_weight_t").contiguous()
        self._be = tensor("encoder_bias")
        self._bd = tensor("decoder_bias")
        self._wd = tensor("decoder_weight").contiguous()
        # exp on the encoding device, as the reference takes it from the loaded log threshold
        self._threshold = tensor("log_threshold").exp()
        self._range = tensor("level_range")
        self._means = np.ascontiguousarray(arrays["level_means"], dtype=np.float32)
        self._mu = tensor("mu")
        self._sd = tensor("sd")

    def standardise(self, layer_states: Sequence[Any]) -> Any:
        """Standardised input ``(residues, in_dim)`` from one sequence's raw layer states.

        Parameters
        ----------
        layer_states : sequence of array-like
            One ``(residues, d_model)`` array per layer, in the artifact's layer order.

        Returns
        -------
        torch.Tensor
            Single precision values that went through half precision, as the lab's residue
            caches store them.
        """
        import torch

        if len(layer_states) != self.n_layers:
            raise ValueError(
                f"{len(layer_states)} layers given, the vocabulary reads {self.n_layers}"
            )
        parts = []
        for i, s in enumerate(layer_states):
            t = torch.as_tensor(s, device=self.device).float()
            parts.append(((t - self._mu[i]) / self._sd[i]).to(torch.float16))
        return torch.cat(parts, 1).float()

    def preactivation(self, x: Any) -> Any:
        """Pre-activations ``(residues, dict_dim)`` of a standardised input."""
        import torch

        x = torch.as_tensor(x, device=self.device).float()
        if x.ndim != 2 or x.shape[1] != self.in_dim:
            raise ValueError(
                f"input of shape {tuple(x.shape)}, the vocabulary reads (n, {self.in_dim})"
            )
        return (x - self._bd) @ self._we_t + self._be

    def symbols_of(self, p: Any) -> Any:
        """Level symbols ``(residues, dict_dim)`` from pre-activations: 0 inactive, 1..levels active."""
        import torch

        s = torch.floor(p / self._range * self.n_levels).clamp(0, self.n_levels - 1).long() + 1
        return torch.where(p > self._threshold, s, torch.zeros_like(s))

    def symbols(self, x: Any) -> Any:
        """Level symbols of a standardised input."""
        return self.symbols_of(self.preactivation(x))

    def encode(self, x: Any) -> ResidueCode:
        """The code of a standardised input ``(residues, in_dim)``."""
        import torch

        s = self.symbols(x)
        r, c = torch.nonzero(s, as_tuple=True)
        counts = torch.bincount(r, minlength=s.shape[0]).cpu().numpy()
        return ResidueCode(
            features=c.to(torch.int16).cpu().numpy(),
            levels=(s[r, c] - 1).to(torch.uint8).cpu().numpy(),
            offsets=np.concatenate([[0], np.cumsum(counts)]).astype(np.int64),
        )

    def encode_states(self, layer_states: Sequence[Any]) -> ResidueCode:
        """The code of one sequence from its raw layer states (see :meth:`standardise`)."""
        return self.encode(self.standardise(layer_states))

    def read_back(self, code: ResidueCode) -> np.ndarray:
        """Read back values ``(residues, dict_dim)``: each level's bin mean, 0 when inactive."""
        out = np.zeros((code.residues, self.dict_dim), dtype=np.float32)
        rows = np.repeat(np.arange(code.residues), np.diff(code.offsets))
        f = code.features.astype(np.int64)
        out[rows, f] = self._means[f, code.levels.astype(np.int64) + 1]
        return out

    def reconstruct(self, values: Any) -> Any:
        """The layers' reconstruction ``(residues, in_dim)`` from read back values."""
        import torch

        v = torch.as_tensor(values, device=self.device).float()
        return v @ self._wd.t() + self._bd


def load_residue_vocabulary(path: str, device: str = "cpu") -> ResidueVocabulary:
    """Read a residue vocabulary artifact (an ``.npz``) and build it on a device.

    The meta travels as a JSON string under the key ``meta_json``, so the file loads without
    pickle.

    Parameters
    ----------
    path : str
        Local path of the artifact, as :func:`resolve_encoder_artifact` returns it.
    device : str
        Where the products run.

    Returns
    -------
    ResidueVocabulary
    """
    with np.load(path, allow_pickle=False) as data:
        if "meta_json" not in data:
            raise ValueError(f"{path} carries no meta_json, so it does not declare a recipe")
        meta = json.loads(str(data["meta_json"]))
        arrays = {k: np.asarray(data[k]) for k in REQUIRED_ARRAYS if k in data}
    return ResidueVocabulary(arrays, meta, device=device)
