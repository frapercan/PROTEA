"""Anc2Vec GO-term embedding index.

Loads the 200-dim pre-trained dictionary shipped by
https://github.com/aedera/anc2vec (GO release 2020-10-06) from a compact
``.npz`` cached under ``artifacts/anc2vec/anc2vec_2020-10.npz``.

Exposes a zero-copy lookup by ``GO:`` id and a batched variant that returns
an (N, D) matrix, filling rows for unknown terms with the zero vector so
that downstream cosine operations degrade to 0 rather than NaN.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import numpy as np

_DEFAULT_PATH = (
    Path(__file__).resolve().parents[2] / "artifacts" / "anc2vec" / "anc2vec_2020-10.npz"
)


class Anc2VecIndex:
    __slots__ = ("embeddings", "go_ids", "_idx", "dim", "release")

    def __init__(self, path: str | Path | None = None) -> None:
        src = Path(path) if path else _DEFAULT_PATH
        data = np.load(src, allow_pickle=True)
        self.embeddings = np.ascontiguousarray(data["embeddings"], dtype=np.float32)
        self.go_ids = [str(g) for g in data["go_ids"]]
        self._idx = {g: i for i, g in enumerate(self.go_ids)}
        self.dim = int(self.embeddings.shape[1])
        self.release = str(data["ontology_release"]) if "ontology_release" in data.files else ""

    def __len__(self) -> int:
        return len(self.go_ids)

    def __contains__(self, go_id: str) -> bool:
        return go_id in self._idx

    def vec(self, go_id: str) -> np.ndarray | None:
        i = self._idx.get(go_id)
        return self.embeddings[i] if i is not None else None

    def batch(self, go_ids: list[str], *, zero_if_missing: bool = True) -> np.ndarray:
        """Return (N, dim) matrix; missing rows are zero (or NaN if disabled)."""
        fill = 0.0 if zero_if_missing else np.nan
        out = np.full((len(go_ids), self.dim), fill, dtype=np.float32)
        for row, g in enumerate(go_ids):
            i = self._idx.get(g)
            if i is not None:
                out[row] = self.embeddings[i]
        return out


@lru_cache(maxsize=2)
def get_index(path: str | None = None) -> Anc2VecIndex:
    """Return a process-wide singleton index (keyed by path)."""
    return Anc2VecIndex(path)
