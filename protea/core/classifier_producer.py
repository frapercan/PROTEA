"""Native full-vocabulary GO-term classifier producer (lafa-integrate INT-4).

The KNN path only proposes GO terms that some reference neighbour already
carries. The classifier proposes terms across the WHOLE training vocabulary
from the query protein's frozen PLM embeddings alone, so the platform can
compute a LAFA-style score from zero rather than depending on neighbour
recall.

Architecture (ported faithfully from
``protea-reranker-lab/fullgo/train_classifier_m2.py``, the ``Hybrid``
``nn.Module``): a two-layer trunk (``Linear -> LayerNorm -> GELU -> Dropout``
twice), an independent head ``Linear(hidden, V)`` and a label-similarity head
``proj(h) @ Lt.T`` scaled by a learned scalar, where ``Lt`` is the L2
normalised anc2vec embedding of each vocabulary term. ``forward`` returns
``indep(h) + scale * (proj(h) @ Lt.T)``; the per-term score is its sigmoid.

The input is the concatenation of six mean-pooled frozen PLM embeddings PER
PROTEIN, in the EXACT training order (see :data:`PLM_CONCAT_ORDER`):
Ankh-base 768, ESM2-3B 2560, Ankh-large 1536, ESM2-650M 1280, ESMC-600M 1152,
ProtT5 1024 = 8320-d, then standardised with the training ``mu`` / ``sd``
(both shipped inside the checkpoint).

The checkpoint (``classifier_m2_anc2vec.pt``) is self-contained: it carries
``state_dict``, ``mu``, ``sd``, ``in_dim``, ``hidden``, ``label_dim``,
``vocab`` and ``arch``, so no sibling vocab / normalisation artifact is
required. The anc2vec label matrix is rebuilt for the checkpoint vocab from
``anc2vec_2020-10.npz`` exactly as training did (L2 normalised, zero for
missing terms).
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass

import numpy as np
import torch
from torch import nn

# Canonical 6-PLM concat order (ADR D35 config ids). The order is
# load-bearing: it MUST match the order the classifier trained on
# (protea-reranker-lab/fullgo/config.yaml ``plm_configs``). Each tuple is
# ``(plm_key, embedding_config_id, dim)``; the dims sum to 8320.
PLM_CONCAT_ORDER: tuple[tuple[str, str, int], ...] = (
    ("ankh_base", "08234f06-ba76-4d7d-aaec-ae601096b4fa", 768),
    ("esm2_3b", "55e43f1c-1a3b-4b1d-88c0-26b433f5f673", 2560),
    ("ankh_large", "238f79b1-3068-4c6f-9013-5cc52b4f662b", 1536),
    ("esm2_650m", "c2e9dda3-e505-4170-b50d-435a451761ac", 1280),
    ("esmc_600m", "2bf1e753-022f-44b8-a131-9a90acb4024e", 1152),
    ("prott5", "084943c6-fec1-441d-bdc5-63b0268ada1b", 1024),
)

CLASSIFIER_INPUT_DIM = sum(dim for _, _, dim in PLM_CONCAT_ORDER)  # 8320

# Default artifact locations. Overridable via env so the dev box, the
# container and CI can each point at their own copies.
_DEFAULT_MODEL_PATH = "/home/frapercan/Thesis2/storage/fullgo_models/classifier_m2_anc2vec.pt"
_DEFAULT_ANC2VEC_PATH = (
    "/home/frapercan/Thesis2/worktrees/protea-deploy/artifacts/anc2vec/anc2vec_2020-10.npz"
)
_MODEL_ENV = "PROTEA_CLASSIFIER_MODEL_PATH"
_ANC2VEC_ENV = "PROTEA_CLASSIFIER_ANC2VEC_PATH"


@dataclass(frozen=True)
class ClassifierPrediction:
    """One classifier-proposed term for one query protein."""

    accession: str
    go_id: str
    score: float


def build_hybrid(in_dim: int, hidden: int, vocab_size: int, label_dim: int) -> nn.Module:
    """Return the ``Hybrid`` nn.Module, a faithful copy of the lab module.

    Trunk: ``Linear -> LayerNorm -> GELU -> Dropout`` twice. Independent head
    ``Linear(hidden, V)``. Label head ``proj(h) @ Lt.T`` with a learned scale.
    """

    class Hybrid(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.trunk = nn.Sequential(
                nn.Linear(in_dim, hidden),
                nn.LayerNorm(hidden),
                nn.GELU(),
                nn.Dropout(0.2),
                nn.Linear(hidden, hidden),
                nn.LayerNorm(hidden),
                nn.GELU(),
                nn.Dropout(0.2),
            )
            self.indep = nn.Linear(hidden, vocab_size)
            self.proj = nn.Linear(hidden, label_dim)
            self.scale = nn.Parameter(torch.tensor(1.0))

        def forward(self, x: torch.Tensor, label_matrix: torch.Tensor) -> torch.Tensor:
            h = self.trunk(x)
            return self.indep(h) + self.scale * (self.proj(h) @ label_matrix.t())

    return Hybrid()


def _label_matrix_for_vocab(vocab: list[str], anc2vec_path: str, label_dim: int) -> np.ndarray:
    """L2-normalised anc2vec embedding per vocab term (zero for missing).

    Mirrors the training label-matrix build in ``train_classifier_m2.py``.
    """
    arr = np.load(anc2vec_path, allow_pickle=True)
    index = {str(g): i for i, g in enumerate(arr["go_ids"])}
    emb = arr["embeddings"]
    matrix = np.zeros((len(vocab), label_dim), np.float32)
    for i, term in enumerate(vocab):
        j = index.get(term)
        if j is not None:
            matrix[i] = emb[j]
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    return (matrix / (norms + 1e-8)).astype(np.float32)


class FullVocabClassifier:
    """Loaded full-vocabulary classifier ready for batch inference.

    Constructed once per worker (cache via :func:`get_classifier`), NOT per
    protein. Loads the checkpoint (``state_dict`` + ``mu`` / ``sd`` + ``vocab``),
    rebuilds the anc2vec label matrix and moves everything onto GPU when
    available else CPU.
    """

    def __init__(self, model_path: str | None = None, anc2vec_path: str | None = None) -> None:
        model_path = model_path or os.environ.get(_MODEL_ENV, _DEFAULT_MODEL_PATH)
        anc2vec_path = anc2vec_path or os.environ.get(_ANC2VEC_ENV, _DEFAULT_ANC2VEC_PATH)
        ckpt = torch.load(model_path, map_location="cpu", weights_only=False)
        self.vocab: list[str] = [str(t) for t in ckpt["vocab"]]
        self.in_dim = int(ckpt["in_dim"])
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.mu = torch.as_tensor(np.asarray(ckpt["mu"]), dtype=torch.float32).to(self.device)
        self.sd = torch.as_tensor(np.asarray(ckpt["sd"]), dtype=torch.float32).to(self.device)
        label_matrix = _label_matrix_for_vocab(
            self.vocab, anc2vec_path, int(ckpt["label_dim"])
        )
        self.label_matrix = torch.as_tensor(label_matrix, dtype=torch.float32).to(self.device)
        model = build_hybrid(
            self.in_dim, int(ckpt["hidden"]), len(self.vocab), int(ckpt["label_dim"])
        )
        model.load_state_dict(ckpt["state_dict"])
        model.eval()
        self.model = model.to(self.device)

    def predict(
        self, features: np.ndarray, accessions: list[str], top_n: int = 100, min_score: float = 0.01
    ) -> list[ClassifierPrediction]:
        """Top-N (term, score) per protein from the 8320-d feature matrix.

        ``features`` is ``(N, in_dim)`` raw concatenated PLM embeddings (the
        same order as :data:`PLM_CONCAT_ORDER`); standardisation with the
        training ``mu`` / ``sd`` happens here. Returns a flat list filtered to
        ``score >= min_score``.
        """
        if features.shape[0] == 0:
            return []
        x = torch.as_tensor(np.ascontiguousarray(features), dtype=torch.float32).to(self.device)
        x = (x - self.mu) / self.sd
        out: list[ClassifierPrediction] = []
        with torch.no_grad():
            for i in range(0, x.shape[0], 512):
                scores = torch.sigmoid(self.model(x[i : i + 512], self.label_matrix)).cpu().numpy()
                out.extend(self._rows_for_block(scores, accessions, i, top_n, min_score))
        return out

    def _rows_for_block(
        self,
        scores: np.ndarray,
        accessions: list[str],
        offset: int,
        top_n: int,
        min_score: float,
    ) -> list[ClassifierPrediction]:
        """Top-N rows above ``min_score`` for one inference block."""
        rows: list[ClassifierPrediction] = []
        for r in range(scores.shape[0]):
            acc = accessions[offset + r]
            for j in np.argsort(-scores[r])[:top_n]:
                s = float(scores[r, j])
                if s >= min_score:
                    rows.append(ClassifierPrediction(acc, self.vocab[j], s))
        return rows


# Process-level cache: load the (large) model once per batch worker.
_CLASSIFIER_CACHE: dict[tuple[str, str], FullVocabClassifier] = {}


def get_classifier(
    model_path: str | None = None, anc2vec_path: str | None = None
) -> FullVocabClassifier:
    """Return a cached :class:`FullVocabClassifier`, loading it on first use."""
    key = (
        model_path or os.environ.get(_MODEL_ENV, _DEFAULT_MODEL_PATH),
        anc2vec_path or os.environ.get(_ANC2VEC_ENV, _DEFAULT_ANC2VEC_PATH),
    )
    cached = _CLASSIFIER_CACHE.get(key)
    if cached is None:
        cached = FullVocabClassifier(model_path, anc2vec_path)
        _CLASSIFIER_CACHE[key] = cached
    return cached


def _load_one_plm(session: object, config_id: str, accessions: list[str]) -> dict[str, np.ndarray]:
    """``{accession: vector}`` for one embedding config (query-protein path)."""
    from protea.infrastructure.orm.models.embedding.sequence_embedding import (  # noqa: PLC0415
        SequenceEmbedding,
    )
    from protea.infrastructure.orm.models.protein.protein import Protein  # noqa: PLC0415

    rows = (
        session.query(Protein.accession, SequenceEmbedding.embedding)  # type: ignore[attr-defined]
        .join(Protein.sequence)
        .join(
            SequenceEmbedding,
            (SequenceEmbedding.sequence_id == Protein.sequence_id)
            & (SequenceEmbedding.embedding_config_id == uuid.UUID(config_id)),
        )
        .filter(Protein.accession.in_(accessions))
        .all()
    )
    return {acc: np.asarray(emb.to_list(), dtype=np.float32) for acc, emb in rows}


def load_concat_features(
    session: object, accessions: list[str]
) -> tuple[np.ndarray, list[str]]:
    """Build the 8320-d 6-PLM concat per accession in :data:`PLM_CONCAT_ORDER`.

    Only accessions that have ALL six embeddings (right dims) are returned, so
    the classifier never sees a zero-padded partial vector. Returns
    ``(features, valid_accessions)`` aligned row-for-row.
    """
    maps = [(_load_one_plm(session, cid, accessions), dim) for _, cid, dim in PLM_CONCAT_ORDER]
    features: list[np.ndarray] = []
    valid: list[str] = []
    for acc in accessions:
        parts = []
        for emb, dim in maps:
            vec = emb.get(acc)
            if vec is None or vec.shape[0] != dim:
                break
            parts.append(vec)
        else:
            features.append(np.concatenate(parts))
            valid.append(acc)
    if not features:
        return np.empty((0, CLASSIFIER_INPUT_DIM), dtype=np.float32), []
    return np.vstack(features).astype(np.float32), valid


def resolve_go_term_ids(
    session: object, go_ids: set[str], snapshot_id: uuid.UUID
) -> dict[str, int]:
    """Map ``go_id`` strings to ``GOTerm.id`` ints for the given snapshot."""
    from sqlalchemy import select  # noqa: PLC0415

    from protea.infrastructure.orm.models.annotation.go_term import GOTerm  # noqa: PLC0415

    if not go_ids:
        return {}
    rows = session.execute(  # type: ignore[attr-defined]
        select(GOTerm.go_id, GOTerm.id).where(
            GOTerm.go_id.in_(go_ids),
            GOTerm.ontology_snapshot_id == snapshot_id,
        )
    ).all()
    return {go_id: gid for go_id, gid in rows}
