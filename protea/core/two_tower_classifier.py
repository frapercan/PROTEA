"""Two-tower sparse functional GO candidate generator (iteration ``sparse-classifier``).

OPT-IN replacement for the M2 ``hybrid_anc2vec`` full-vocabulary classifier
(:mod:`protea.core.classifier_producer`). It is selected at serve / export time
by ``PROTEA_CLASSIFIER_IMPL=two_tower_sparse``; the default
(``m2`` / unset) leaves the production classifier UNCHANGED.

Architecture (ported faithfully from
``protea-reranker-lab/results/sparse_classifier/p2/two_tower.py`` ``ProjHead``
and ``p2/generate.py`` ``generate_top100``), two frozen towers plus one learned
head:

* PROTEIN tower input: the learned-champion ``d8979601`` k-WTA codes (2048-d),
  already materialised in the platform as a ``SequenceEmbedding`` for that
  embedding config. The serve / export path loads it exactly like any other
  embedding config, so NO re-embedding is required.
* GO tower (frozen): the sparse functional GO codes (1024-d) shipped in
  ``go_sparse_codes.npz`` (whitened-BioBERT text k-WTA + t0 co-annotation
  PPMI/SVD k-WTA). A vocabulary file pins the trained term ORDER so the head's
  per-term bias aligns. The co-annotation block is the only t0-dependent part:
  for the EXPORT path it can be resolved PER training snapshot pair from
  ``PROTEA_TWO_TOWER_GO_CODES_DIR`` (see :func:`resolve_per_cut_go_codes_path`),
  mirroring the per-cut ``association`` feature so an earlier pair never sees a
  later cut's co-annotation. The protein tower and the learned head stay frozen
  and t0-independent; only this GO co-annotation matrix varies.
* learned head (``ProjHead``): projects the 2048-d protein code into the 1024-d
  GO-code space, scores it against every GO code, adds a per-term bias and a
  temperature.

``score(protein, term) = mean_seeds( temp * <proj(protein_code), go_code[term]> + bias[term] )``

and the top-100 terms per protein are the candidate-generator output. The 7
seed heads are output-averaged (the lab ensemble), never weight-averaged.

The class mirrors the :class:`~protea.core.classifier_producer.SeedAveragedClassifier`
interface verbatim, so :func:`~protea.core.classifier_producer.get_classifier`,
``apply_classifier`` and the export classifier-union path can consume it without
any other change:

* ``.predict(features, accessions, top_n=100, min_score=...) -> list[ClassifierPrediction]``
* ``.vocab: list[str]``
* ``.checkpoint_paths: tuple[str, ...]`` (drives the export output cache key)

Unlike the M2 head, the two-tower scores are RAW retrieval logits (not sigmoid
probabilities in ``[0, 1]``), so ``min_score`` is intentionally NOT applied:
the generator always emits the top-``top_n`` terms, exactly as the validated
``generate_top100`` did. Downstream the reranker consumes ``classifier_score``
as a feature regardless of its magnitude.

Validated (overnight 2026-06-28, temporal 227 to 230 recall ceiling uplift over
a KNN-only pool, biggest on the lose cells): PK-bpo +0.068, PK-mfo +0.057,
LK-bpo +0.056, PK-cco +0.055, NK-bpo +0.041; all nine NK/LK/PK x aspect cells up.

No live DB and no lab import: the head ckpts, the GO codes and the vocab are
loaded from the artifact-store paths resolved from env (registered by the
conductor), never from the lab tree.
"""

from __future__ import annotations

import glob
import logging
import os
from typing import TYPE_CHECKING

import numpy as np
import torch
from torch import nn

from protea.core.classifier_producer import ClassifierPrediction

if TYPE_CHECKING:
    pass

_LOG = logging.getLogger(__name__)

# The protein tower input: the learned-champion encoder embedding config. The
# 2048-d k-WTA codes are already stored as a ``SequenceEmbedding`` for this
# config, so the serve / export path loads them like any other embedding.
TWO_TOWER_INPUT_CONFIG_ID = "d8979601-ea59-4de1-9c16-21036ed67c36"
TWO_TOWER_INPUT_DIM = 2048
TWO_TOWER_GO_DIM = 1024

# Artifact locations (registered by the conductor in the artifact store). Each
# is overridable via env so the dev box, the container and CI point at their
# own copies; there is NO hard-coded lab path.
_SEED_DIR_ENV = "PROTEA_TWO_TOWER_SEED_DIR"  # dir of head_seed*.pt (7 ckpts)
_GO_CODES_ENV = "PROTEA_TWO_TOWER_GO_CODES"  # go_sparse_codes.npz (go_ids + codes)
_VOCAB_ENV = "PROTEA_TWO_TOWER_VOCAB"  # vocab_go.npy/.json/.npz: trained term ORDER
_CONFIG_ID_ENV = "PROTEA_TWO_TOWER_CONFIG_ID"  # protein-tower embedding config id
# EXPORT-ONLY per-cut GO co-annotation codes. A directory of artifacts named
# ``go_sparse_codes_v{source_version}.npz``, one per t0 ``AnnotationSet``
# ``source_version``. When set, the export resolves each training snapshot pair's
# GO codes from THAT pair's own t0 (mirroring the per-cut ``association``
# feature), instead of reusing the single ``_GO_CODES_ENV`` artifact for every
# cut (which would leak the latest co-annotation into earlier pairs). Unset (or a
# missing per-cut file) falls back to ``_GO_CODES_ENV`` (today's behaviour). The
# LIVE predict path never reads this: it always serves the single latest codes.
_GO_CODES_DIR_ENV = "PROTEA_TWO_TOWER_GO_CODES_DIR"
# Per-cut codes filename convention: ``go_sparse_codes_v{source_version}.npz``.
_GO_CODES_FILENAME = "go_sparse_codes_v{source_version}.npz"


class ProjHead(nn.Module):
    """Protein-code to GO-code-space projection (faithful port of the lab module).

    ``score = temp * <proj(protein_code), go_code> + bias`` where ``bias`` is a
    per-term learned frequency prior (constant across proteins; it only
    recalibrates the per-term threshold, it does not break the retrieval
    geometry). The trunk is a 1-hidden GELU MLP (the trained config) or a bare
    linear; an optional query k-WTA matches the lab.
    """

    def __init__(
        self,
        d_in: int = 2048,
        d_out: int = 1024,
        vocab_size: int = 0,
        hidden: int = 0,
        kwta: int = 0,
    ) -> None:
        super().__init__()
        self.kwta = kwta
        if hidden and hidden > 0:
            self.net: nn.Module = nn.Sequential(
                nn.Linear(d_in, hidden),
                nn.GELU(),
                nn.Identity(),
                nn.Linear(hidden, d_out),
            )
        else:
            self.net = nn.Linear(d_in, d_out)
        self.bias = nn.Parameter(torch.zeros(vocab_size)) if vocab_size else None
        self.log_temp = nn.Parameter(torch.zeros(()))

    def project(self, x: torch.Tensor) -> torch.Tensor:
        z = self.net(x)
        if self.kwta and self.kwta > 0:
            thresh = z.abs().topk(self.kwta, dim=1).values[:, -1:].detach()
            z = z * (z.abs() >= thresh)
        return z

    def forward(self, x: torch.Tensor, go_t: torch.Tensor) -> torch.Tensor:
        logits = self.log_temp.exp() * (self.project(x) @ go_t)
        if self.bias is not None:
            logits = logits + self.bias
        return logits


def _load_vocab(vocab_path: str) -> list[str]:
    """Load the trained GO term ORDER (the head bias aligns to this order)."""
    if vocab_path.endswith(".json"):
        import json  # noqa: PLC0415

        with open(vocab_path) as fh:
            return [str(t) for t in json.load(fh)]
    arr = np.load(vocab_path, allow_pickle=True)
    # ``.npz`` ships the order under ``vocab_go``; ``.npy`` is the bare array.
    terms = arr["vocab_go"] if hasattr(arr, "files") else arr
    return [str(t) for t in terms]


def _build_go_matrix(go_codes_path: str, vocab: list[str]) -> np.ndarray:
    """Frozen GO-code matrix (V x 1024) row-aligned to ``vocab`` (trained order).

    ``go_sparse_codes.npz`` carries the FULL GO vocabulary (``go_ids`` +
    ``codes``); the trained head only scores the ``vocab`` subset and its bias
    is indexed in ``vocab`` order, so the matrix is selected and reordered to
    match. A trained term absent from the codes file is a registration error.
    """
    arr = np.load(go_codes_path, allow_pickle=True)
    go_ids = [str(g) for g in arr["go_ids"]]
    codes = np.asarray(arr["codes"], dtype=np.float32)
    index = {g: i for i, g in enumerate(go_ids)}
    missing = [t for t in vocab if t not in index]
    if missing:
        raise ValueError(
            f"{len(missing)} trained GO terms missing from {go_codes_path} "
            f"(first: {missing[:3]}); the vocab and GO-codes artifacts are mismatched"
        )
    rows = np.fromiter((index[t] for t in vocab), dtype=np.int64, count=len(vocab))
    return codes[rows].astype(np.float32)


class TwoTowerSparseClassifier:
    """7-seed two-tower sparse functional candidate generator (output-averaged).

    Constructed once per worker (cache via :func:`get_two_tower_classifier`),
    NOT per protein. Loads the GO codes + trained vocab once, builds the frozen
    GO matrix, and loads each seed head onto GPU when available else CPU.
    """

    def __init__(
        self,
        seed_dir: str | None = None,
        go_codes_path: str | None = None,
        vocab_path: str | None = None,
    ) -> None:
        seed_dir = seed_dir or os.environ.get(_SEED_DIR_ENV)
        go_codes_path = go_codes_path or os.environ.get(_GO_CODES_ENV)
        vocab_path = vocab_path or os.environ.get(_VOCAB_ENV)
        if not (seed_dir and go_codes_path and vocab_path):
            raise ValueError(
                "TwoTowerSparseClassifier requires "
                f"{_SEED_DIR_ENV}, {_GO_CODES_ENV} and {_VOCAB_ENV} to be set"
            )
        seed_paths = sorted(glob.glob(os.path.join(seed_dir, "head_seed*.pt"))) or sorted(
            glob.glob(os.path.join(seed_dir, "*.pt"))
        )
        if not seed_paths:
            raise FileNotFoundError(f"no head_seed*.pt checkpoints in {seed_dir}")
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.vocab = _load_vocab(vocab_path)
        go_matrix = _build_go_matrix(go_codes_path, self.vocab)
        self._go_t = (
            torch.as_tensor(go_matrix, dtype=torch.float32).to(self.device).t().contiguous()
        )
        self.models = [self._load_head(p) for p in seed_paths]
        self.n_seeds = len(self.models)
        # The ordered head set plus the GO-code / vocab artifacts identify the
        # model for the export classifier-output cache key.
        self.checkpoint_paths: tuple[str, ...] = (*seed_paths, go_codes_path, vocab_path)

    def _load_head(self, path: str) -> ProjHead:
        ckpt = torch.load(path, map_location="cpu", weights_only=False)
        cfg = ckpt.get("cfg", {})
        model = ProjHead(
            TWO_TOWER_INPUT_DIM,
            TWO_TOWER_GO_DIM,
            vocab_size=int(ckpt.get("V", len(self.vocab))),
            hidden=int(cfg.get("hidden", 0)),
            kwta=int(cfg.get("kwta", 0)),
        )
        model.load_state_dict(ckpt["state_dict"])
        model.eval()
        return model.to(self.device)

    def predict(
        self,
        features: np.ndarray,
        accessions: list[str],
        top_n: int = 100,
        min_score: float = 0.01,  # noqa: ARG002 - interface parity; see module docstring
    ) -> list[ClassifierPrediction]:
        """Top-``top_n`` (term, score) per protein from the 2048-d code matrix.

        ``features`` is ``(N, 2048)`` learned-champion codes aligned to
        ``accessions``. Scores are the seed-averaged raw retrieval logits; the
        top-``top_n`` terms per protein are returned (``min_score`` is NOT
        applied: the two-tower emits raw logits, not probabilities).
        """
        if features.shape[0] == 0:
            return []
        x_all = torch.as_tensor(np.ascontiguousarray(features), dtype=torch.float32).to(self.device)
        k = min(top_n, len(self.vocab))
        out: list[ClassifierPrediction] = []
        with torch.no_grad():
            for i in range(0, x_all.shape[0], 512):
                x = x_all[i : i + 512]
                scores: torch.Tensor | None = None
                for m in self.models:
                    lg = m(x, self._go_t)
                    scores = lg if scores is None else scores + lg
                assert scores is not None
                scores = scores / self.n_seeds
                vals, idx = scores.topk(k, dim=1)
                out.extend(
                    self._rows_for_block(vals.cpu().numpy(), idx.cpu().numpy(), accessions, i)
                )
        return out

    def _rows_for_block(
        self, vals: np.ndarray, idx: np.ndarray, accessions: list[str], offset: int
    ) -> list[ClassifierPrediction]:
        """Top-k rows for one inference block as :class:`ClassifierPrediction`."""
        rows: list[ClassifierPrediction] = []
        for r in range(vals.shape[0]):
            acc = accessions[offset + r]
            for j in range(vals.shape[1]):
                rows.append(ClassifierPrediction(acc, self.vocab[idx[r, j]], float(vals[r, j])))
        return rows


# Process-level cache: load the heads + GO matrix once per batch worker.
_TWO_TOWER_CACHE: dict[tuple[str, str, str], TwoTowerSparseClassifier] = {}


def get_two_tower_classifier(go_codes_path: str | None = None) -> TwoTowerSparseClassifier:
    """Return the cached env-configured two-tower classifier (one per worker).

    ``go_codes_path`` overrides the single ``_GO_CODES_ENV`` GO codes artifact
    with a per-cut one (resolved by :func:`resolve_per_cut_go_codes_path` on the
    EXPORT path); the cache key includes it so distinct cuts get distinct frozen
    GO matrices and never collide. ``None`` (the default, and every LIVE predict
    call) keeps today's single-artifact behaviour.
    """
    key = (
        os.environ.get(_SEED_DIR_ENV, ""),
        go_codes_path or os.environ.get(_GO_CODES_ENV, ""),
        os.environ.get(_VOCAB_ENV, ""),
    )
    cached = _TWO_TOWER_CACHE.get(key)
    if cached is None:
        cached = TwoTowerSparseClassifier(go_codes_path=go_codes_path)
        _TWO_TOWER_CACHE[key] = cached
    return cached


def resolve_per_cut_go_codes_path(session: object, t0_annotation_set_id: object) -> str | None:
    """Per-cut GO co-annotation codes npz for a training pair's t0, or ``None``.

    EXPORT-ONLY. Resolves the ``source_version`` of ``t0_annotation_set_id`` and
    returns ``{PROTEA_TWO_TOWER_GO_CODES_DIR}/go_sparse_codes_v{source_version}.npz``
    when that dir env is set and the file exists; otherwise ``None`` so the
    caller falls back to the single ``PROTEA_TWO_TOWER_GO_CODES`` artifact
    (today's behaviour). Mirrors the per-cut ``association`` feature: each cut
    reads the co-annotation built at its own t0, never a later cut's.
    """
    codes_dir = os.environ.get(_GO_CODES_DIR_ENV)
    if not codes_dir:
        return None
    source_version = _resolve_source_version(session, t0_annotation_set_id)
    if not source_version:
        _LOG.debug(
            "no source_version for t0=%s; using single %s", t0_annotation_set_id, _GO_CODES_ENV
        )
        return None
    path = os.path.join(codes_dir, _GO_CODES_FILENAME.format(source_version=source_version))
    if not os.path.exists(path):
        _LOG.info("per-cut go codes %s absent; falling back to single %s", path, _GO_CODES_ENV)
        return None
    _LOG.debug("per-cut go codes for t0=%s (v%s) -> %s", t0_annotation_set_id, source_version, path)
    return path


def _resolve_source_version(session: object, annotation_set_id: object) -> str | None:
    """``AnnotationSet.source_version`` for an id (string), or ``None`` if absent."""
    from sqlalchemy import select  # noqa: PLC0415

    from protea.infrastructure.orm.models.annotation.annotation_set import (  # noqa: PLC0415
        AnnotationSet,
    )

    row = session.execute(  # type: ignore[attr-defined]
        select(AnnotationSet.source_version).where(AnnotationSet.id == annotation_set_id)
    ).first()
    if row is None or row[0] is None:
        return None
    return str(row[0])


def two_tower_config_id() -> str:
    """Protein-tower embedding config id (``d8979601`` default, env-overridable)."""
    return os.environ.get(_CONFIG_ID_ENV, TWO_TOWER_INPUT_CONFIG_ID)


def load_two_tower_features(session: object, accessions: list[str]) -> tuple[np.ndarray, list[str]]:
    """Build the ``(N, 2048)`` learned-champion code matrix per accession.

    Loads the protein tower's ``SequenceEmbedding`` for
    :func:`two_tower_config_id` with the same COPY loader the M2 path uses, and
    keeps only accessions whose code has the expected dimension. Returns
    ``(features, valid_accessions)`` aligned row-for-row, matching the
    ``load_concat_features`` contract.
    """
    from protea.core.classifier_producer import _load_one_plm  # noqa: PLC0415

    wanted = [a for a in accessions if a]
    emb = _load_one_plm(session, two_tower_config_id(), wanted)
    features: list[np.ndarray] = []
    valid: list[str] = []
    for acc in wanted:
        vec = emb.get(acc)
        if vec is not None and vec.shape[0] == TWO_TOWER_INPUT_DIM:
            features.append(vec)
            valid.append(acc)
    if not features:
        return np.empty((0, TWO_TOWER_INPUT_DIM), dtype=np.float32), []
    return np.vstack(features).astype(np.float32), valid


def reset_two_tower_cache() -> None:
    """Clear the process-level two-tower cache (test hook)."""
    _TWO_TOWER_CACHE.clear()


__all__ = (
    "TWO_TOWER_INPUT_CONFIG_ID",
    "TWO_TOWER_INPUT_DIM",
    "ProjHead",
    "TwoTowerSparseClassifier",
    "get_two_tower_classifier",
    "load_two_tower_features",
    "reset_two_tower_cache",
    "resolve_per_cut_go_codes_path",
    "two_tower_config_id",
)
