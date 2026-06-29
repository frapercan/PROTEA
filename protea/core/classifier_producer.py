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

import glob
import hashlib
import json
import logging
import os
import re
import sqlite3
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import torch
from torch import nn

if TYPE_CHECKING:
    from collections.abc import Iterable

    from protea.core.two_tower_classifier import TwoTowerSparseClassifier

_LOG = logging.getLogger(__name__)

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

# Seed-averaging (INT-7). When either of these is set, ``get_classifier``
# loads N self-contained seed checkpoints and averages their TOP-K outputs
# with the lab's consensus rule (see :class:`SeedAveragedClassifier`).
#  - ``PROTEA_CLASSIFIER_SEED_DIR``: a directory; every ``*.pt`` inside is a
#    seed checkpoint (sorted by filename for determinism).
#  - ``PROTEA_CLASSIFIER_SEED_PATHS``: an explicit ``os.pathsep``-separated
#    list (or a single glob) of checkpoint paths; takes precedence over the
#    directory env when both are set.
_SEED_DIR_ENV = "PROTEA_CLASSIFIER_SEED_DIR"
_SEED_PATHS_ENV = "PROTEA_CLASSIFIER_SEED_PATHS"

# Classifier implementation selector (iteration ``sparse-classifier``). The
# default (unset / ``m2``) keeps the production M2 ``hybrid_anc2vec`` head; set
# ``PROTEA_CLASSIFIER_IMPL=two_tower_sparse`` to serve / export the opt-in
# two-tower sparse functional candidate generator instead. Both share this
# module's ``get_classifier`` entry point and the ``ClassifierPrediction``
# contract, so ``apply_classifier`` and the export union path are unchanged.
_IMPL_ENV = "PROTEA_CLASSIFIER_IMPL"
_TWO_TOWER_IMPL = "two_tower_sparse"


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
        # The resolved checkpoint paths identify the model weights for the
        # export classifier-output cache key (P2). A single-checkpoint model
        # is identified by its one path.
        self.checkpoint_paths: tuple[str, ...] = (model_path,)
        ckpt = torch.load(model_path, map_location="cpu", weights_only=False)
        self.vocab: list[str] = [str(t) for t in ckpt["vocab"]]
        self.in_dim = int(ckpt["in_dim"])
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.mu = torch.as_tensor(np.asarray(ckpt["mu"]), dtype=torch.float32).to(self.device)
        self.sd = torch.as_tensor(np.asarray(ckpt["sd"]), dtype=torch.float32).to(self.device)
        label_matrix = _label_matrix_for_vocab(self.vocab, anc2vec_path, int(ckpt["label_dim"]))
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


def resolve_seed_paths(seed_dir: str | None = None, seed_paths: str | None = None) -> list[str]:
    """Return the sorted seed-checkpoint paths, or ``[]`` when none configured.

    ``PROTEA_CLASSIFIER_SEED_PATHS`` (an ``os.pathsep``-separated list, or a
    single glob expanded with :func:`glob.glob`) takes precedence over
    ``PROTEA_CLASSIFIER_SEED_DIR`` (every ``*.pt`` in the directory). The
    result is ALWAYS sorted so seed-averaging is order-independent and
    deterministic across processes.
    """
    raw_paths = seed_paths if seed_paths is not None else os.environ.get(_SEED_PATHS_ENV)
    if raw_paths:
        parts = [s for s in raw_paths.split(os.pathsep) if s]
        expanded: list[str] = []
        for part in parts:
            hits = glob.glob(part)
            expanded.extend(hits if hits else [part])
        return sorted(expanded)
    raw_dir = seed_dir if seed_dir is not None else os.environ.get(_SEED_DIR_ENV)
    if raw_dir:
        return sorted(glob.glob(os.path.join(raw_dir, "*.pt")))
    return []


class SeedAveragedClassifier:
    """N independently-trained seed checkpoints, output-averaged for inference.

    Seed-averaging the M2 anc2vec classifier (the champion's
    ``classifier = "M2 anc2vec seed-avg (7 seeds ..., consensus union top-100,
    score=sum/n)"``) is the anti-winner's-curse lever that lifts the head
    above any single seed. The averaging is done on the OUTPUT SCORES, never
    on the weights (the net is nonlinear, so weight-averaging is invalid).

    The contract is a faithful port of
    ``protea-reranker-lab/fullgo/seed_average.py``: each seed proposes its own
    top-K terms per protein (``FullVocabClassifier.predict`` already applies
    ``argsort -> top_n -> min_score`` exactly as the lab's
    ``train_classifier_m2.py`` did). The consensus is the UNION of every
    seed's ``(protein, term)`` pairs; the score of a pair is the SUM of that
    term's scores across the seeds where it surfaced, divided by the TOTAL
    number of seeds ``n`` (a seed that did not surface the pair contributes
    ``0``). So a term present in only 1 of ``n`` seeds gets ``its_score / n``.

    All seeds share the one anc2vec label matrix and the same vocab, so the
    rule is well defined on a single index space.
    """

    def __init__(self, seed_paths: list[str], anc2vec_path: str | None = None) -> None:
        if not seed_paths:
            raise ValueError("SeedAveragedClassifier requires at least one seed path")
        self.seed_paths = list(seed_paths)
        # The full ordered seed set identifies the averaged model for the
        # export classifier-output cache key (P2).
        self.checkpoint_paths: tuple[str, ...] = tuple(self.seed_paths)
        self.n_seeds = len(self.seed_paths)
        self.seeds = [FullVocabClassifier(path, anc2vec_path) for path in self.seed_paths]
        self.vocab = self.seeds[0].vocab

    def predict(
        self, features: np.ndarray, accessions: list[str], top_n: int = 100, min_score: float = 0.01
    ) -> list[ClassifierPrediction]:
        """Union-top-K consensus across the seeds; score = sum-present / n_seeds.

        Each seed contributes its own top-``top_n`` terms (above ``min_score``)
        per protein; the union is scored with the lab's ``sum / n`` rule.
        Deterministic: seed order is fixed at construction, the accumulation is
        commutative, and the output is sorted by ``(accession, go_id)``.
        """
        accumulator: dict[tuple[str, str], float] = defaultdict(float)
        for seed in self.seeds:
            for pr in seed.predict(features, accessions, top_n=top_n, min_score=min_score):
                accumulator[(pr.accession, pr.go_id)] += pr.score
        rows = [
            ClassifierPrediction(acc, go_id, total / self.n_seeds)
            for (acc, go_id), total in accumulator.items()
        ]
        rows.sort(key=lambda r: (r.accession, r.go_id))
        return rows


def classifier_impl() -> str:
    """Selected classifier implementation (``m2`` default, env-overridable)."""
    return (os.environ.get(_IMPL_ENV) or "m2").strip().lower()


def load_classifier_features(
    session: object, accessions: list[str]
) -> tuple[np.ndarray, list[str]]:
    """Load the feature matrix for the SELECTED classifier implementation.

    M2 consumes the 8320-d 6-PLM concat (:func:`load_concat_features`); the
    two-tower sparse head consumes the 2048-d learned-champion codes
    (``load_two_tower_features``). Both return ``(features, valid_accessions)``
    aligned row-for-row, so the predict / export callers stay implementation
    agnostic.
    """
    if classifier_impl() == _TWO_TOWER_IMPL:
        from protea.core.two_tower_classifier import (  # noqa: PLC0415
            load_two_tower_features,
        )

        return load_two_tower_features(session, accessions)
    return load_concat_features(session, accessions)


# Process-level cache: load the (large) model once per batch worker.
_CLASSIFIER_CACHE: dict[tuple[str, str], FullVocabClassifier] = {}
_SEED_CLASSIFIER_CACHE: dict[tuple[tuple[str, ...], str], SeedAveragedClassifier] = {}


def get_classifier(
    model_path: str | None = None, anc2vec_path: str | None = None
) -> FullVocabClassifier | SeedAveragedClassifier | TwoTowerSparseClassifier:
    """Return a cached classifier, seed-averaged when a seed env is configured.

    When ``PROTEA_CLASSIFIER_SEED_DIR`` / ``PROTEA_CLASSIFIER_SEED_PATHS``
    resolve to one-or-more checkpoints, a cached
    :class:`SeedAveragedClassifier` is returned (the champion path). Otherwise
    behaviour is byte-identical to the historical single-checkpoint
    :class:`FullVocabClassifier`. An explicit ``model_path`` argument always
    forces the single-checkpoint path (it is how unit tests pin one file).

    When ``PROTEA_CLASSIFIER_IMPL=two_tower_sparse`` is set (and no explicit
    ``model_path`` pins the M2 single-checkpoint path), the opt-in two-tower
    sparse functional candidate generator is returned instead.
    """
    if model_path is None and classifier_impl() == _TWO_TOWER_IMPL:
        from protea.core.two_tower_classifier import (  # noqa: PLC0415
            get_two_tower_classifier,
        )

        return get_two_tower_classifier()
    anc = anc2vec_path or os.environ.get(_ANC2VEC_ENV, _DEFAULT_ANC2VEC_PATH)
    seed_paths = [] if model_path else resolve_seed_paths()
    if seed_paths:
        key = (tuple(seed_paths), anc)
        cached_seed = _SEED_CLASSIFIER_CACHE.get(key)
        if cached_seed is None:
            cached_seed = SeedAveragedClassifier(seed_paths, anc2vec_path)
            _SEED_CLASSIFIER_CACHE[key] = cached_seed
        return cached_seed
    single_key = (model_path or os.environ.get(_MODEL_ENV, _DEFAULT_MODEL_PATH), anc)
    cached = _CLASSIFIER_CACHE.get(single_key)
    if cached is None:
        cached = FullVocabClassifier(model_path, anc2vec_path)
        _CLASSIFIER_CACHE[single_key] = cached
    return cached


# Validated UniProt-accession shape. Accessions reach the loader as
# ground-truth / query-set strings, but they are inlined verbatim into a COPY
# SELECT (which cannot take bound params, exactly like the cooccurrence loader
# in ``_association_loader._load_into_cache``), so each is re-validated against
# this pattern before inlining to guarantee no SQL-injection surface. The shape
# covers canonical accessions, isoforms (``<canonical>-<n>``) and the synthetic
# accessions unit tests use (``Q1``); an unexpected shape is a programming error
# and raises rather than silently inlining attacker-controlled text.
_ACCESSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]*$")


def _quote_accession_list(accessions: list[str]) -> str:
    """Validate + SQL-quote accessions into a ``'a', 'b'`` IN-list literal.

    Inlines the accession filter into the COPY (SELECT ...) TO STDOUT text. Each
    accession is re-validated against :data:`_ACCESSION_RE` and the quote escaped
    defensively before inlining. (The cooccurrence loader in
    ``_association_loader`` instead binds its go_id filter as a ``= ANY(%s)``
    array parameter; psycopg3 ``cursor.copy`` accepts bound params, so a future
    cleanup could move this loader to the same parameterized form.)
    """
    quoted: list[str] = []
    for acc in accessions:
        if not _ACCESSION_RE.match(acc):
            raise ValueError(f"refusing to inline non-accession literal: {acc!r}")
        quoted.append("'" + acc.replace("'", "''") + "'")
    return ", ".join(quoted)


def _load_one_plm(session: object, config_id: str, accessions: list[str]) -> dict[str, np.ndarray]:
    """``{accession: vector}`` for one embedding config (query-protein path).

    Reads ``(protein.accession, sequence_embedding.embedding)`` for the given
    config with a raw psycopg3 ``COPY (SELECT ...) TO STDOUT`` on the live DBAPI
    connection rather than an ORM ``.all()`` + per-row ``emb.to_list()``. The
    Row-object materialization plus ``HalfVector`` decode under the ORM path was
    the dominant export build-records cost (~22% of profiled samples, the same
    pattern #660 replaced for the cooccurrence loader); COPY streams the halfvec
    TEXT literal and parses it straight into ``np.float32``.

    Bit-exact parity with the prior ``.to_list()`` path: pgvector emits each
    ``halfvec`` element as the shortest decimal that round-trips its stored
    half-precision value, and ``np.fromstring(..., dtype=np.float32)`` recovers
    exactly that float (a float16 value is exactly representable in float32), so
    the returned vectors are byte-identical to the ORM load for any input.

    The original ORM query joined ``Protein.sequence`` then ``SequenceEmbedding``
    on ``sequence_id == Protein.sequence_id``; both legs key on the same
    ``sequence_id``, so a protein with a NULL ``sequence_id`` matched nothing
    either way. The COPY join (``sequence_embedding.sequence_id =
    protein.sequence_id``) is therefore the same row set.
    """
    if not accessions:
        return {}
    raw = session.connection().connection  # type: ignore[attr-defined]  # psycopg3 DBAPI conn
    in_list = _quote_accession_list(accessions)
    config_literal = str(uuid.UUID(config_id))
    sql = (
        "COPY (SELECT protein.accession, sequence_embedding.embedding "
        "FROM protein "
        "JOIN sequence_embedding "
        "ON sequence_embedding.sequence_id = protein.sequence_id "
        f"AND sequence_embedding.embedding_config_id = '{config_literal}' "
        f"WHERE protein.accession IN ({in_list})"
        ") TO STDOUT"
    )
    out: dict[str, np.ndarray] = {}
    with raw.cursor() as cur, cur.copy(sql) as cp:
        for acc, emb_txt in cp.rows():
            # halfvec TEXT literal is ``[a,b,c,...]``; strip the brackets and
            # parse the comma-separated decimals straight into float32.
            out[acc] = np.fromstring(emb_txt[1:-1], sep=",", dtype=np.float32)
    return out


def load_concat_features(session: object, accessions: list[str]) -> tuple[np.ndarray, list[str]]:
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


# ---------------------------------------------------------------------------
# Export classifier-output cache (P2).
#
# The classifier output for a protein depends on the protein's frozen 6-PLM
# concat (the same :data:`PLM_CONCAT_ORDER` embeddings) and the classifier
# checkpoint. The protein tower and the learned head are t0-INDEPENDENT; the one
# t0-dependent part is the two-tower's frozen GO CO-ANNOTATION codes, which the
# EXPORT path may resolve PER training snapshot pair from its own t0 (see
# ``two_tower_classifier.resolve_per_cut_go_codes_path``), mirroring the per-cut
# ``association`` feature so an earlier pair never sees a later cut's
# co-annotation. The M2 head and the single-artifact two-tower (no per-cut dir
# configured) remain fully t0-independent.
#
# The export iterates the 13 train snapshot pairs (+ the test pair) and the same
# proteins recur heavily across consecutive pairs, so recomputing the classifier
# per pair wastes most of the classifier GPU compute. This cache memoises the
# classifier's top-100 output per protein, keyed by ``(accession,
# plm_concat_signature, classifier_checkpoint_sha, go_codes_sha)`` so it is
# correctly invalidated when the embedding configs, the checkpoint, OR the
# per-cut GO codes change. ``go_codes_sha`` is empty (and the key is byte-for-
# byte today's) for the M2 head and for a two-tower without a per-cut dir.
# Subsequent pairs (and subsequent queries within a pair) hit the cache.
#
# This is an EXPORT-RUN-scoped optimisation. The live predict path
# (``apply_classifier``) stays per-request and does NOT use this cache.
# ---------------------------------------------------------------------------

# Signature of the frozen 6-PLM concat identity: the ordered ``(plm_key,
# embedding_config_id, dim)`` tuples. If any embedding config id (or the concat
# order / dims) changes, the signature changes and old cache rows miss.
_PLM_CONCAT_SIGNATURE = hashlib.sha256(
    json.dumps([[k, cid, dim] for k, cid, dim in PLM_CONCAT_ORDER], sort_keys=True).encode()
).hexdigest()[:16]

# Process-level memo of checkpoint-file content shas, keyed by absolute path so
# the (potentially large) file is hashed at most once per process.
_CHECKPOINT_SHA_CACHE: dict[str, str] = {}

# Process-level classifier-output cache: ``cache_key -> [(go_id, score), ...]``.
_CLF_OUTPUT_CACHE: dict[str, list[tuple[str, float]]] = {}

_CLF_CACHE_DIR_ENV = "PROTEA_CLASSIFIER_CACHE_DIR"


def _checkpoint_sha(paths: Iterable[str]) -> str:
    """Content sha256 over the ordered classifier checkpoint files.

    Memoised per path so a multi-seed model hashes each seed file at most once
    per process. A missing path falls back to its string (so tests pinning a
    synthetic in-memory model still get a stable, distinct key).
    """
    parts: list[str] = []
    for path in paths:
        cached = _CHECKPOINT_SHA_CACHE.get(path)
        if cached is None:
            try:
                h = hashlib.sha256()
                with open(path, "rb") as fh:
                    for block in iter(lambda: fh.read(1 << 20), b""):
                        h.update(block)
                cached = h.hexdigest()
            except OSError:
                cached = hashlib.sha256(path.encode()).hexdigest()
            _CHECKPOINT_SHA_CACHE[path] = cached
        parts.append(cached)
    return hashlib.sha256("\x00".join(parts).encode()).hexdigest()[:16]


def _clf_cache_key(accession: str, checkpoint_sha: str, go_codes_sha: str = "") -> str:
    """Cache key for one protein's classifier output.

    ``go_codes_sha`` identifies the per-cut two-tower GO co-annotation codes used
    for this cut so distinct cuts do not collide. It is empty for the M2 head and
    for a single-artifact two-tower, keeping those keys byte-identical to before.
    """
    suffix = f"\x1f{go_codes_sha}" if go_codes_sha else ""
    return f"{accession}\x1f{_PLM_CONCAT_SIGNATURE}\x1f{checkpoint_sha}{suffix}"


def _resolve_clf_cache_dir() -> Path | None:
    """On-disk classifier-output cache directory, or ``None`` to disable.

    Mirrors the alignment cache pattern (``_pair_feature_compute``).
    ``PROTEA_CLASSIFIER_CACHE_DIR`` overrides; set it empty to disable on-disk
    persistence. The default is OFF (``None``) so the process-level memo is the
    safe minimum and no stray sqlite file appears unless opted in.
    """
    raw = os.environ.get(_CLF_CACHE_DIR_ENV)
    if raw is None:
        return None
    return Path(raw) if raw.strip() else None


class _ClassifierOutputDiskCache:
    """Thin sqlite-backed ``key -> json [[go_id, score], ...]`` store."""

    def __init__(self, cache_dir: Path) -> None:
        cache_dir.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(cache_dir / "classifier_output.sqlite"))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("CREATE TABLE IF NOT EXISTS clf (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
        self._conn.commit()

    def get_many(self, keys: list[str]) -> dict[str, list[tuple[str, float]]]:
        out: dict[str, list[tuple[str, float]]] = {}
        for i in range(0, len(keys), 800):
            chunk = keys[i : i + 800]
            qs = ",".join("?" * len(chunk))
            rows = self._conn.execute(
                f"SELECT k, v FROM clf WHERE k IN ({qs})",  # noqa: S608
                chunk,
            ).fetchall()
            for k, v in rows:
                out[k] = [(str(go), float(sc)) for go, sc in json.loads(v)]
        return out

    def put_many(self, items: list[tuple[str, list[tuple[str, float]]]]) -> None:
        if not items:
            return
        self._conn.executemany(
            "INSERT OR IGNORE INTO clf (k, v) VALUES (?, ?)",
            [(k, json.dumps([[go, sc] for go, sc in v])) for k, v in items],
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


def reset_classifier_output_cache() -> None:
    """Clear the process-level classifier-output memo (test hook)."""
    _CLF_OUTPUT_CACHE.clear()


def predict_proteins_cached(
    session: object,
    accessions: list[str],
    classifier: (
        FullVocabClassifier | SeedAveragedClassifier | TwoTowerSparseClassifier | None
    ) = None,
    go_codes_path: str | None = None,
) -> list[ClassifierPrediction]:
    """Classifier top-100 per protein, memoised across snapshot pairs (P2).

    Returns the SAME rows ``get_classifier().predict(load_concat_features(...))``
    would (same ``(accession, go_id, score)`` per protein), but the per-protein
    classifier compute runs at most once per export run: the output is cached by
    ``(accession, plm_concat_signature, classifier_checkpoint_sha, go_codes_sha)``
    so later snapshot pairs reuse it. Only the proteins missing from the cache go
    through ``load_concat_features`` + ``predict``; cache hits skip both the
    embedding load and the GPU forward pass.

    ``classifier`` defaults to the env-selected :func:`get_classifier` (the
    seed-averaged champion when configured), matching the export's inline
    applier. ``go_codes_path`` (EXPORT path only) pins this cut's per-cut
    two-tower GO co-annotation codes: when given AND the two-tower impl is
    selected, the classifier is built from that artifact and the codes identity
    enters the cache key so distinct cuts do not collide. ``None`` keeps today's
    single-artifact behaviour, and the codes identity stays empty for the M2
    head, so its keys are unchanged. An on-disk sqlite tier is engaged when
    ``PROTEA_CLASSIFIER_CACHE_DIR`` is set (mirrors the alignment cache), so the
    output survives across export runs / processes.
    """
    use_per_cut = (
        classifier is None and go_codes_path is not None and classifier_impl() == _TWO_TOWER_IMPL
    )
    go_codes_sha = ""
    if classifier is not None:
        clf: FullVocabClassifier | SeedAveragedClassifier | TwoTowerSparseClassifier = classifier
    elif use_per_cut and go_codes_path is not None:
        from protea.core.two_tower_classifier import get_two_tower_classifier  # noqa: PLC0415

        clf = get_two_tower_classifier(go_codes_path=go_codes_path)
        go_codes_sha = _checkpoint_sha([go_codes_path])
    else:
        clf = get_classifier()
    checkpoint_sha = _checkpoint_sha(clf.checkpoint_paths)
    # De-dup while preserving order so a protein appearing twice is scored once.
    wanted = [a for a in dict.fromkeys(accessions) if a]
    key_by_acc = {a: _clf_cache_key(a, checkpoint_sha, go_codes_sha) for a in wanted}
    cached = _resolve_classifier_cache(session, clf, wanted, key_by_acc)

    out: list[ClassifierPrediction] = []
    for acc in wanted:
        for go_id, score in cached.get(acc, []):
            out.append(ClassifierPrediction(acc, go_id, score))
    return out


def _resolve_classifier_cache(
    session: object,
    clf: FullVocabClassifier | SeedAveragedClassifier | TwoTowerSparseClassifier,
    wanted: list[str],
    key_by_acc: dict[str, str],
) -> dict[str, list[tuple[str, float]]]:
    """Return ``{acc: [(go_id, score)]}`` from the memo+disk cache, computing
    any misses through the classifier (and storing them back into both tiers)."""
    cached: dict[str, list[tuple[str, float]]] = {}
    misses: list[str] = []
    for acc in wanted:
        memo = _CLF_OUTPUT_CACHE.get(key_by_acc[acc])
        if memo is not None:
            cached[acc] = memo
        else:
            misses.append(acc)

    disk = _open_disk_cache()
    try:
        if disk is not None and misses:
            hits = disk.get_many([key_by_acc[a] for a in misses])
            still_missing: list[str] = []
            for acc in misses:
                rows = hits.get(key_by_acc[acc])
                if rows is not None:
                    cached[acc] = rows
                    _CLF_OUTPUT_CACHE[key_by_acc[acc]] = rows
                else:
                    still_missing.append(acc)
            misses = still_missing
        if misses:
            _compute_and_store_misses(session, clf, misses, key_by_acc, cached, disk)
    finally:
        if disk is not None:
            disk.close()
    return cached


def _open_disk_cache() -> _ClassifierOutputDiskCache | None:
    """Open the on-disk classifier-output cache when configured, else ``None``."""
    cache_dir = _resolve_clf_cache_dir()
    if cache_dir is None:
        return None
    try:
        return _ClassifierOutputDiskCache(cache_dir)
    except sqlite3.Error as exc:  # pragma: no cover - defensive
        _LOG.warning("classifier-output disk cache disabled (%s)", exc)
        return None


def _compute_and_store_misses(
    session: object,
    clf: FullVocabClassifier | SeedAveragedClassifier | TwoTowerSparseClassifier,
    misses: list[str],
    key_by_acc: dict[str, str],
    cached: dict[str, list[tuple[str, float]]],
    disk: _ClassifierOutputDiskCache | None,
) -> None:
    """Compute the classifier output for the cache MISSES and populate caches.

    A protein with all six PLM embeddings present gets its real top-100; a
    protein dropped by :func:`load_concat_features` (missing a PLM) is cached as
    an EMPTY output so repeated pairs do not re-attempt the (failing) load.
    """
    features, valid = load_classifier_features(session, misses)
    by_acc: dict[str, list[tuple[str, float]]] = {a: [] for a in misses}
    if valid:
        for pr in clf.predict(features, valid):
            by_acc[pr.accession].append((pr.go_id, pr.score))
    to_disk: list[tuple[str, list[tuple[str, float]]]] = []
    for acc in misses:
        rows = by_acc[acc]
        _CLF_OUTPUT_CACHE[key_by_acc[acc]] = rows
        cached[acc] = rows
        to_disk.append((key_by_acc[acc], rows))
    if disk is not None:
        disk.put_many(to_disk)
