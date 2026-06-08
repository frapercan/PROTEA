"""Universal (single-booster, aspect-conditioned, K-augmented) reranker scoring.

The F-RERANK-UNIVERSAL booster is a *single* LightGBM model trained over a
pooled, aspect-conditioned, K-augmented dataset. Its feature layout differs
from the per-cell boosters the rest of PROTEA scores:

* it DROPS the reserved ``aspect`` column (aspect is a grouping key during
  training/calibration, not a model feature), and
* it ADDS two source-derived constants injected at staging time:
  ``plm_id`` (categorical, vocab-encoded to an int code) and ``k_context``
  (numeric, the K of the KNN context).

Because of that, the generic ``protea_method.reranker.predict`` /
``apply_reranker`` helpers cannot score it correctly: they would coerce the
string categoricals (``qualifier`` / ``evidence_code`` / ``taxonomic_relation``
/ ``plm_id``) to NaN and leave the injected constants unset. This module
reproduces the lab's *authoritative* scoring path
(``protea_reranker_lab.pooled_staging._inject_src_features`` +
``_encode_cat_batch``) bit-for-bit:

* feature order = ``booster.feature_name()`` (the model's own 53-column order);
* ``plm_id`` -> the integer code of the model's plm in the training vocabulary
  (``CAT_MISSING_CODE`` = -1 when the plm is unseen);
* ``k_context`` -> the model's ``k_context`` constant as float32;
* every other categorical -> vocab-encoded int code (-1 for None / unseen);
* every other numeric -> ``pd.to_numeric`` (NaN routed through LightGBM's
  native missing branch);
* raw LambdaRank scores -> sigmoid (monotonic, so ranking + cafaeval metrics
  are unchanged) so callers receive a value in ``(0, 1)``.

Validated 2026-06-08: scoring ``eval.parquet`` for
``bench-v1-K10-v227-lineage-prot_t5`` through this module is bit-identical
(max abs diff 0.0 over 226590 rows) to the lab's
``eval_universal_v227_window._score_eval_direct``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - import only for typing
    import lightgbm as lgb
    import numpy as np
    import pandas as pd

#: Sentinel code for a None / unseen categorical value. Mirrors
#: ``protea_reranker_lab.bucket_io.CAT_MISSING_CODE``; the booster was
#: trained with -1 in that slot, so inference must match.
CAT_MISSING_CODE = -1

#: Constants injected from the source manifest at staging time. They are
#: never read from the prediction rows; their values come from the model's
#: own metadata (``plm_id`` / ``k_context``).
INJECTED_COLUMNS = frozenset({"plm_id", "k_context"})

#: Categorical columns the universal booster carries. ``aspect`` is NOT a
#: feature of the universal model (it is a grouping key), so it is absent
#: here even though the per-cell boosters treat it as categorical.
UNIVERSAL_CATEGORICAL_COLUMNS = (
    "qualifier",
    "evidence_code",
    "taxonomic_relation",
    "plm_id",
)


def is_universal_booster(booster: lgb.Booster) -> bool:
    """True when ``booster`` is a universal (pooled, K-augmented) model.

    Detection key: the universal layout is the only one that carries BOTH
    injected source constants (``plm_id`` and ``k_context``) as features.
    Per-cell boosters carry neither.
    """
    feats = set(booster.feature_name())
    return "plm_id" in feats and "k_context" in feats


def _code_maps(
    categorical_codes: dict[str, list[str]],
) -> dict[str, dict[str, int]]:
    """Build ``{column: {value: code}}`` from the lab's sorted-unique vocab.

    The code of a value is its index in the sorted-unique list, exactly as
    ``protea_reranker_lab.pooled_staging._SrcEncoder`` builds it.
    """
    return {
        col: {val: idx for idx, val in enumerate(vals)} for col, vals in categorical_codes.items()
    }


def score_universal(
    booster: lgb.Booster,
    df: pd.DataFrame,
    *,
    categorical_codes: dict[str, list[str]],
    plm_id: str,
    k_context: float,
) -> np.ndarray:
    """Score ``df`` with the universal ``booster`` reproducing the lab encoding.

    Args:
        booster: the loaded universal LightGBM booster.
        df: per-(protein, go) candidate rows. Must carry the numeric KNN /
            alignment / taxonomy / anc2vec / emb_pca feature columns and the
            string categoricals ``qualifier`` / ``evidence_code`` /
            ``taxonomic_relation``. Missing columns are routed through the
            model's native missing-value branch.
        categorical_codes: the lab's per-column sorted-unique string vocab
            (``{column: [val0, val1, ...]}``), persisted at registration under
            ``RerankerModel.metrics['__categorical_codes__']``. Must include a
            ``plm_id`` entry.
        plm_id: the model's PLM id (e.g. ``"prot_t5"``); encoded against the
            ``plm_id`` vocabulary.
        k_context: the model's K-context constant (e.g. ``10``); injected as
            a float32 feature.

    Returns:
        ``np.ndarray`` of sigmoid-squashed scores in ``(0, 1)`` aligned to the
        rows of ``df``. Empty input returns an empty array.
    """
    import numpy as np

    n = len(df)
    if n == 0:
        return np.empty(0, dtype=np.float64)

    x = _build_feature_matrix(
        booster, df, categorical_codes=categorical_codes, plm_id=plm_id, k_context=k_context
    )
    raw = np.asarray(booster.predict(x), dtype=np.float64)
    if raw.size == 0:
        return raw
    # LambdaRank emits unbounded reals; squash so callers get (0, 1). Monotonic,
    # so per-protein ranking and cafaeval threshold sweeps are unchanged.
    return np.asarray(1.0 / (1.0 + np.exp(-raw)))


def _build_feature_matrix(
    booster: lgb.Booster,
    df: pd.DataFrame,
    *,
    categorical_codes: dict[str, list[str]],
    plm_id: str,
    k_context: float,
) -> np.ndarray:
    """Build the float32 feature matrix in the booster's own column order.

    Reproduces ``_inject_src_features`` + ``_encode_cat_batch`` from the lab's
    pooled staging: inject ``plm_id`` / ``k_context`` constants, vocab-encode
    categoricals, numeric-coerce the rest, NaN for absent columns.
    """
    import numpy as np
    import pandas as pd

    feature_cols = list(booster.feature_name())
    n = len(df)
    code_maps = _code_maps(categorical_codes)
    cat_set = set(UNIVERSAL_CATEGORICAL_COLUMNS)
    plm_code = code_maps.get("plm_id", {}).get(plm_id, CAT_MISSING_CODE)
    k_val = np.float32(k_context)

    x = np.empty((n, len(feature_cols)), dtype=np.float32)
    for j, col in enumerate(feature_cols):
        if col == "plm_id":
            x[:, j] = np.float32(plm_code)
        elif col == "k_context":
            x[:, j] = k_val
        elif col in cat_set:
            cm = code_maps.get(col, {})
            x[:, j] = _encode_cat_series(df[col] if col in df.columns else None, cm, n)
        elif col in df.columns:
            x[:, j] = pd.to_numeric(df[col], errors="coerce").to_numpy(dtype=np.float32)
        else:
            x[:, j] = np.nan
    return x


def _encode_cat_series(
    series: pd.Series | None,
    code_map: dict[str, int],
    n: int,
) -> np.ndarray:
    """Vocab-encode one categorical column to int codes (-1 for None / unseen).

    Mirrors ``protea_reranker_lab.bucket_io._encode_cat_batch``: None / NaN map
    to ``CAT_MISSING_CODE`` and any value absent from ``code_map`` (an
    out-of-vocabulary string the booster never saw) also maps to it.
    """
    import numpy as np

    if series is None:
        return np.full(n, CAT_MISSING_CODE, dtype=np.float32)
    out = np.empty(n, dtype=np.float32)
    for i, v in enumerate(series.to_numpy(dtype=object)):
        if v is None or (isinstance(v, float) and v != v):
            out[i] = CAT_MISSING_CODE
        else:
            out[i] = code_map.get(v, CAT_MISSING_CODE)
    return out


def load_universal_context(artifact_uri: str, store: Any) -> dict[str, Any]:
    """Recover universal-scoring metadata from the model's sibling run.json.

    The universal booster needs three pieces of state that are NOT in
    ``model.txt``: the categorical vocabulary (``categorical_codes``), the
    source PLM, and the K-context. They are published alongside the booster
    under the same prefix (``runs/<id>/run.json``). We resolve that sibling
    key from the ``model.txt`` artifact URI, read it through the same store,
    and pull the metadata block.

    This is the single source of truth for universal context recovery,
    shared by both the predict path
    (:class:`protea.core.operations.predict_go_terms._reranker_scorer.RerankerScorer`)
    and the evaluation-artifacts path
    (:mod:`protea.core.operations._run_cafa_artifacts`).

    Returns ``{"categorical_codes": dict, "plm_id": str, "k_context": float}``.

    Raises:
        ValueError: when ``run.json`` is missing ``categorical_codes`` or the
            single-source ``multi_manifest_pool`` block needed for plm/k.
    """
    import json

    from protea.core.reranker import _uri_to_key

    run_uri = artifact_uri.rsplit("/", 1)[0] + "/run.json"
    run_key = _uri_to_key(run_uri, store)
    run = json.loads(store.get(run_key).decode("utf-8"))
    categorical_codes = run.get("categorical_codes")
    if not categorical_codes:
        raise ValueError(
            "universal booster run.json is missing 'categorical_codes'; "
            "cannot reproduce the training categorical encoding "
            f"(run.json at {run_uri})."
        )
    meta = universal_meta_from_run(run)
    if meta is None:
        raise ValueError(
            "universal booster run.json is missing the single-source "
            "'multi_manifest_pool' block needed for plm_id / k_context "
            f"(run.json at {run_uri})."
        )
    return {
        "categorical_codes": categorical_codes,
        "plm_id": meta["plm_id"],
        "k_context": meta["k_context"],
    }


def universal_meta_from_run(run: dict[str, Any]) -> dict[str, Any] | None:
    """Extract the universal-scoring metadata block from a lab ``run.json``.

    Returns ``{"plm_id": str, "k_context": float}`` when the run advertises a
    single-source universal pool, else ``None``. The PLM and K come from the
    run's ``multi_manifest_pool`` (the lab records one entry per source); a
    universal v227-lineage run has exactly one entry.
    """
    pool = run.get("multi_manifest_pool") or []
    if not pool:
        return None
    first = pool[0]
    plm_id = first.get("plm_id")
    k_context = first.get("k_context")
    if plm_id is None or k_context is None:
        return None
    return {"plm_id": str(plm_id), "k_context": float(k_context)}


__all__ = [
    "CAT_MISSING_CODE",
    "INJECTED_COLUMNS",
    "UNIVERSAL_CATEGORICAL_COLUMNS",
    "is_universal_booster",
    "load_universal_context",
    "score_universal",
    "universal_meta_from_run",
]
