"""Universal reranker scoring for the method-runtime container.

Self-contained mirror of :mod:`protea.core._universal_reranker` for the
``protea-method-runtime`` image, which ships only ``protea-method`` (not the
full ``protea`` package). The scoring algorithm is identical: it reproduces
the lab's authoritative staging encoding
(``protea_reranker_lab.pooled_staging._inject_src_features`` +
``_encode_cat_batch``) so a universal booster trained in the lab scores
bit-for-bit the same here as it did at validation time.

A regression test (``tests/test_universal_scorer_parity.py``) pins this
module's output to the core module's output so the two cannot drift.

Universal layout (vs the per-cell boosters the rest of the image scores):

* DROPS the reserved ``aspect`` column (a grouping key, not a feature);
* ADDS ``plm_id`` (vocab-encoded int code) and ``k_context`` (float32),
  the source constants injected at staging time.

The generic ``protea_method.reranker.apply_reranker`` cannot score it
(it coerces string categoricals to NaN and never injects the constants),
so the container post-hoc rescores feature-complete prediction rows with
this module after running ``predict`` in no-reranker mode.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:  # pragma: no cover
    import lightgbm as lgb
    import pandas as pd

CAT_MISSING_CODE = -1

UNIVERSAL_CATEGORICAL_COLUMNS = (
    "qualifier",
    "evidence_code",
    "taxonomic_relation",
    "plm_id",
)


def is_universal_booster(booster: lgb.Booster) -> bool:
    """True when ``booster`` carries both injected source constants."""
    feats = set(booster.feature_name())
    return "plm_id" in feats and "k_context" in feats


def load_universal_meta(bundle: Path) -> dict[str, Any] | None:
    """Load ``categorical_codes`` + plm/k metadata for the universal booster.

    Reads ``<bundle>/reranker/universal_run.json`` (the enriched lab run.json
    shipped alongside ``universal.txt``). Returns ``None`` when absent or
    missing the required blocks; the caller then falls back to the per-cell /
    distance path.
    """
    run_path = bundle / "reranker" / "universal_run.json"
    if not run_path.exists():
        return None
    run = json.loads(run_path.read_text())
    cat_codes = run.get("categorical_codes")
    pool = run.get("multi_manifest_pool") or []
    if not cat_codes or not pool:
        return None
    first = pool[0]
    plm_id = first.get("plm_id")
    k_context = first.get("k_context")
    if plm_id is None or k_context is None:
        return None
    return {
        "categorical_codes": cat_codes,
        "plm_id": str(plm_id),
        "k_context": float(k_context),
    }


def score_universal(
    booster: lgb.Booster,
    df: pd.DataFrame,
    *,
    categorical_codes: dict[str, list[str]],
    plm_id: str,
    k_context: float,
) -> np.ndarray:
    """Score ``df`` with the universal ``booster`` (sigmoid-squashed, (0, 1)).

    See :func:`protea.core._universal_reranker.score_universal` for the full
    contract; this is the standalone container copy.
    """
    import pandas as pd

    feature_cols = list(booster.feature_name())
    n = len(df)
    if n == 0:
        return np.empty(0, dtype=np.float64)

    code_maps = {
        col: {val: idx for idx, val in enumerate(vals)}
        for col, vals in categorical_codes.items()
    }
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
            x[:, j] = _encode_cat_series(
                df[col] if col in df.columns else None, code_maps.get(col, {}), n
            )
        elif col in df.columns:
            x[:, j] = pd.to_numeric(df[col], errors="coerce").to_numpy(dtype=np.float32)
        else:
            x[:, j] = np.nan

    raw = np.asarray(booster.predict(x), dtype=np.float64)
    if raw.size == 0:
        return raw
    return np.asarray(1.0 / (1.0 + np.exp(-raw)))


def _encode_cat_series(
    series: pd.Series | None,
    code_map: dict[str, int],
    n: int,
) -> np.ndarray:
    if series is None:
        return np.full(n, CAT_MISSING_CODE, dtype=np.float32)
    out = np.empty(n, dtype=np.float32)
    for i, v in enumerate(series.to_numpy(dtype=object)):
        if v is None or (isinstance(v, float) and v != v):
            out[i] = CAT_MISSING_CODE
        else:
            out[i] = code_map.get(v, CAT_MISSING_CODE)
    return out


__all__ = [
    "CAT_MISSING_CODE",
    "UNIVERSAL_CATEGORICAL_COLUMNS",
    "is_universal_booster",
    "load_universal_meta",
    "score_universal",
]
