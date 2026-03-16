"""K-nearest-neighbor search backends for GO term prediction.

Backends
--------
numpy
    Exact brute-force cosine or L2 distance via matrix multiplication.
    No dependencies beyond NumPy. Suitable for reference sets up to ~100K.

faiss
    Wraps the FAISS library (``faiss-cpu``).
    Supports exact (Flat) and approximate (IVFFlat, HNSW) indices.
    Significantly faster for large reference sets (>100K vectors).

Metric convention
-----------------
Both backends return **distances** (lower = more similar):

- ``cosine``  → D = 1 − cosine_similarity ∈ [0, 2]
- ``l2``      → D = squared Euclidean distance ∈ [0, ∞)

Returned type
-------------
``search_knn`` returns::

    list[list[tuple[str, float]]]

One inner list per query; each tuple is ``(ref_accession, distance)``,
sorted ascending by distance, length ≤ ``k`` (may be shorter if
``distance_threshold`` filters them out).
"""

from __future__ import annotations

from typing import Any

import numpy as np

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def search_knn(
    query_embeddings: np.ndarray,
    ref_embeddings: np.ndarray,
    ref_accessions: list[str],
    k: int,
    *,
    distance_threshold: float | None = None,
    backend: str = "numpy",
    metric: str = "cosine",
    faiss_index_type: str = "Flat",
    faiss_nlist: int = 100,
    faiss_nprobe: int = 10,
    faiss_hnsw_m: int = 32,
    faiss_hnsw_ef_search: int = 64,
) -> list[list[tuple[str, float]]]:
    """Search for the k nearest reference proteins for each query embedding.

    Parameters
    ----------
    query_embeddings:
        Shape ``(n_queries, dim)``.  Need not be normalised.
    ref_embeddings:
        Shape ``(n_refs, dim)``.  Need not be normalised.
    ref_accessions:
        Length ``n_refs``.  Maps index positions to accession strings.
    k:
        Maximum number of neighbours to return per query.
    distance_threshold:
        If set, discard neighbours with distance > threshold.
    backend:
        ``"numpy"`` (exact brute-force) or ``"faiss"``.
    metric:
        ``"cosine"`` or ``"l2"``.
    faiss_index_type:
        One of ``"Flat"``, ``"IVFFlat"``, ``"HNSW"`` (ignored for numpy).
    faiss_nlist:
        Number of Voronoi cells for ``IVFFlat``.
    faiss_nprobe:
        Cells visited at search time for ``IVFFlat``.
    faiss_hnsw_m:
        Connections per node for ``HNSW``.
    faiss_hnsw_ef_search:
        Beam width at search time for ``HNSW``.

    Returns
    -------
    list[list[tuple[str, float]]]
        Outer list: one entry per query.
        Inner list: ``(ref_accession, distance)`` sorted ascending by distance.
    """
    if backend == "faiss":
        return _search_faiss(
            query_embeddings,
            ref_embeddings,
            ref_accessions,
            k,
            distance_threshold=distance_threshold,
            metric=metric,
            index_type=faiss_index_type,
            nlist=faiss_nlist,
            nprobe=faiss_nprobe,
            hnsw_m=faiss_hnsw_m,
            hnsw_ef_search=faiss_hnsw_ef_search,
        )
    if backend == "numpy":
        return _search_numpy(
            query_embeddings,
            ref_embeddings,
            ref_accessions,
            k,
            distance_threshold=distance_threshold,
            metric=metric,
        )
    raise ValueError(f"Unknown search backend: {backend!r}. Choose 'numpy' or 'faiss'.")


# ---------------------------------------------------------------------------
# NumPy backend
# ---------------------------------------------------------------------------


def _search_numpy(
    Q: np.ndarray,
    R: np.ndarray,
    ref_accessions: list[str],
    k: int,
    *,
    distance_threshold: float | None,
    metric: str,
) -> list[list[tuple[str, float]]]:
    """Exact brute-force search via matrix multiplication."""
    dist = _compute_distance_matrix(Q, R, metric)  # (n_queries, n_refs)
    results: list[list[tuple[str, float]]] = []
    for row in dist:
        order = np.argsort(row)
        if distance_threshold is not None:
            order = order[row[order] <= distance_threshold]
        top = order[:k]
        results.append([(ref_accessions[i], float(row[i])) for i in top])
    return results


def _compute_distance_matrix(Q: np.ndarray, R: np.ndarray, metric: str) -> np.ndarray:
    if metric == "cosine":
        Q_n = Q / (np.linalg.norm(Q, axis=1, keepdims=True) + 1e-9)
        R_n = R / (np.linalg.norm(R, axis=1, keepdims=True) + 1e-9)
        return 1.0 - (Q_n @ R_n.T)
    elif metric == "l2":
        # ||q - r||^2 = ||q||^2 + ||r||^2 - 2 q·r
        Q2 = (Q**2).sum(axis=1, keepdims=True)
        R2 = (R**2).sum(axis=1)
        return np.maximum(0.0, Q2 + R2 - 2.0 * (Q @ R.T))
    else:
        raise ValueError(f"Unknown metric: {metric!r}. Choose 'cosine' or 'l2'.")


# ---------------------------------------------------------------------------
# FAISS backend
# ---------------------------------------------------------------------------


def _search_faiss(
    Q: np.ndarray,
    R: np.ndarray,
    ref_accessions: list[str],
    k: int,
    *,
    distance_threshold: float | None,
    metric: str,
    index_type: str,
    nlist: int,
    nprobe: int,
    hnsw_m: int,
    hnsw_ef_search: int,
) -> list[list[tuple[str, float]]]:
    """FAISS-based approximate or exact nearest-neighbour search."""
    try:
        import faiss
    except ImportError as exc:
        raise ImportError("FAISS is not installed. Run `pip install faiss-cpu`.") from exc

    n_refs, dim = R.shape

    Q_f = np.ascontiguousarray(Q, dtype=np.float32)
    R_f = np.ascontiguousarray(R, dtype=np.float32)

    # Normalise for cosine (IP after normalisation ≡ cosine similarity)
    use_ip = metric == "cosine"
    if use_ip:
        faiss.normalize_L2(Q_f)
        faiss.normalize_L2(R_f)

    index = _build_faiss_index(
        R_f,
        dim,
        n_refs,
        metric=metric,
        index_type=index_type,
        nlist=nlist,
        nprobe=nprobe,
        hnsw_m=hnsw_m,
        hnsw_ef_search=hnsw_ef_search,
        use_ip=use_ip,
    )

    # Search — ask for more than k to handle duplicate accessions safely
    k_search = min(k * 4, n_refs)
    raw_distances, indices = index.search(Q_f, k_search)

    results: list[list[tuple[str, float]]] = []
    for dist_row, idx_row in zip(raw_distances, indices, strict=False):
        hits: list[tuple[str, float]] = []
        seen: set[str] = set()
        for raw_d, idx in zip(dist_row, idx_row, strict=False):
            if idx < 0:  # FAISS sentinel for "not enough neighbours"
                continue
            # Convert inner product back to cosine distance
            d = float(1.0 - raw_d) if use_ip else float(raw_d)
            if distance_threshold is not None and d > distance_threshold:
                break  # sorted by distance asc (IP desc), safe to stop
            acc = ref_accessions[idx]
            if acc in seen:
                continue
            seen.add(acc)
            hits.append((acc, d))
            if len(hits) >= k:
                break
        results.append(hits)

    return results


def _build_faiss_index(
    R_f: np.ndarray,
    dim: int,
    n_refs: int,
    *,
    metric: str,
    index_type: str,
    nlist: int,
    nprobe: int,
    hnsw_m: int,
    hnsw_ef_search: int,
    use_ip: bool,
) -> Any:  # faiss.Index — lazy import
    import faiss

    faiss_metric = faiss.METRIC_INNER_PRODUCT if use_ip else faiss.METRIC_L2

    if index_type == "Flat":
        index = faiss.IndexFlatIP(dim) if use_ip else faiss.IndexFlatL2(dim)

    elif index_type == "IVFFlat":
        # nlist must be <= n_refs and >= 1
        effective_nlist = max(1, min(nlist, n_refs))
        quantizer = faiss.IndexFlatIP(dim) if use_ip else faiss.IndexFlatL2(dim)
        index = faiss.IndexIVFFlat(quantizer, dim, effective_nlist, faiss_metric)
        index.train(R_f)
        index.nprobe = min(nprobe, effective_nlist)

    elif index_type == "HNSW":
        index = faiss.IndexHNSWFlat(dim, hnsw_m, faiss_metric)
        index.hnsw.efSearch = hnsw_ef_search

    else:
        raise ValueError(
            f"Unknown faiss_index_type: {index_type!r}. Choose 'Flat', 'IVFFlat', or 'HNSW'."
        )

    index.add(R_f)
    return index
