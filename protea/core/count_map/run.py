"""One aspect fitted and read, and the map written as a weights file (ADR-D65).

The map file is an ``.npz`` that ``numpy.load`` reads, written with fixed entry
dates so the same fit writes the same bytes and the method id derived from its
digest is stable. Per aspect ``a`` it holds ``a/terms`` (the columns),
``a/count_data``, ``a/count_indices`` and ``a/count_indptr`` (K, units x terms,
CSR), ``a/carried`` (N per unit), ``a/base_rate`` (pi per column) and
``a/strength`` (m).
"""

from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from typing import Any

import numpy as np

from protea.core.count_map.fit import StrengthFit
from protea.core.count_map.inputs import AspectProblem, Columns
from protea.core.count_map.reading import TOP, CountMap, Rows, ranks, read

#: Every entry's date: the format's earliest, so the bytes do not carry the run's time.
FIXED_DATE = (1980, 1, 1, 0, 0, 0)


@dataclass(frozen=True)
class AspectResult:
    """One aspect's fit, its reading and what the record keeps of it."""

    columns: Columns
    cmap: CountMap
    rows: Rows
    record: dict[str, Any]


def fit_and_read(columns: Columns, problem: AspectProblem, top: int | None) -> AspectResult:
    """m fitted on the donors alone, then the targets read with it.

    A reading at the grid floor (``top`` None) also records how many of its rows
    rank past the laboratory's fixed depth of 100 within their target.
    """
    fit = StrengthFit(problem.carriers, problem.states, problem.group, problem.base_rate)
    m, bits, grid = fit.best()
    cmap = CountMap(fit.counts, fit.carried, problem.base_rate, m)
    rows = read(cmap, problem.target_view, top=top)
    record = {
        "donors": len(problem.donors),
        "groups": int(problem.group.max()) + 1 if len(problem.group) else 0,
        "terms": len(columns.terms),
        "targets": len(problem.targets),
        "strength": m,
        "bits_at_strength": bits,
        "grid": grid,
        "sizes": fit.sizes(),
        "rows": len(rows),
        "rows_from_a_unit": int((rows.unit >= 0).sum()),
    }
    if top is None:
        record["rows_past_rank_100"] = int((ranks(rows) > TOP).sum())
    return AspectResult(columns, cmap, rows, record)


def _arrays(result: AspectResult) -> dict[str, np.ndarray]:
    a, cmap = result.columns.aspect, result.cmap
    counts = cmap.counts.tocsr()
    counts.sort_indices()
    return {
        f"{a}/terms": np.asarray(result.columns.terms, dtype=str),
        f"{a}/count_data": counts.data.astype(np.float64),
        f"{a}/count_indices": counts.indices.astype(np.int32),
        f"{a}/count_indptr": counts.indptr.astype(np.int64),
        f"{a}/carried": np.asarray(cmap.carried, dtype=np.float64),
        f"{a}/base_rate": np.asarray(cmap.base_rate, dtype=np.float64),
        f"{a}/strength": np.asarray([cmap.strength], dtype=np.float64),
    }


def map_bytes(results: list[AspectResult]) -> bytes:
    """The map of every aspect as one ``.npz`` with fixed entry dates."""
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for result in sorted(results, key=lambda r: r.columns.aspect):
            for name, array in _arrays(result).items():
                buffer = io.BytesIO()
                np.save(buffer, array, allow_pickle=False)
                info = zipfile.ZipInfo(f"{name}.npy", date_time=FIXED_DATE)
                info.compress_type = zipfile.ZIP_DEFLATED
                archive.writestr(info, buffer.getvalue())
    return sink.getvalue()


__all__ = ["AspectResult", "fit_and_read", "map_bytes"]
