"""InterPro signature->GO feature producer for the research-dataset export.

S3 of the reranker-vNext + InterPro chain. Materialises the 11
``interpro_*`` / presence columns (contracts 1.1.0) on the existing KNN
leaf records, keyed by ``(protein, go_id)``. This slice does NOT change
the candidate row-set (no KNN+InterPro union); it only annotates the KNN
candidates already emitted by the leaf-record builder. The union
expansion is a separate follow-up (S3b).

Data source. The producer left-joins an InterPro GO-prediction table loaded from the
path in :data:`INTERPRO_GO_PRED_PATH_ENV` (``PROTEA_INTERPRO_GO_PRED_PATH``).
The table is the ``(accession, go_id, score)`` TSV emitted by
``protea_sources.interpro.InterProSource.predict_go`` (true-path
propagated, max-score-wins). When the env var is unset, or the file is
absent at load time, the table is empty and every record keeps the
zero-fill defaults from
``_leaf_record_builder._interpro_default_fields`` (all ``interpro_*`` =
0/False, ``interpro_present`` = False), so existing exports and tests
keep working unchanged.

Optional richer schema: columns 4 and 5, when present, carry the
member-DB list (comma/semicolon separated, e.g. ``pfam,smart``) and the
signature count. A 3-column table yields empty member-DBs and a count of
1 per matched pair.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

#: Env var naming the InterPro GO-prediction TSV the producer joins on.
INTERPRO_GO_PRED_PATH_ENV = "PROTEA_INTERPRO_GO_PRED_PATH"

#: Canonical member-DB one-hot stems (contracts 1.1.0). The column name
#: is ``interpro_db_<stem>``.
INTERPRO_DB_ONEHOTS: tuple[str, ...] = (
    "pfam",
    "panther",
    "superfamily",
    "smart",
    "cdd",
    "prosite",
)


@dataclass(frozen=True)
class _InterProMatch:
    """One ``(protein, go_id)`` InterPro evidence row."""

    score: float
    n_signatures: int
    member_dbs: frozenset[str]


def _normalise_member_dbs(raw: str) -> frozenset[str]:
    """Map a free-form member-DB list onto the canonical one-hot stems."""
    out: set[str] = set()
    for token in raw.replace(";", ",").split(","):
        tok = token.strip().lower()
        if not tok:
            continue
        for stem in INTERPRO_DB_ONEHOTS:
            if stem in tok:
                out.add(stem)
    return frozenset(out)


def load_interpro_go_pred(
    path: str | os.PathLike[str] | None = None,
) -> dict[tuple[str, str], _InterProMatch]:
    """Load the ``(protein, go_id) -> _InterProMatch`` table.

    ``path`` defaults to the :data:`INTERPRO_GO_PRED_PATH_ENV` env var.
    Returns an empty mapping when unset (graceful no-op). Raises
    :class:`FileNotFoundError` only when an explicit path is configured
    but missing, so a misconfiguration fails fast rather than silently
    emitting an all-absent InterPro column block after hours of compute.
    """
    if path is None:
        path = os.environ.get(INTERPRO_GO_PRED_PATH_ENV)
    if not path:
        return {}
    fp = Path(path)
    if not fp.exists():
        raise FileNotFoundError(f"{INTERPRO_GO_PRED_PATH_ENV} points at a missing file: {fp}")

    table: dict[tuple[str, str], _InterProMatch] = {}
    with fp.open(encoding="utf-8") as handle:
        for line in handle:
            row = line.rstrip("\n").split("\t")
            if len(row) < 3 or not row[0] or not row[1]:
                continue
            protein, go_id = row[0], row[1]
            try:
                score = float(row[2])
            except ValueError:
                continue
            member_dbs = _normalise_member_dbs(row[3]) if len(row) >= 4 else frozenset()
            explicit_n = None
            if len(row) >= 5 and row[4].strip():
                try:
                    explicit_n = int(row[4])
                except ValueError:
                    explicit_n = None
            key = (protein, go_id)
            prev = table.get(key)
            if prev is None:
                n_sig = explicit_n if explicit_n is not None else max(1, len(member_dbs))
                table[key] = _InterProMatch(score, n_sig, member_dbs)
            else:
                merged_dbs = prev.member_dbs | member_dbs
                n_sig = explicit_n if explicit_n is not None else prev.n_signatures + 1
                table[key] = _InterProMatch(max(prev.score, score), n_sig, merged_dbs)
    return table


def apply_interpro_features(
    records: Iterable[dict[str, Any]],
    table: Mapping[tuple[str, str], _InterProMatch],
) -> None:
    """Overwrite the InterPro zero-fill defaults for matched records.

    Mutates each record in place. Records absent from ``table`` keep the
    defaults set by ``_leaf_record_builder._interpro_default_fields``
    (so all 11 columns stay present unconditionally). ``knn_present`` is
    left untouched: it is already ``True`` for every KNN leaf record in
    this slice.
    """
    if not table:
        return
    for rec in records:
        match = table.get((rec["protein_accession"], rec["go_id"]))
        if match is None:
            continue
        rec["interpro_hit"] = True
        rec["interpro_score"] = float(match.score)
        rec["interpro_n_signatures"] = int(match.n_signatures)
        rec["interpro_present"] = True
        for stem in INTERPRO_DB_ONEHOTS:
            if stem in match.member_dbs:
                rec[f"interpro_db_{stem}"] = True
