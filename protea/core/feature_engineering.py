"""Feature engineering utilities for functional annotation enrichment.

Provides pairwise alignment metrics (Needleman–Wunsch and Smith–Waterman)
via parasail and taxonomic distance computation via ete3 NCBITaxa.

These features complement the embedding-space KNN distance stored in
``GOPrediction.distance`` with sequence-level and phylogenetic signals.

Performance notes:

- Alignment is O(m*n) per pair; parasail uses SIMD acceleration.
- Taxonomy lookups use an LRU cache over lineage queries (ete3 local
  SQLite). First call may trigger a DB download if the ete3 database
  is absent, unless ``PROTEA_TAXONOMY_DB_PATH`` declares the database,
  which is then loaded from that file and never downloaded.
"""

from __future__ import annotations

import os
import sqlite3
from functools import lru_cache
from typing import Any

try:
    import parasail  # type: ignore[import-untyped]

    _PARASAIL_AVAILABLE = True
except ImportError:
    _PARASAIL_AVAILABLE = False

_ETE3_IMPORT_ERROR: ImportError | None = None
try:
    from ete3 import NCBITaxa  # type: ignore[import-untyped]

    _ETE3_AVAILABLE = True
except ImportError as exc:
    # Kept for the declared taxonomy's refusal. On Python 3.13 and later ete3
    # 3.1.3 fails here, because its package import reaches ``import cgi``.
    _ETE3_AVAILABLE = False
    _ETE3_IMPORT_ERROR = exc


# ---------------------------------------------------------------------------
# Alignment
# ---------------------------------------------------------------------------


def compute_nw(seq1: str, seq2: str, *, gap_open: int = 10, gap_extend: int = 1) -> dict[str, Any]:
    """Global alignment (Needleman–Wunsch) via parasail/BLOSUM62.

    Returns a dict with keys:
        identity_nw, similarity_nw, alignment_score_nw,
        gaps_pct_nw, alignment_length_nw, length_query, length_ref
    """
    if not _PARASAIL_AVAILABLE:
        raise RuntimeError(
            "parasail is required for alignment features. Install it with: pip install parasail"
        )

    result = parasail.nw_trace_striped_32(seq1, seq2, gap_open, gap_extend, parasail.blosum62)
    return _parse_alignment(result, seq1, seq2, suffix="nw")


def compute_sw(seq1: str, seq2: str, *, gap_open: int = 10, gap_extend: int = 1) -> dict[str, Any]:
    """Local alignment (Smith–Waterman) via parasail/BLOSUM62.

    Returns a dict with keys:
        identity_sw, similarity_sw, alignment_score_sw,
        gaps_pct_sw, alignment_length_sw
    """
    if not _PARASAIL_AVAILABLE:
        raise RuntimeError(
            "parasail is required for alignment features. Install it with: pip install parasail"
        )

    result = parasail.sw_trace_striped_32(seq1, seq2, gap_open, gap_extend, parasail.blosum62)
    return _parse_alignment(result, seq1, seq2, suffix="sw")


def _parse_alignment(result: Any, seq1: str, seq2: str, suffix: str) -> dict[str, Any]:
    aligned_q = result.traceback.query
    aligned_r = result.traceback.ref
    comp_line = result.traceback.comp

    aln_len = len(aligned_q)
    if aln_len == 0:
        out: dict[str, Any] = {
            f"identity_{suffix}": 0.0,
            f"similarity_{suffix}": 0.0,
            f"alignment_score_{suffix}": float(result.score),
            f"gaps_pct_{suffix}": 0.0,
            f"alignment_length_{suffix}": 0.0,
        }
    else:
        matches = sum(
            a == b for a, b in zip(aligned_q, aligned_r, strict=False) if a != "-" and b != "-"
        )
        similarity = sum(c in "|:" for c in comp_line)
        gaps = aligned_q.count("-") + aligned_r.count("-")
        out = {
            f"identity_{suffix}": matches / aln_len,
            f"similarity_{suffix}": similarity / aln_len,
            f"alignment_score_{suffix}": float(result.score),
            f"gaps_pct_{suffix}": gaps / aln_len,
            f"alignment_length_{suffix}": float(aln_len),
        }

    if suffix == "nw":
        out["length_query"] = len(seq1)
        out["length_ref"] = len(seq2)

    return out


def compute_alignment(seq1: str, seq2: str) -> dict[str, Any]:
    """Compute both NW and SW alignment metrics in one call."""
    nw = compute_nw(seq1, seq2)
    sw = compute_sw(seq1, seq2)
    return {**nw, **sw}


# ---------------------------------------------------------------------------
# Taxonomy
# ---------------------------------------------------------------------------

#: The declared taxonomy database. Unset, ``NCBITaxa()`` opens ete3's default
#: under ``$HOME`` and downloads today's NCBI dump when that file is absent or
#: in a format it does not read, as it always has. Set, the named file is the
#: taxonomy: it is read as it is on disk and never downloaded or rebuilt, so a
#: missing or unreadable file raises :class:`DeclaredTaxonomyUnavailable`.
TAXONOMY_DB_PATH_ENV = "PROTEA_TAXONOMY_DB_PATH"


class DeclaredTaxonomyUnavailable(RuntimeError):
    """The declared taxonomy cannot be used as it is.

    The file is missing, ete3 would have to rebuild it, or ete3 itself cannot
    be imported. A configuration error and not a fact about two taxa, so
    :func:`compute_taxonomy` lets it through rather than calling the pair
    unrelated: swallowed, it would null every taxonomy feature in silence.
    """


if _ETE3_AVAILABLE:

    class _NeverDownloadingNCBITaxa(NCBITaxa):
        """``NCBITaxa`` with its only door to the network shut.

        ete3 3.1.3 reaches the network only through ``update_taxonomy_database``,
        and its constructor calls it when the file is missing and when the
        file's format is not the one it reads. Refusing there makes "never
        downloads" a property of this object, not of a check made just before
        building it.
        """

        def update_taxonomy_database(self, taxdump_file: str | None = None) -> None:
            db = getattr(self, "db", None)
            if db is not None:
                db.close()
            raise DeclaredTaxonomyUnavailable(
                f"{TAXONOMY_DB_PATH_ENV}={self.dbfile} cannot be read without rebuilding "
                "it from a downloaded NCBI dump, and a declared taxonomy is never downloaded"
            )


_ncbi: NCBITaxa | None = None


def _declared_taxonomy_path() -> str | None:
    """The file ``PROTEA_TAXONOMY_DB_PATH`` declares, or ``None`` when unset."""
    return os.environ.get(TAXONOMY_DB_PATH_ENV, "").strip() or None


def _load_declared_taxonomy(path: str) -> NCBITaxa:
    """Open the declared database as it is on disk, or refuse.

    ``update=True`` keeps ete3's own format check, which the subclass turns
    from a rebuild into a refusal.
    """
    if not os.path.isfile(path):
        raise DeclaredTaxonomyUnavailable(
            f"{TAXONOMY_DB_PATH_ENV}={path} does not exist; a declared taxonomy is "
            "loaded from that file and never downloaded"
        )
    try:
        return _NeverDownloadingNCBITaxa(dbfile=path, update=True)
    except sqlite3.DatabaseError as exc:
        raise DeclaredTaxonomyUnavailable(
            f"{TAXONOMY_DB_PATH_ENV}={path} is not an ete3 taxonomy database ({exc})"
        ) from exc


def _get_ncbi() -> NCBITaxa:
    global _ncbi
    if not _ETE3_AVAILABLE:
        declared = _declared_taxonomy_path()
        if declared is not None:
            raise DeclaredTaxonomyUnavailable(
                f"{TAXONOMY_DB_PATH_ENV}={declared} is declared, but ete3 cannot be "
                f"imported on this interpreter ({_ETE3_IMPORT_ERROR})"
            )
        raise RuntimeError(
            "ete3 is required for taxonomy features. Install it with: pip install ete3"
        )
    if _ncbi is None:
        declared = _declared_taxonomy_path()
        _ncbi = NCBITaxa() if declared is None else _load_declared_taxonomy(declared)
    return _ncbi


def warmup_taxonomy_db() -> None:
    """Pre-initialize the NCBITaxa database.

    Call at worker startup so the download (~100 MB on first run)
    happens before any batch is processed, not mid-flight. With
    ``PROTEA_TAXONOMY_DB_PATH`` set it loads that file instead and never
    downloads; a declared file that cannot be loaded raises
    :class:`DeclaredTaxonomyUnavailable`.
    """
    if not _ETE3_AVAILABLE and _declared_taxonomy_path() is None:
        return
    import logging

    log = logging.getLogger(__name__)
    log.info("Warming up NCBI taxonomy database...")
    _get_ncbi()
    log.info("NCBI taxonomy database ready.")


@lru_cache(maxsize=100_000)
def _cached_lineage(tid: int) -> list[int]:
    return _get_ncbi().get_lineage(tid)  # type: ignore[return-value]


def _normalize_tax_id(raw: Any) -> int | None:
    if raw is None:
        return None
    try:
        return int(str(raw).strip())
    except (ValueError, TypeError):
        return None


def compute_taxonomy(t1_raw: Any, t2_raw: Any) -> dict[str, Any]:
    """Compute taxonomic distance between two NCBI taxonomy IDs.

    Returns a dict with keys:
        taxonomic_lca, taxonomic_distance, taxonomic_common_ancestors,
        taxonomic_relation
    """
    t1 = _normalize_tax_id(t1_raw)
    t2 = _normalize_tax_id(t2_raw)

    _null: dict[str, Any] = {
        "taxonomic_lca": None,
        "taxonomic_distance": None,
        "taxonomic_common_ancestors": 0,
        "taxonomic_relation": "unrelated",
    }

    if t1 is None or t2 is None:
        return _null

    if t1 == t2:
        return {
            "taxonomic_lca": t1,
            "taxonomic_distance": 0,
            "taxonomic_common_ancestors": 1,
            "taxonomic_relation": "same",
        }

    try:
        lin1 = _cached_lineage(t1)
        lin2 = _cached_lineage(t2)
    except DeclaredTaxonomyUnavailable:
        raise
    except Exception:
        return _null

    common = set(lin1).intersection(lin2)
    common_count = len(common)
    lca = max(common, key=lambda x: lin1.index(x)) if common_count > 0 else None

    if lca is not None:
        try:
            distance = (len(lin1) - lin1.index(lca)) + (len(lin2) - lin2.index(lca))
        except ValueError:
            distance = None
    else:
        distance = None

    relation = _classify_relation(t1, t2, common_count, lca, lin1, lin2)

    return {
        "taxonomic_lca": lca,
        "taxonomic_distance": distance,
        "taxonomic_common_ancestors": common_count,
        "taxonomic_relation": relation,
    }


def _classify_relation(
    t1: int,
    t2: int,
    common_count: int,
    lca: int | None,
    lin1: list[int],
    lin2: list[int],
) -> str:
    if t1 == t2:
        return "same"
    if t1 in lin2:
        return "ancestor"
    if t2 in lin1:
        return "descendant"
    if len(lin1) >= 2 and lin1[-2] == t2:
        return "child"
    if len(lin2) >= 2 and lin2[-2] == t1:
        return "parent"
    if common_count == 1 and lca == 1:
        return "root-only"
    if common_count <= 2:
        return "distant"
    if 3 <= common_count <= 15:
        return "intermediate"
    if common_count > 15:
        return "close"
    return "unrelated"
