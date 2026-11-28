"""The base frame's aggregates describe its own cut, or they describe nothing.

The base frame IS the candidate set after the cut. Its
``neighbor_vote_fraction`` arrives from the row, where it was written over
the whole neighbourhood the retrieval used, so under a cut the frame
carries a consensus measured over a wider set than the one it holds. An
arm cut to depth 2 then reports depth 30's agreement as its own, and
nothing about the frame says otherwise.

Recounting here rather than at the scorer is deliberate. Every consumer of
the frame reads the same columns: the scalar scorer, the vectorised
scorer, the reranker, the per-protein artefacts. Correcting the column
once, where the frame is built, means none of them has to know a cut
happened, and none of them can be the one that forgot. The frame is also
cached keyed by its cut, so the corrected values are cached with it and
each depth keeps its own.

The donor arrays are fetched only when there is a cut, and dropped again
before the frame is handed on. They are the widest columns in the table
and nothing downstream reads them, so carrying them into the parquet
would multiply its size to no purpose.

The neighbourhood signals of ``_donor_recount`` are added here too, at the
cut and only when a scoring configuration weights them: the similarity vote
reads a protein's rows together, so it can only be taken where the whole
frame of the protein is at hand.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from protea.core.operations._donor_recount import (
    KERNEL_FRACTION,
    SIMILARITY_VOTE,
    DepthCut,
    NeighbourhoodRequest,
    kernel_fraction,
    recount_at_depth,
    similarity_votes,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "DONOR_LEDGER_COLS",
    "LEDGER_SELECT_COLS",
    "build_base_frame",
    "cut_of",
    "ledger_columns",
    "recount_frame_aggregates",
)


def ledger_columns(wanted: bool) -> tuple[Any, ...]:
    """The columns to add to a SELECT, in the order of ``LEDGER_SELECT_COLS``, or nothing.

    Kept here so the names appear once. A SELECT that added them in a
    different order than ``LEDGER_SELECT_COLS`` would build a frame whose
    ranks and distances belong to different columns, which is the kind of
    mistake that produces plausible numbers. The term's aspect comes from
    ``go_term``, which every candidate SELECT already joins for the GO id.
    """
    if not wanted:
        return ()
    from protea.infrastructure.orm.models.annotation.go_term import GOTerm
    from protea.infrastructure.orm.models.embedding.go_prediction import GOPrediction

    return (*(getattr(GOPrediction, name) for name in DONOR_LEDGER_COLS), GOTerm.aspect)

#: Fetched only under a cut, dropped before the frame is cached.
DONOR_LEDGER_COLS: tuple[str, ...] = (
    "donor_k_positions",
    "donor_sequence_ranks",
    "donor_distances",
)

#: What a SELECT under a cut adds, in order: the ledger, then the term's
#: aspect. An aspect-separated search ranks each aspect's neighbours on their
#: own, so the neighbourhood the similarity vote divides by is one protein's in
#: one aspect. Dropped with the ledger.
LEDGER_SELECT_COLS: tuple[str, ...] = (*DONOR_LEDGER_COLS, "aspect")

#: Written by the recount. Every one of them is a function of the cut; the
#: minima are not here because the argument of a minimum is the term's
#: shallowest donor, which survives every cut that keeps the row at all.
#:
#: ``vote_count`` is NOT here, deliberately. It counts annotation rows and a
#: recount counts donors, and those differ on 37.6 per cent of pairs. Writing
#: the donor count into it would leave one column meaning voters in a cut arm
#: and paperwork in an uncut one, which is the defect this campaign keeps
#: finding: a level that is not named by every field that varies. The donor
#: count goes to ``donor_count``, which already means exactly that.
_RECOUNTED: tuple[str, ...] = (
    "donor_count",
    "neighbor_vote_fraction",
    "neighbor_mean_distance",
    "neighbor_distance_std",
)


def cut_of(ctx: Any) -> DepthCut | None:
    """The depth this frame was cut at, or None when it was not cut."""
    by_protein = getattr(ctx, "max_k_position", None)
    by_sequence = getattr(ctx, "max_sequence_rank", None)
    if by_protein is None and by_sequence is None:
        return None
    return DepthCut(max_k_position=by_protein, max_sequence_rank=by_sequence)


def recount_frame_aggregates(
    df: Any, cut: DepthCut | None, neighbourhood: NeighbourhoodRequest | None = None
) -> Any:
    """Rewrite the cut-dependent columns from each row's donor ledger.

    Args:
        df: The base frame, carrying :data:`DONOR_LEDGER_COLS`, and the
            term's ``aspect`` when the similarity vote is asked for over an
            aspect-separated search.
        cut: The depth the frame was selected at, or None to leave the
            frame alone, which is correct: with no cut the stored
            aggregates already describe the whole neighbourhood.
        neighbourhood: The neighbourhood signals to add at the cut, or None
            to add neither. Asked for only by a configuration that weights
            them, since the similarity vote stops at a protein whose
            neighbourhood is not whole (``similarity_votes``).

    Returns:
        The frame with the cut-dependent columns recounted, the asked
        neighbourhood signals added, and the ledger columns dropped. Rows
        whose ledger is absent keep a null in every recounted column rather
        than their stored value, so the absence travels instead of being
        papered over. The run guard refuses such a set up front, so this is
        the second line rather than the first.
    """
    if cut is None:
        return df.drop(columns=[c for c in DONOR_LEDGER_COLS if c in df], errors="ignore")
    if df.empty:
        return df.drop(columns=[c for c in LEDGER_SELECT_COLS if c in df], errors="ignore")
    ledgers = [_as_lists(row) for row in df[list(DONOR_LEDGER_COLS)].to_dict("records")]
    recounted: dict[str, list[Any]] = {name: [] for name in _RECOUNTED}
    for ledger in ledgers:
        got = recount_at_depth(ledger, cut)
        recounted["donor_count"].append(None if got is None else got.donor_count)
        recounted["neighbor_vote_fraction"].append(
            None if got is None else got.vote_fraction()
        )
        recounted["neighbor_mean_distance"].append(
            None if got is None else got.mean_distance
        )
        recounted["neighbor_distance_std"].append(
            None if got is None else got.distance_std
        )
    if neighbourhood is not None:
        recounted.update(_neighbourhood_columns(df, ledgers, cut, neighbourhood))
    out = df.copy()
    for name, values in recounted.items():
        out[name] = values
    return out.drop(columns=[c for c in LEDGER_SELECT_COLS if c in out], errors="ignore")


def _neighbourhood_columns(
    df: Any, ledgers: list[dict[str, Any]], cut: DepthCut, request: NeighbourhoodRequest
) -> dict[str, list[Any]]:
    """The asked neighbourhood signals of every row, in the frame's order.

    The kernel fraction reads each row alone. The similarity vote reads a
    protein's rows together, per aspect when the search ranked each aspect on
    its own (``NeighbourhoodRequest.by_aspect``), since their ledgers together
    are the neighbourhood it divides by. ``ledgers`` are the frame's rows'
    ledgers, in its order.
    """
    out: dict[str, list[Any]] = {}
    if KERNEL_FRACTION in request.signals:
        out[KERNEL_FRACTION] = [kernel_fraction(ledger, cut) for ledger in ledgers]
    if SIMILARITY_VOTE in request.signals:
        keys = ["protein_accession", "aspect"] if request.by_aspect else ["protein_accession"]
        votes: list[float | None] = [None] * len(ledgers)
        for key, index in df.groupby(keys, sort=False, dropna=False).indices.items():
            named = key if isinstance(key, tuple) else (key,)
            where = "protein " + " in aspect ".join(str(part) for part in named)
            group = similarity_votes([ledgers[i] for i in index], cut, where)
            for i, vote in zip(index, group, strict=True):
                votes[i] = vote
        out[SIMILARITY_VOTE] = votes
    return out


def _as_lists(row: dict[str, Any]) -> dict[str, Any]:
    """Normalise the array columns pandas may hand back as numpy arrays.

    ``recount_at_depth`` compares lengths and zips strictly, and a numpy
    array of dtype object holding None is not the same absence as a None,
    so the two are made one shape here rather than inside the recount.
    """
    return {key: _seq_or_none(row.get(key)) for key in DONOR_LEDGER_COLS}


def _seq_or_none(value: Any) -> Sequence[Any] | None:
    if value is None:
        return None
    try:
        return list(value)
    except TypeError:
        return None


def build_base_frame(
    session: Any, ctx: Any, base_select: Any, base_cols: tuple[str, ...]
) -> tuple[Any, int]:
    """Fetch, dedup and recount the base frame; return ``(df, raw_count)``.

    Dedup keeps the lowest-distance row per ``(protein, go_id)``, matching
    the ORM path's ``order_by(... distance).first()`` winner and leaving
    the output ordered by ``(protein, go_id)``.

    The recount runs after the dedup, so it is done once per surviving row
    rather than once per row that was about to be dropped.
    """
    import pandas as pd

    cut = cut_of(ctx)
    columns = list(base_cols) + (list(LEDGER_SELECT_COLS) if cut else [])
    rows = session.execute(base_select(ctx, with_ledger=bool(cut))).all()
    df = pd.DataFrame.from_records([tuple(r) for r in rows], columns=columns)
    raw_count = int(len(df))
    if raw_count:
        df = (
            df.sort_values(["protein_accession", "go_id", "distance"], kind="mergesort")
            .drop_duplicates(subset=["protein_accession", "go_id"], keep="first")
            .reset_index(drop=True)
        )
    neighbourhood = getattr(ctx, "neighbourhood", None)
    return recount_frame_aggregates(df, cut, neighbourhood), raw_count
