"""Thin facade over the ``_knn_transfer_runner`` Method Object.

Kept as a separate submodule (T2B.6) so the test-split and train-split
helpers can import the entry point without each pulling the runner
module into their import graph at top level.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.orm import Session

from protea.core.training_dump._contexts import (
    KnnTransferContext,
    SequenceContext,
    StreamOutput,
)

if TYPE_CHECKING:
    from protea.core.training_dump._payload import TrainRerankerAutoPayload


def _knn_transfer_and_label(
    session: Session,
    p: TrainRerankerAutoPayload,
    ctx: KnnTransferContext,
    *,
    sequence_context: SequenceContext | None = None,
    stream_output: StreamOutput | None = None,
) -> list[dict[str, Any]] | dict[str, Any]:
    """Run per-aspect KNN, transfer GO terms, label, compute features.

    Thin wrapper that delegates to ``_KnnTransferRunner`` (T2B.5 partial
    #8 Method Object refactor; runner lives in
    ``protea.core._knn_transfer_runner`` so this module stays under the
    §3 file-LOC ceiling). Behaviour is unchanged; the runner holds the
    per-call state as attributes so the phase methods do not have to
    thread 20+ locals through their signatures.

    ``ctx.query_known_gos`` is ``{protein_accession: {go_id}}`` of
    annotations the query already carries before the prediction cutoff
    (from ``EvaluationData.known``). Used to compute query-side Anc2Vec
    coherence features (the PK-killer signal: how close is each candidate
    GO to the query's existing annotation profile).

    Streaming mode: when ``stream_output`` is given, records are written
    to disk in ``stream_output.chunk_rows`` chunks as they are generated
    (re-ordered to iterate per ``(q_acc, aspect)`` group so the
    ancestor-expansion stays local). In this mode the function returns
    ``{"parquet_path": str, "n_rows": int}`` instead of the full list.
    ``ctx.pivot_go_ids`` (orthogonal to streaming) filters records by
    go_id; useful in either mode.
    """
    # Local import keeps the runner module decoupled from helpers' import
    # graph and avoids the cycle (the runner needs the dataclass types
    # exposed by this module).
    from protea.core._knn_transfer_runner import run_knn_transfer_and_label

    return run_knn_transfer_and_label(
        session,
        p,
        ctx,
        sequence_context=sequence_context,
        stream_output=stream_output,
    )
