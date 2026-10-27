"""How the ground truth becomes a parquet, and comes back.

Extracted from :mod:`protea.core.evaluation`, which was at the file-LOC
budget, and extracted as this unit because it is the one group in it that
answers a single question: the shape the delta takes on disk and in the
artifact store, as opposed to how the delta is computed.

Re-exported from ``protea.core.evaluation`` so every existing import keeps
working; the names are the public surface, not the module path.

THE STRICT REUSE RULE LIVES HERE. ``load_evaluation_data_for_set`` raises
rather than recomputing a missing artifact. An evaluation that silently
rebuilt its own ground truth would be comparing against a truth nobody
sealed, which is the whole reason the artifact exists.
"""

from __future__ import annotations

import io
import uuid
from collections import defaultdict
from pathlib import Path

from sqlalchemy.orm import Session

from protea.core.evaluation import _GROUNDTRUTH_BUCKETS, EvaluationData


def _eval_data_to_dataframe(data: EvaluationData):
    """Flatten EvaluationData's five buckets into a long DataFrame.

    Columns: ``protein_accession`` (str), ``go_id`` (str), ``bucket`` (categorical
    one of nk/lk/pk/known/pk_known). One row per (protein, go_id, bucket) triple.
    """
    import pandas as pd

    rows: list[tuple[str, str, str]] = []
    for bucket_name, bucket_dict in (
        ("nk", data.nk),
        ("lk", data.lk),
        ("pk", data.pk),
        ("known", data.known),
        ("pk_known", data.pk_known),
        ("removed", data.removed),
    ):
        for protein, go_ids in bucket_dict.items():
            for go_id in go_ids:
                rows.append((protein, go_id, bucket_name))
    df = pd.DataFrame(rows, columns=["protein_accession", "go_id", "bucket"])
    df["bucket"] = df["bucket"].astype(pd.CategoricalDtype(categories=list(_GROUNDTRUTH_BUCKETS)))
    return df


def _dataframe_to_eval_data(df) -> EvaluationData:
    """Inverse of ``_eval_data_to_dataframe``."""
    nk: dict[str, set[str]] = defaultdict(set)
    lk: dict[str, set[str]] = defaultdict(set)
    pk: dict[str, set[str]] = defaultdict(set)
    known: dict[str, set[str]] = defaultdict(set)
    pk_known: dict[str, set[str]] = defaultdict(set)
    removed: dict[str, set[str]] = defaultdict(set)
    # Files written before the removed bucket existed simply carry no such
    # rows, so an older artifact deserializes with removed empty rather than
    # failing.
    bucket_to_dict = {
        "nk": nk,
        "lk": lk,
        "pk": pk,
        "known": known,
        "pk_known": pk_known,
        "removed": removed,
    }
    for protein, go_id, bucket in df[["protein_accession", "go_id", "bucket"]].itertuples(
        index=False, name=None
    ):
        bucket_to_dict[str(bucket)][str(protein)].add(str(go_id))
    return EvaluationData(
        nk=dict(nk),
        lk=dict(lk),
        pk=dict(pk),
        known=dict(known),
        pk_known=dict(pk_known),
        removed=dict(removed),
    )


def serialize_evaluation_data_to_parquet(data: EvaluationData, dest: Path) -> Path:
    """Write ``data`` to a parquet file at ``dest`` (creates parent dirs).

    Returns ``dest`` for convenience. Uses snappy compression and the long
    layout produced by ``_eval_data_to_dataframe`` — kept stable since
    consumers parse it back with ``deserialize_evaluation_data_from_bytes``.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    df = _eval_data_to_dataframe(data)
    df.to_parquet(dest, index=False, compression="snappy")
    return dest


def deserialize_evaluation_data_from_bytes(blob: bytes) -> EvaluationData:
    """Parse parquet bytes (as returned by ``ArtifactStore.get``) into EvaluationData."""
    import pandas as pd

    df = pd.read_parquet(io.BytesIO(blob))
    return _dataframe_to_eval_data(df)


def load_evaluation_data_for_set(session: Session, eval_set) -> tuple[EvaluationData, uuid.UUID]:
    """Load ground-truth for an EvaluationSet row.

    Strict reuse path: if ``eval_set.groundtruth_uri`` is set, deserializes the
    persisted parquet via the configured ArtifactStore and returns it. If not
    set, raises — recomputation on-the-fly is intentionally not allowed (see
    the project's "no on-the-fly reuse" rule). Use
    ``scripts/backfill_evaluation_groundtruth.py`` to materialize artifacts for
    legacy EvaluationSet rows that predate this column.

    Returns the EvaluationData plus the pivot OntologySnapshot ID — the caller
    should use the pivot snapshot (not the old set's) when loading the OBO for
    cafaeval, since propagated go_ids live in pivot term space.
    """
    from protea.infrastructure.orm.models.annotation.annotation_set import AnnotationSet
    from protea.infrastructure.settings import load_settings
    from protea.infrastructure.storage import get_artifact_store

    ann_old = session.get(AnnotationSet, eval_set.old_annotation_set_id)
    ann_new = session.get(AnnotationSet, eval_set.new_annotation_set_id)

    stats = eval_set.stats or {}
    pivot_raw = stats.get("pivot_ontology_snapshot_id")
    if pivot_raw:
        pivot_id = uuid.UUID(str(pivot_raw))
    else:
        # Both ann_new and ann_old are validated non-None by the caller
        # (run_cafa_evaluation); the ternary short-circuits before
        # dereferencing ann_old when ann_new is set.
        pivot_id = ann_new.ontology_snapshot_id if ann_new else ann_old.ontology_snapshot_id  # type: ignore[union-attr]

    if not eval_set.groundtruth_uri:
        raise RuntimeError(
            f"EvaluationSet {eval_set.id} has no groundtruth_uri. "
            "Run scripts/backfill_evaluation_groundtruth.py to materialize "
            "the parquet artifact, or regenerate via /annotations/evaluation-sets/generate."
        )

    project_root = Path(__file__).resolve().parents[2]
    settings = load_settings(project_root)
    store = get_artifact_store(settings)
    key = groundtruth_key_for(eval_set.id)
    blob = store.get(key)
    data = deserialize_evaluation_data_from_bytes(blob)
    return data, pivot_id


def groundtruth_key_for(eval_set_id) -> str:
    """Storage key under which an EvaluationSet's ground-truth parquet lives."""
    return f"eval_groundtruth/{eval_set_id}/groundtruth.parquet"
