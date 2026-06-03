"""t3_5_index_audit_hot_query_paths

Revision ID: 9d2fb5a07e4c
Revises: 8c4d2a1f6e9b
Create Date: 2026-05-12 12:00:00.000000

T3.5 of the executor plan (F4 schema hardening). Walks every ORM
model in ``protea/infrastructure/orm/models/`` and audits the
columns referenced in WHERE / ORDER BY across ``protea/{api, core,
services, workers}/``. Adds indexes for the access patterns that
were previously seq-scan-bound on hot paths.

Strictly additive: every operation is ``CREATE INDEX`` (no
table rewrites, no drops). All indexes are named so the
downgrade is a deterministic ``DROP INDEX``.

Audit summary
=============

Already covered (kept untouched)
--------------------------------
* job: ``ix_job_status_created_at``, ``ix_job_operation_created_at``,
  ``ix_job_created_at``, ``parent_job_id`` (FK index).
* job_event: ``ix_job_event_job_id_ts_desc``, ``ix_job_event_event_ts_desc``.
* job_comment: ``ix_job_comment_job_id_created_at``.
* protein_go_annotation: ``ix_pga_set_accession`` (composite),
  per-FK indexes.
* go_prediction: ``ix_go_prediction_set_accession`` (composite),
  per-FK indexes.
* visitor_event: ``ix_visitor_event_day_hash``,
  ``ix_visitor_event_created_at``, ``ix_visitor_event_path``.
* experiment_run: ``ix_experiment_run_name`` (unique),
  ``ix_experiment_run_status_created_at``.
* sequence_embedding: per-FK indexes + uniqueness.
* interpro_annotation, interpro_go_mapping: dedicated
  ``idx_*`` indexes from their bring-up migrations.

Added by this migration
-----------------------
Listing queries (``ORDER BY created_at DESC LIMIT ...``) ran a
seq-scan + sort on the parent table because no ``created_at``
column index existed. The following ``(created_at DESC)``
indexes flip those to an index scan:

* ``ix_prediction_set_created_at`` — ``embeddings_service.py``
  list endpoint.
* ``ix_evaluation_set_created_at`` —
  ``annotations_service.list_evaluation_sets``.
* ``ix_annotation_set_created_at`` —
  ``annotations_service.list_annotation_sets``.
* ``ix_dataset_created_at`` — ``api/routers/datasets.py`` list.
* ``ix_embedding_config_created_at`` —
  ``api/routers/embeddings.py`` list.
* ``ix_reranker_model_created_at`` — ``api/routers/annotate.py``
  latest reranker lookup.
* ``ix_query_set_created_at`` — ``api/routers/query_sets.py``
  list.
* ``ix_api_key_created_at`` — ``api/routers/auth_api_keys.py``
  list.
* ``ix_support_entry_created_at`` —
  ``api/routers/support.py`` list.
* ``ix_ontology_snapshot_loaded_at`` —
  ``annotations_service.latest_snapshot`` /
  ``api/routers/annotate.py``.

Filtered listings (composite indexes that match WHERE + ORDER BY):

* ``ix_evaluation_result_eval_set_created_at`` —
  ``annotations_service.list_evaluation_results`` filters by
  ``evaluation_set_id`` and orders by ``created_at DESC``.

Worker hot paths:

* ``ix_job_event_job_id_event`` —
  ``base_worker._on_retry_later`` counts ``job.retry_later``
  events for a job; previously fell back to scanning every event
  for that job. Composite on ``(job_id, event)`` makes the count
  an index-only scan.

Notes
-----
* No index drops. Redundancy audit is intentionally kept out of
  scope here so the migration stays strictly additive; the
  follow-up T3.5b can prune any duplicate-FK + composite
  overlap with explicit benchmarks.
* All new indexes are non-unique and non-partial. Postgres
  evaluates ``ORDER BY <col> DESC`` against an ASC b-tree
  equally well via reverse scan, so the index DDL omits the
  ``DESC`` modifier for simplicity.
"""
from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9d2fb5a07e4c"
down_revision: str | Sequence[str] | None = "8c4d2a1f6e9b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Order matters only for downgrade reverse — keep the upgrade list
# stable so reviewers can diff against the audit notes above.
_NEW_INDEXES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("ix_prediction_set_created_at", "prediction_set", ("created_at",)),
    ("ix_evaluation_set_created_at", "evaluation_set", ("created_at",)),
    ("ix_annotation_set_created_at", "annotation_set", ("created_at",)),
    ("ix_dataset_created_at", "dataset", ("created_at",)),
    ("ix_embedding_config_created_at", "embedding_config", ("created_at",)),
    ("ix_reranker_model_created_at", "reranker_model", ("created_at",)),
    ("ix_query_set_created_at", "query_set", ("created_at",)),
    ("ix_api_key_created_at", "api_key", ("created_at",)),
    ("ix_support_entry_created_at", "support_entry", ("created_at",)),
    ("ix_ontology_snapshot_loaded_at", "ontology_snapshot", ("loaded_at",)),
    (
        "ix_evaluation_result_eval_set_created_at",
        "evaluation_result",
        ("evaluation_set_id", "created_at"),
    ),
    ("ix_job_event_job_id_event", "job_event", ("job_id", "event")),
)


def upgrade() -> None:
    """Add missing indexes for hot query paths (T3.5 audit)."""
    for name, table, columns in _NEW_INDEXES:
        op.create_index(name, table, list(columns))


def downgrade() -> None:
    """Drop the indexes added by :func:`upgrade` (reverse order)."""
    for name, table, _ in reversed(_NEW_INDEXES):
        op.drop_index(name, table_name=table)
