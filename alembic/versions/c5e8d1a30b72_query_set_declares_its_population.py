"""query_set declares where its population came from

Revision ID: c5e8d1a30b72
Revises: b4c17e2a9f08
Create Date: 2026-11-08

THE GAP THIS CLOSES. ``query_set`` was the only one of the six set tables with
NO outgoing foreign key at all, so the population every number is measured over
declared its derivation in free text:

    "Las 40.531 dianas de la ventana ea0f4334 (projected, live_in_both_ends),
    exportadas por el job a9b1c60e con sha256 c4f15a254272..."

The window as a prefix to resolve by hand, the sha as a string nothing compares
against, and the job as a word. Two of the four sets are named almost the same
(``dianas-valid-211-227`` and ``...-titular``) and come from DIFFERENT windows,
``ea94e57e`` and ``ea0f4334``, 63.734 targets against 40.531.

WHY THIS MIGRATION DECLARES NOTHING. Every existing row becomes
``legacy_undeclared``, including the four whose derivation IS recoverable:
``job.payload->>'evaluation_set_id'`` holds the window and
``job_event.fields->>'sha256'`` holds the digest, and both agree with the name
suffix and the entry count. Recovering them is a judgement that should leave a
job and its events, not a silent write inside a schema change, and it has to
fetch each FASTA and compare accession sets to be evidence rather than
inference. That is ``declare_query_set_provenance``, not this file.

``legacy_undeclared`` is a VALUE and not a NULL for the same reason: NULL cannot
tell "not written yet" from "looked and could not establish it".
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "c5e8d1a30b72"
down_revision = "b4c17e2a9f08"
branch_labels = None
depends_on = None

SOURCES = ("evaluation_targets", "fasta_upload", "quick_annotation", "legacy_undeclared")


def upgrade() -> None:
    op.add_column(
        "query_set",
        sa.Column(
            "source_kind",
            sa.String(length=32),
            nullable=False,
            server_default="legacy_undeclared",
        ),
    )
    op.add_column(
        "query_set", sa.Column("evaluation_set_id", sa.dialects.postgresql.UUID(), nullable=True)
    )
    op.add_column("query_set", sa.Column("artifact_uri", sa.String(length=512), nullable=True))
    op.add_column("query_set", sa.Column("content_sha256", sa.String(length=64), nullable=True))
    op.add_column("query_set", sa.Column("job_id", sa.dialects.postgresql.UUID(), nullable=True))
    op.add_column("query_set", sa.Column("code_revision", sa.String(length=40), nullable=True))

    op.create_index("ix_query_set_evaluation_set_id", "query_set", ["evaluation_set_id"])
    op.create_index("ix_query_set_job_id", "query_set", ["job_id"])
    op.create_foreign_key(
        "fk_query_set_evaluation_set_id",
        "query_set",
        "evaluation_set",
        ["evaluation_set_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_query_set_job_id", "query_set", "job", ["job_id"], ["id"], ondelete="SET NULL"
    )

    listed = ", ".join(f"'{v}'" for v in SOURCES)
    op.create_check_constraint(
        "ck_query_set_source_kind", "query_set", f"source_kind IN ({listed})"
    )
    op.create_check_constraint(
        "ck_query_set_targets_are_declared",
        "query_set",
        "source_kind <> 'evaluation_targets' OR ("
        "evaluation_set_id IS NOT NULL AND content_sha256 IS NOT NULL "
        "AND job_id IS NOT NULL AND description IS NOT NULL)",
    )
    op.create_check_constraint(
        "ck_query_set_uri_with_its_sha",
        "query_set",
        "artifact_uri IS NULL OR content_sha256 IS NOT NULL",
    )
    op.create_unique_constraint(
        "uq_query_set_window_content", "query_set", ["evaluation_set_id", "content_sha256"]
    )


def downgrade() -> None:
    op.drop_constraint("uq_query_set_window_content", "query_set", type_="unique")
    op.drop_constraint("ck_query_set_uri_with_its_sha", "query_set", type_="check")
    op.drop_constraint("ck_query_set_targets_are_declared", "query_set", type_="check")
    op.drop_constraint("ck_query_set_source_kind", "query_set", type_="check")
    op.drop_constraint("fk_query_set_job_id", "query_set", type_="foreignkey")
    op.drop_constraint("fk_query_set_evaluation_set_id", "query_set", type_="foreignkey")
    op.drop_index("ix_query_set_job_id", table_name="query_set")
    op.drop_index("ix_query_set_evaluation_set_id", table_name="query_set")
    # Named one by one and not in a loop. A loop hides the names from
    # ``tests/test_the_query_set_declares_its_population.py``, which asserts
    # that the downgrade mentions exactly what the upgrade made; a downgrade
    # that cannot be checked is one nobody finds out is wrong until they run
    # it on a real database.
    op.drop_column("query_set", "code_revision")
    op.drop_column("query_set", "job_id")
    op.drop_column("query_set", "content_sha256")
    op.drop_column("query_set", "artifact_uri")
    op.drop_column("query_set", "evaluation_set_id")
    op.drop_column("query_set", "source_kind")
