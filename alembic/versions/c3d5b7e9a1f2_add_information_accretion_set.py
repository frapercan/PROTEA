"""Add information_accretion_set: IA tables with all three axes pinned

Information Accretion is estimated from term frequencies over an annotation
corpus, so an IA table is identified by (ontology snapshot, annotation set,
evidence regime). The predecessor of this table was a single nullable
``ontology_snapshot.ia_url`` string, which pinned none of the three: two tables
computed from different corpora were distinguishable only by file basename, and
``docs/IA_PROVENANCE_v227.md`` records two legitimate v227 tables that differ by
up to 14.59 while carrying the same name.

``ia_url`` is deliberately left in place. It stays as the legacy resolution
fallback in ``_resolve_ia_file`` and is not dropped here, so this revision is
additive and downgrade is a clean drop.

Revision ID: c3d5b7e9a1f2
Revises: e7a1c4f9b2d6
Create Date: 2026-07-30 00:00:00.000000

"""
from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'c3d5b7e9a1f2'
down_revision: str | Sequence[str] | None = 'e7a1c4f9b2d6'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        'information_accretion_set',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('ontology_snapshot_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('annotation_set_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('evidence_regime', sa.String(length=32), nullable=False),
        sa.Column('evidence_codes', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('job_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('artifact_uri', sa.String(length=512), nullable=False),
        sa.Column('content_sha256', sa.String(length=64), nullable=False),
        sa.Column('term_count', sa.Integer(), nullable=False),
        sa.Column('nonzero_count', sa.Integer(), nullable=False),
        sa.Column('annotation_count', sa.BigInteger(), nullable=False),
        sa.Column('protein_count', sa.Integer(), nullable=False),
        sa.Column('propagated_pairs', sa.BigInteger(), nullable=False),
        sa.Column('ia_max', sa.Float(), nullable=False),
        sa.Column('ia_mean', sa.Float(), nullable=False),
        sa.Column('stats', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ['annotation_set_id'], ['annotation_set.id'], ondelete='RESTRICT'
        ),
        sa.ForeignKeyConstraint(
            ['ontology_snapshot_id'], ['ontology_snapshot.id'], ondelete='RESTRICT'
        ),
        sa.ForeignKeyConstraint(['job_id'], ['job.id'], ondelete='SET NULL'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint(
            'ontology_snapshot_id',
            'annotation_set_id',
            'evidence_regime',
            name='uq_ia_set_snapshot_corpus_regime',
        ),
    )
    op.create_index(
        'ix_information_accretion_set_created_at',
        'information_accretion_set',
        ['created_at'],
        unique=False,
    )
    op.create_index(
        op.f('ix_information_accretion_set_ontology_snapshot_id'),
        'information_accretion_set',
        ['ontology_snapshot_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_information_accretion_set_annotation_set_id'),
        'information_accretion_set',
        ['annotation_set_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_information_accretion_set_job_id'),
        'information_accretion_set',
        ['job_id'],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        op.f('ix_information_accretion_set_job_id'),
        table_name='information_accretion_set',
    )
    op.drop_index(
        op.f('ix_information_accretion_set_annotation_set_id'),
        table_name='information_accretion_set',
    )
    op.drop_index(
        op.f('ix_information_accretion_set_ontology_snapshot_id'),
        table_name='information_accretion_set',
    )
    op.drop_index(
        'ix_information_accretion_set_created_at',
        table_name='information_accretion_set',
    )
    op.drop_table('information_accretion_set')
