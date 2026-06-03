"""reranker_model_artifact_columns

Revision ID: c517e16da06b
Revises: 7a2c9e1d0b33
Create Date: 2026-04-21 02:57:27.951747

"""
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'c517e16da06b'
down_revision: str | Sequence[str] | None = '7a2c9e1d0b33'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('reranker_model', sa.Column('artifact_uri', sa.String(length=512), nullable=True))
    op.add_column('reranker_model', sa.Column('feature_schema_sha', sa.String(length=16), nullable=True))
    op.add_column('reranker_model', sa.Column('embedding_config_id', sa.UUID(), nullable=True))
    op.add_column('reranker_model', sa.Column('ontology_snapshot_id', sa.UUID(), nullable=True))
    op.add_column('reranker_model', sa.Column('producer_version', sa.String(length=64), nullable=True))
    op.add_column('reranker_model', sa.Column('producer_git_sha', sa.String(length=40), nullable=True))
    op.add_column('reranker_model', sa.Column('spec_yaml', sa.Text(), nullable=True))
    # model_data goes nullable so new rows can live exclusively by reference
    # (artifact_uri). Downgrade restores NOT NULL — will fail loudly if any
    # row has a NULL model_data, which is the correct behavior.
    op.alter_column(
        'reranker_model', 'model_data',
        existing_type=sa.TEXT(),
        nullable=True,
    )
    op.create_index(
        op.f('ix_reranker_model_embedding_config_id'),
        'reranker_model', ['embedding_config_id'], unique=False,
    )
    op.create_index(
        op.f('ix_reranker_model_ontology_snapshot_id'),
        'reranker_model', ['ontology_snapshot_id'], unique=False,
    )
    op.create_foreign_key(
        'fk_reranker_model_ontology_snapshot_id',
        'reranker_model', 'ontology_snapshot',
        ['ontology_snapshot_id'], ['id'], ondelete='SET NULL',
    )
    op.create_foreign_key(
        'fk_reranker_model_embedding_config_id',
        'reranker_model', 'embedding_config',
        ['embedding_config_id'], ['id'], ondelete='SET NULL',
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        'fk_reranker_model_embedding_config_id',
        'reranker_model', type_='foreignkey',
    )
    op.drop_constraint(
        'fk_reranker_model_ontology_snapshot_id',
        'reranker_model', type_='foreignkey',
    )
    op.drop_index(op.f('ix_reranker_model_ontology_snapshot_id'), table_name='reranker_model')
    op.drop_index(op.f('ix_reranker_model_embedding_config_id'), table_name='reranker_model')
    op.alter_column(
        'reranker_model', 'model_data',
        existing_type=sa.TEXT(),
        nullable=False,
    )
    op.drop_column('reranker_model', 'spec_yaml')
    op.drop_column('reranker_model', 'producer_git_sha')
    op.drop_column('reranker_model', 'producer_version')
    op.drop_column('reranker_model', 'ontology_snapshot_id')
    op.drop_column('reranker_model', 'embedding_config_id')
    op.drop_column('reranker_model', 'feature_schema_sha')
    op.drop_column('reranker_model', 'artifact_uri')
