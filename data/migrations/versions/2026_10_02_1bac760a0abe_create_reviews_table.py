"""create reviews table

Revision ID: 1bac760a0abe
Revises: 
Create Date: 2026-10-02 17:16:05.304189+00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '1bac760a0abe'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table('reviews',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('source', sa.String(length=50), nullable=False),
    sa.Column('business_id', sa.String(length=255), nullable=False),
    sa.Column('business_name', sa.String(length=255), nullable=False),
    sa.Column('review_id', sa.String(length=255), nullable=False),
    sa.Column('text', sa.Text(), nullable=False),
    sa.Column('rating', sa.Float(), nullable=True),
    sa.Column('review_date', sa.DateTime(timezone=True), nullable=True),
    sa.Column('scraped_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('source', 'review_id', name='uq_reviews_source_review_id')
    )
    op.create_index('ix_reviews_business_id', 'reviews', ['business_id'], unique=False)
    op.create_index('ix_reviews_scraped_at', 'reviews', ['scraped_at'], unique=False)
    op.create_index('ix_reviews_source', 'reviews', ['source'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index('ix_reviews_source', table_name='reviews')
    op.drop_index('ix_reviews_scraped_at', table_name='reviews')
    op.drop_index('ix_reviews_business_id', table_name='reviews')
    op.drop_table('reviews')
