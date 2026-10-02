from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Text, Float, DateTime, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy import func
# uuid4 generates the primary key value. UUID is the Python type for Mapped[].
from uuid import uuid4, UUID
# datetime is the Python type used in Mapped[] annotations.
# DateTime (SQLAlchemy) is the column type — they are different things.
from datetime import datetime, timezone

# Base is the DeclarativeBase all ORM models must inherit from.
# Alembic reads Base.metadata to detect and diff schema changes.
from shared.db.base import Base


class Review(Base):
    """ORM model for the reviews table.

    Stores raw review records from all ingestion sources — Yelp, Reddit,
    App Store, News API. Sentiment scores are added in Phase 2 via nullable
    columns on this same table, populated by the NLP pipeline.
    """

    __tablename__ = "reviews"

    # Mapped[UUID] uses Python's uuid.UUID — the type checker understands this.
    # PgUUID(as_uuid=True) is the column type — tells SQLAlchemy to store as
    # a native Postgres UUID and return Python UUID objects, not strings.
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, default=uuid4
    )

    # Which scraper produced this record — used for filtering and partitioning.
    # Values match the source_name on BaseScraper: "yelp_api", "reddit", etc.
    source: Mapped[str] = mapped_column(String(50))

    # Source-specific business identifier (e.g. Yelp business ID).
    # Combined with source in the unique constraint to prevent duplicates.
    business_id: Mapped[str] = mapped_column(String(255))

    # Human-readable business name — stored at scrape time so queries
    # don't need a separate business lookup.
    business_name: Mapped[str] = mapped_column(String(255))

    # Source-specific review identifier. Combined with source in the unique
    # constraint — the same review re-scraped on a later run will not be
    # inserted again.
    review_id: Mapped[str] = mapped_column(String(255))

    # The raw review text — the primary input to the NLP pipeline in Phase 2.
    text: Mapped[str] = mapped_column(Text)

    # Star rating from the source (1–5 for Yelp, varies by source).
    # Nullable because not all sources provide a numeric rating.
    rating: Mapped[float | None] = mapped_column(Float)

    # When the review was originally written on the source platform.
    # Nullable because some sources don't expose this.
    review_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )

    # Python-side default — stamped by the application at scrape time.
    # Use a lambda so datetime.now() is called fresh per row, not once at
    # class definition time.
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    # When this row was inserted into Postgres — set by the database server.
    # Useful for auditing and distinguishing scrape time from insert time.
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        # Prevents duplicate reviews across re-scrapes
        UniqueConstraint("source", "review_id", name="uq_reviews_source_review_id"),
        # source is filtered on almost every query — dashboard views, NLP pipeline runs
        Index("ix_reviews_source", "source"),
        # business_id is used when fetching all reviews for a specific business
        Index("ix_reviews_business_id", "business_id"),
        # scraped_at supports time-range queries — e.g. "reviews from the last 7 days"
        Index("ix_reviews_scraped_at", "scraped_at"),
    )