from datetime import datetime, timezone

import psycopg2
import pytest
from pyspark.sql import SparkSession

from etl.schemas.yelp import YELP_RAW_SCHEMA
from etl.transforms.yelp import transform_yelp_reviews
from etl.writers.postgres import to_psycopg2_dsn, write_reviews
from etl.schemas.reviews import REVIEWS_COLUMNS
from etl.tests.db_helpers import PREFIX, fetch_rows

SCRAPED_AT = datetime(2026, 10, 1, tzinfo=timezone.utc)

# Every row these tests create has a review_id starting with this prefix,
# which is how cleanup finds them without touching real data.
PREFIX = "etl-test-"


def reviews_df(spark: SparkSession, *ids: str, time_created: str = "2026-10-01 12:34:56", rating=4.0):
    """A transformed DataFrame with one review per id, built via the real transform."""
    rows = [
        {
            "id": PREFIX + i,
            "text": f"review {i}",
            "rating": 4.0,
            "time_created": time_created,
            "business_id": "biz-1",
            "business_name": "Greggs",
            "rating": rating,
        }
        for i in ids
    ]
    # Keys missing from the dicts (url, user, source) simply become NULL.
    raw = spark.createDataFrame(rows, schema=YELP_RAW_SCHEMA)
    return transform_yelp_reviews(raw, SCRAPED_AT)


class TestWriteReviews:

    def test_inserts_rows_and_returns_count(self, spark, dsn):
        """New reviews are inserted and the return value counts them."""
        assert write_reviews(reviews_df(spark, "1", "2"), dsn) == 2
        assert [r[0] for r in fetch_rows(dsn)] == [PREFIX + "1", PREFIX + "2"]

    def test_rerun_inserts_nothing(self, spark, dsn):
        """Writing the same reviews again is a no-op, not an error."""
        write_reviews(reviews_df(spark, "1", "2"), dsn)
        # The transform generates fresh UUIDs on every run; the skip still
        # works because the conflict is on (source, review_id), not on id.
        assert write_reviews(reviews_df(spark, "1", "2"), dsn) == 0
        assert len(fetch_rows(dsn)) == 2

    def test_mixed_new_and_existing(self, spark, dsn):
        """Only the genuinely new reviews in an overlapping batch are inserted."""
        write_reviews(reviews_df(spark, "1", "2"), dsn)
        assert write_reviews(reviews_df(spark, "2", "3"), dsn) == 1
        assert len(fetch_rows(dsn)) == 3

    def test_empty_dataframe_writes_nothing(self, spark, dsn):
        """An empty DataFrame returns 0 and doesn't error."""
        assert write_reviews(reviews_df(spark), dsn) == 0
        assert fetch_rows(dsn) == []


class TestStoredValues:

    def test_timestamps_are_stored_as_utc(self, spark, dsn):
        """review_date and scraped_at land in Postgres at the right instant."""
        write_reviews(reviews_df(spark, "1"), dsn)
        _, rating, review_date, scraped_at, created_at = fetch_rows(dsn)[0]

        # Aware datetimes compare by instant, so this holds whatever
        # timezone the database connection happens to use.
        assert review_date == datetime(2026, 10, 1, 12, 34, 56, tzinfo=timezone.utc)
        assert scraped_at == SCRAPED_AT
        assert rating == 4.0
        assert created_at is not None  # filled by the server default

    def test_unparseable_date_is_stored_as_null(self, spark, dsn):
        """A review with a bad timestamp is still stored, with review_date NULL."""
        write_reviews(reviews_df(spark, "1", time_created="not-a-date"), dsn)
        assert fetch_rows(dsn)[0][2] is None


class TestGuards:

    def test_non_utc_session_is_rejected(self, spark):
        """The writer refuses to run if timestamps would get the wrong offset."""
        df = reviews_df(spark, "1")
        spark.conf.set("spark.sql.session.timeZone", "Europe/London")
        try:
            # The check happens before any connection, so no DSN is needed.
            with pytest.raises(ValueError):
                write_reviews(df, "unused")
        finally:
            spark.conf.set("spark.sql.session.timeZone", "UTC")


class TestDsn:

    @pytest.mark.parametrize(
        "url, expected",
        [
            ("postgresql+asyncpg://u:p@h:5432/db", "postgresql://u:p@h:5432/db"),
            ("postgresql+psycopg2://u:p@h/db", "postgresql://u:p@h/db"),
            ("postgresql://u:p@h/db", "postgresql://u:p@h/db"),
        ],
    )
    def test_driver_suffix_is_stripped(self, url, expected):
        """Any SQLAlchemy driver suffix is removed; plain URLs pass through."""
        assert to_psycopg2_dsn(url) == expected


class TestModelContract:

    def test_columns_match_the_review_model(self):
        """The writer's column list can't drift from the ORM model."""
        from shared.db.models import Review

        # created_at is filled by the database, so the writer never sends it.
        model_columns = {c.name for c in Review.__table__.columns} - {"created_at"}
        assert set(REVIEWS_COLUMNS) == model_columns


class TestNullableRoundTrip:

    def test_missing_rating_is_stored_as_null(self, spark, dsn):
        """A review without a rating is stored with rating NULL."""
        write_reviews(reviews_df(spark, "1", rating=None), dsn)
        assert fetch_rows(dsn)[0][1] is None