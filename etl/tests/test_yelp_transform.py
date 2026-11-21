import uuid
from datetime import datetime, timezone

import pytest
from pyspark.sql import functions as F

from etl.schemas.reviews import REVIEWS_COLUMNS
from etl.schemas.yelp import YELP_RAW_SCHEMA
from etl.transforms.yelp import transform_yelp_reviews

SCRAPED_AT = datetime(2026, 10, 1, tzinfo=timezone.utc)


def make_raw(**overrides) -> dict:
    """One valid raw Yelp review row, with any field overridable."""
    row = {
        "id": "rev-1",
        "url": "https://yelp.example/rev-1",
        "text": "Great sausage rolls",
        "rating": 5.0,
        "time_created": "2026-10-01 12:34:56",
        "user": {"id": "u1", "name": "Sam", "profile_url": None, "image_url": None},
        "business_id": "biz-1",
        "business_name": "Greggs",
        "source": "yelp_api",
    }
    return {**row, **overrides}


def run_transform(spark, rows: list[dict]):
    """Build a DataFrame with the real raw schema and run the transform."""
    raw = spark.createDataFrame(rows, schema=YELP_RAW_SCHEMA)
    return transform_yelp_reviews(raw, SCRAPED_AT)


class TestMapping:

    def test_output_columns_match_reviews_table(self, spark):
        """Columns and their order match what the writer expects."""
        result = run_transform(spark, [make_raw()])
        assert result.columns == REVIEWS_COLUMNS

    def test_fields_are_mapped_and_trimmed(self, spark):
        """Yelp's id becomes review_id, strings are trimmed, source is constant."""
        row = run_transform(
            spark, [make_raw(id="  rev-9 ", text="  Lovely  ", source=None)]
        ).collect()[0]

        assert row.review_id == "rev-9"
        assert row.text == "Lovely"
        assert row.source == "yelp_api"
        assert row.business_name == "Greggs"
        assert row.rating == 5.0

    def test_review_date_is_parsed_as_utc(self, spark):
        """time_created is parsed using the UTC session timezone."""
        result = run_transform(spark, [make_raw()])
        # date_format renders in the Spark session timezone. collect() would
        # return a Python datetime in the *machine's* timezone and make this
        # test depend on where it runs.
        shown = result.select(
            F.date_format("review_date", "yyyy-MM-dd HH:mm:ss").alias("d")
        ).collect()[0].d
        assert shown == "2026-10-01 12:34:56"

    def test_unparseable_date_becomes_null_but_keeps_row(self, spark):
        """A bad timestamp loses the date, not the review."""
        result = run_transform(spark, [make_raw(time_created="yesterday-ish")])
        row = result.collect()[0]
        assert row.review_date is None
        assert row.review_id == "rev-1"

    def test_scraped_at_applied_to_every_row(self, spark):
        """All rows get the scraped_at passed in by the caller."""
        result = run_transform(spark, [make_raw(), make_raw(id="rev-2")])
        shown = {
            r.d for r in result.select(
                F.date_format("scraped_at", "yyyy-MM-dd HH:mm:ss").alias("d")
            ).collect()
        }
        assert shown == {"2026-10-01 00:00:00"}

    def test_naive_scraped_at_is_rejected(self, spark):
        """A timezone-naive scraped_at raises instead of silently shifting."""
        raw = spark.createDataFrame([make_raw()], schema=YELP_RAW_SCHEMA)
        with pytest.raises(ValueError):
            transform_yelp_reviews(raw, datetime(2026, 10, 1))


class TestCleaning:

    @pytest.mark.parametrize("column", ["id", "text", "business_id", "business_name"])
    @pytest.mark.parametrize("bad_value", [None, "", "   "])
    def test_rows_missing_required_fields_are_dropped(self, spark, column, bad_value):
        """NULL, empty and whitespace-only values in NOT NULL columns drop the row."""
        # "id" in the raw data is review_id in the output.
        rows = [make_raw(),
                make_raw(**{column: bad_value},
                         **({} if column == "id" else {"id": "rev-bad"}))]
        result = run_transform(spark, rows)
        assert [r.review_id for r in result.collect()] == ["rev-1"]

    def test_duplicates_within_a_file_are_collapsed(self, spark):
        """The same review appearing twice (e.g. via two search terms) is kept once."""
        result = run_transform(spark, [make_raw(), make_raw()])
        assert result.count() == 1

    def test_same_review_id_from_different_businesses_still_collapses(self, spark):
        """Uniqueness is (source, review_id), matching the table's constraint."""
        result = run_transform(spark, [make_raw(), make_raw(business_id="biz-2")])
        assert result.count() == 1

    def test_ids_are_unique_valid_uuids(self, spark):
        """Each output row gets its own UUID."""
        result = run_transform(spark, [make_raw(), make_raw(id="rev-2")])
        ids = [r.id for r in result.collect()]
        assert len(set(ids)) == 2
        for value in ids:
            uuid.UUID(value)  # raises ValueError if it isn't a valid UUID

    def test_empty_input_returns_empty_output(self, spark):
        """No rows in, no rows out, and the columns are still correct."""
        result = run_transform(spark, [])
        assert result.count() == 0
        assert result.columns == REVIEWS_COLUMNS
