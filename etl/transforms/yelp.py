from datetime import datetime

from pyspark.sql import DataFrame
# functions is conventionally imported as F: F.col, F.lit, F.trim...
from pyspark.sql import functions as F

from etl.schemas.reviews import REQUIRED_COLUMNS, REVIEWS_COLUMNS

# The source tag stored on every row. Constant here because this transform
# only handles Yelp; other sources will get their own transform module.
SOURCE = "yelp_api"

# Spark's pattern syntax (not Python's strftime), for "2026-10-01 12:34:56".
TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


def transform_yelp_reviews(raw: DataFrame, scraped_at: datetime) -> DataFrame:
    """Convert raw Yelp review rows into rows matching the reviews table.

    Args:
        raw: DataFrame read with YELP_RAW_SCHEMA.
        scraped_at: timezone-aware UTC time stamped on every row.
    """
    # Fail fast on a naive datetime: Spark would silently interpret it in the
    # machine's local timezone, giving different results locally and on Glue.
    if scraped_at.tzinfo is None:
        raise ValueError("scraped_at must be timezone-aware (UTC)")

    # Rename raw fields to table columns. Yelp's "id" is the review's own ID,
    # so it becomes review_id. url and user are dropped, since the table has no use for them.
    mapped = raw.select(
        F.lit(SOURCE).alias("source"),
        F.trim(F.col("business_id")).alias("business_id"),
        F.trim(F.col("business_name")).alias("business_name"),
        F.trim(F.col("id")).alias("review_id"),
        F.trim(F.col("text")).alias("text"),
        F.col("rating").cast("double").alias("rating"),
        # A string that doesn't match the format becomes NULL, not an error.
        # review_date is nullable, so a bad timestamp never loses the review.
        F.to_timestamp(F.col("time_created"), TIMESTAMP_FORMAT).alias("review_date"),
        F.lit(scraped_at).alias("scraped_at"),
    )

    # dropna removes rows where any required column is NULL.
    valid = mapped.dropna(subset=REQUIRED_COLUMNS)

    # A whitespace-only value becomes "" after trim, which would pass the
    # NOT NULL check but is still junk, so filter those out as well.
    for column in REQUIRED_COLUMNS:
        valid = valid.filter(F.col(column) != "")

    # Keep one row per (source, review_id), matching the table's unique constraint.
    deduped = valid.dropDuplicates(["source", "review_id"])

    # uuid() is Spark SQL's random UUID generator, one per row. It runs after
    # the filtering above so we don't generate IDs for rows we throw away.
    return deduped.withColumn("id", F.expr("uuid()")).select(*REVIEWS_COLUMNS)