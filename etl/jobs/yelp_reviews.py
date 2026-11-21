import argparse
import json
import logging
import os
import re
from datetime import datetime, timezone

# StorageLevel controls how a DataFrame is cached (memory, spilling to disk).
from pyspark import StorageLevel
from pyspark.sql import SparkSession

from etl.schemas.yelp import YELP_RAW_SCHEMA
from etl.transforms.yelp import transform_yelp_reviews
from etl.writers.postgres import to_psycopg2_dsn, write_reviews

# Plain stdlib logging: the ETL deliberately doesn't depend on shared/,
# so it can be shipped to Glue on its own. Glue forwards this to CloudWatch.
logger = logging.getLogger("etl.yelp_reviews")

# The ingestion layer writes raw/{source}/{YYYY-MM-DD}/data.jsonl. Anchoring to
# "/raw/<source>/" means a date elsewhere in the path (a bucket named
# "2025-01-01", say) can't be mistaken for the partition date.
PARTITION_DATE = re.compile(r"/raw/[^/]+/(\d{4}-\d{2}-\d{2})/")


def scraped_at_from_path(path: str) -> datetime:
    """Midnight UTC of the partition date in an S3 path.

    The raw records carry no scrape time, so the partition date is the best
    we have. It's day-accurate, not exact. Fine for "reviews from the last
    7 days" queries, but not for ordering within a day.
    """
    match = PARTITION_DATE.search(path)
    if match is None:
        raise ValueError(f"No YYYY-MM-DD partition found in path: {path}")
    # strptime also rejects impossible dates like 2026-13-45.
    return datetime.strptime(match.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load one day of raw Yelp reviews into Postgres")
    parser.add_argument(
        "--input-path",
        required=True,
        help="e.g. s3://bucket/raw/yelp_api/2026-10-01/data.jsonl (a local path also works)",
    )
    # Falls back to the environment so the password needn't appear on a command line.
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))

    # parse_known_args ignores extra arguments: Glue passes its own (--JOB_NAME etc.).
    args, _ = parser.parse_known_args(argv)
    if not args.database_url:
        parser.error("--database-url or the DATABASE_URL environment variable is required")
    return args


def run(spark: SparkSession, input_path: str, dsn: str) -> dict[str, int]:
    """Read, transform and write one partition. Returns row counts for the logs."""
    scraped_at = scraped_at_from_path(input_path)

    # With an explicit schema, a malformed line becomes a row of NULLs, which
    # the transform's required-column check drops. A missing file raises.
    raw = spark.read.schema(YELP_RAW_SCHEMA).json(input_path)

    # Persist so the transform runs once, even though we count the result
    # and then write it (separate Spark actions).
    transformed = transform_yelp_reviews(raw, scraped_at).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        rows_read = raw.count()
        rows_valid = transformed.count()
        rows_inserted = write_reviews(transformed, dsn)
    finally:
        transformed.unpersist()

    counts = {
        "rows_read": rows_read,
        # Invalid (missing required fields) or duplicated within the file.
        "rows_dropped": rows_read - rows_valid,
        "rows_valid": rows_valid,
        "rows_inserted": rows_inserted,
        # Already in Postgres. Approximate: see write_reviews on task retries.
        "rows_skipped_existing": rows_valid - rows_inserted,
    }
    # The DSN is never logged: it contains the password.
    # JSON so CloudWatch Logs Insights can parse the counts into fields.
    logger.info("job complete input=%s counts=%s", input_path, json.dumps(counts))
    return counts


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    args = parse_args(argv)

    spark = SparkSession.builder.appName("yelp-reviews-etl").getOrCreate()
    # Set explicitly: on Glue a session may already exist,
    # and the transform and writer both assume UTC.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        run(spark, args.input_path, to_psycopg2_dsn(args.database_url))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
