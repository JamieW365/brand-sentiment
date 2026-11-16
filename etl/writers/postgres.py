from collections.abc import Iterator
from itertools import islice

import psycopg2
# execute_values expands one INSERT into a multi-row VALUES list: much
# faster than one round trip per row.
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F

from etl.transforms.yelp import REVIEWS_COLUMNS

# ON CONFLICT DO NOTHING makes the insert idempotent: a (source, review_id)
# already in the table is skipped silently. The column list comes from the
# same constant the transform uses, so the two can't drift apart.
INSERT_SQL = f"""
    INSERT INTO reviews ({", ".join(REVIEWS_COLUMNS)})
    VALUES %s
    ON CONFLICT (source, review_id) DO NOTHING
"""

# Timestamp columns are sent as text with an explicit UTC offset (see below).
TIMESTAMP_COLUMNS = ["review_date", "scraped_at"]


def to_psycopg2_dsn(database_url: str) -> str:
    """Convert the app's SQLAlchemy URL into one psycopg2 understands.

    Our settings use "postgresql+asyncpg://..." for the async API layer.
    psycopg2 needs the plain "postgresql://..." form.
    """
    return database_url.replace("postgresql+asyncpg://", "postgresql://", 1)


def write_reviews(df: DataFrame,
                  dsn: str,
                  batch_size: int = 500,
                  max_connections: int = 4) -> int:
    """Insert reviews into Postgres, skipping rows that already exist.

    Returns the number of rows actually inserted.
    """
    spark = df.sparkSession

    # Timestamps are converted to text below using the Spark session timezone.
    # If that isn't UTC, we'd stamp the wrong offset on them, so refuse to run.
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Spark session timezone must be UTC")

    # Rows reach Python workers as datetimes in the *worker's* local timezone,
    # which is the trap we avoided in the tests. Sending text with an explicit
    # +00 offset means Postgres never has to guess.
    prepared = df.select(
        *[
            F.concat(F.date_format(c, "yyyy-MM-dd HH:mm:ss"), F.lit("+00")).alias(c)
            if c in TIMESTAMP_COLUMNS
            else F.col(c)
            for c in REVIEWS_COLUMNS
        ]
    )

    # An accumulator is a counter that workers add to and the driver reads back.
    inserted = spark.sparkContext.accumulator(0)

    def write_partition(rows: Iterator[Row]) -> None:
        """Runs on a worker: one connection, rows inserted in batches."""
        rows = iter(rows)
        chunk = list(islice(rows, batch_size))
        if not chunk:
            return  # empty partition, so don't open a connection for nothing

        connection = psycopg2.connect(dsn)
        try:
            # "with connection" commits at the end, or rolls back on error.
            # It does NOT close the connection, hence the finally below.
            with connection, connection.cursor() as cursor:
                while chunk:
                    values = [tuple(row[c] for c in REVIEWS_COLUMNS) for row in chunk]
                    # page_size=len(values) keeps this to one statement, so
                    # cursor.rowcount is the true number of rows inserted.
                    execute_values(cursor, INSERT_SQL, values, page_size=len(values))
                    inserted.add(cursor.rowcount)
                    chunk = list(islice(rows, batch_size))
        finally:
            connection.close()

    # coalesce reduces the partition count without a shuffle. Each partition
    # opens one connection, so this caps simultaneous connections to Postgres.
    prepared.coalesce(max_connections).foreachPartition(write_partition)

    return inserted.value