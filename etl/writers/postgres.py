import re
from collections.abc import Iterator
from itertools import islice

import psycopg2

# execute_values expands one INSERT into a multi-row VALUES list: much
# faster than one round trip per row.
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F

from etl.schemas.reviews import REVIEWS_COLUMNS

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
    """Convert a SQLAlchemy URL into a plain postgresql:// URL for psycopg2.

    Strips any "+driver" suffix: postgresql+asyncpg://, postgresql+psycopg2://, etc.
    """
    return re.sub(r"^postgresql\+\w+://", "postgresql://", database_url)


def write_reviews(df: DataFrame,
                  dsn: str,
                  batch_size: int = 500,
                  max_connections: int = 4) -> int:
    """Insert reviews into Postgres, skipping rows that already exist.

    Idempotent: a row whose (source, review_id) already exists is left
    untouched (ON CONFLICT DO NOTHING), so an edited review is NOT refreshed.
    This is deliberate: later phases store sentiment scores computed from
    `text`, and silently changing the text would leave those scores stale.

    Args:
        df: Rows shaped like REVIEWS_COLUMNS (the output of a transform).
        dsn: A plain postgresql:// connection string (see to_psycopg2_dsn).
        batch_size: Rows per INSERT statement and per commit.
        max_connections: Upper bound on concurrent database connections.

    Returns:
        Rows inserted by successful task attempts. Treat this as a lower
        bound: if Spark retries a task after some batches were committed,
        those earlier inserts are not counted.

    Requires the Spark session timezone to be UTC.
    """
    spark = df.sparkSession

    # Timestamps are converted to text below using the Spark session timezone.
    # If that isn't UTC, we'd stamp the wrong offset on them, so refuse to run.
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Spark session timezone must be UTC")

    # Spark hands Python naive datetimes in the worker's local timezone, so
    # we send the timestamps as text with an explicit +00 offset instead,
    # and Postgres never has to guess.
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
        chunk = list(islice(rows, batch_size))
        if not chunk:
            return  # empty partition, so don't open a connection for nothing

        # connect_timeout makes a Postgres outage fail the task in seconds
        # instead of hanging until the Glue job times out.
        connection = psycopg2.connect(dsn, connect_timeout=10)
        try:
            with connection.cursor() as cursor:
                while chunk:
                    values = [tuple(row[c] for c in REVIEWS_COLUMNS) for row in chunk]
                    # page_size=len(values) keeps this to one statement, so
                    # cursor.rowcount is the true number of rows inserted.
                    execute_values(cursor, INSERT_SQL, values, page_size=len(values))
                    inserted.add(cursor.rowcount)
                    # Commit per batch to bound transaction size and lock time.
                    # Safe because the insert is idempotent: a retried task
                    # just skips whatever was already committed.
                    connection.commit()
                    chunk = list(islice(rows, batch_size))
        finally:
            # An uncommitted batch is rolled back when the connection closes.
            connection.close()

    # coalesce reduces the partition count without a shuffle. Each partition
    # opens one connection, so this caps simultaneous connections to Postgres.
    prepared.coalesce(max_connections).foreachPartition(write_partition)

    return inserted.value
