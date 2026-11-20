import os

import psycopg2

from etl.writers.postgres import to_psycopg2_dsn

# Defaults to the Docker container from earlier; override with TEST_DATABASE_URL.
TEST_DSN = to_psycopg2_dsn(
    os.environ.get(
        "TEST_DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/brand_sentiment",
    )
)

# Every row the tests create has a review_id starting with this prefix,
# which is how cleanup finds them without touching real data.
PREFIX = "etl-test-"


def delete_test_rows(connection) -> None:
    with connection, connection.cursor() as cursor:
        cursor.execute("DELETE FROM reviews WHERE review_id LIKE %s", (PREFIX + "%",))


def fetch_rows(dsn: str) -> list[tuple]:
    """Read back every test row, ordered by review_id."""
    connection = psycopg2.connect(dsn)
    try:
        with connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT review_id, rating, review_date, scraped_at, created_at "
                "FROM reviews WHERE review_id LIKE %s ORDER BY review_id",
                (PREFIX + "%",),
            )
            return cursor.fetchall()
    finally:
        connection.close()