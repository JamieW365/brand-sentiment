import os

import psycopg2
import pytest
from pyspark.sql import SparkSession

from etl.tests.db_helpers import TEST_DSN, delete_test_rows


@pytest.fixture(scope="session")
def spark():
    """One local Spark session shared by every test (starting Spark is slow)."""
    session = (
        SparkSession.builder
        # One thread, no cluster.
        .master("local[1]")
        .appName("etl-tests")
        # The transform and writer both assume UTC.
        .config("spark.sql.session.timeZone", "UTC")
        # Tiny data, so skip the 200-partition default.
        .config("spark.sql.shuffle.partitions", "1")
        # No web UI port needed.
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def dsn():
    """A DSN for a reachable Postgres, with test rows cleaned before and after."""
    try:
        connection = psycopg2.connect(TEST_DSN)
    except psycopg2.OperationalError:
        # Only REQUIRE_DB=1 turns the skip into a failure; "0" or "" must not.
        if os.environ.get("REQUIRE_DB") == "1":
            pytest.fail("Postgres not reachable and REQUIRE_DB=1")
        pytest.skip("Postgres not reachable")
    delete_test_rows(connection)
    yield TEST_DSN
    delete_test_rows(connection)
    connection.close()
