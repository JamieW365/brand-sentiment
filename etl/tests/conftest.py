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
        .master("local[1]")                                 # one thread, no cluster
        .appName("etl-tests")
        .config("spark.sql.session.timeZone", "UTC")        # matches the Glue job's assumption
        .config("spark.sql.shuffle.partitions", "1")        # tiny data, so skip the 200-partition default
        .config("spark.ui.enabled", "false")                # no web UI port needed
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
        # In CI, set REQUIRE_DB=1 so an unreachable database fails the run
        # instead of silently skipping every integration test.
        if os.environ.get("REQUIRE_DB"):
            pytest.fail("Postgres not reachable and REQUIRE_DB is set")
        pytest.skip("Postgres not reachable")
    delete_test_rows(connection)
    yield TEST_DSN
    delete_test_rows(connection)
    connection.close()
