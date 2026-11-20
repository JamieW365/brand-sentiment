import pytest
import psycopg2
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
        pytest.skip("Postgres not reachable")
    delete_test_rows(connection)
    yield TEST_DSN
    delete_test_rows(connection)
    connection.close()