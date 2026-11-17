import pytest
from pyspark.sql import SparkSession


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