# StructType/StructField describe a DataFrame's columns and their types.
# Each type class (StringType, DoubleType...) is Spark's equivalent of a Python/SQL type.
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# Yelp nests reviewer details under a "user" key. We declare it as its own
# StructType so it can be embedded in the main schema below.
# nullable=True everywhere: raw data is untrusted, so any field may be absent.
YELP_USER_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("profile_url", StringType(), True),
    StructField("image_url", StringType(), True),
])

# Schema for one line of raw/yelp_api/{date}/data.jsonl.
YELP_RAW_SCHEMA = StructType([
    # --- Fields returned by the Yelp Fusion reviews endpoint ---
    StructField("id", StringType(), True),            # becomes reviews.review_id
    StructField("url", StringType(), True),
    StructField("text", StringType(), True),          # becomes reviews.text
    StructField("rating", DoubleType(), True),        # Yelp sends 1-5 as a whole number
    # Yelp sends "2026-10-01 12:34:56" as a plain string with no timezone.
    # We keep it as a string here and parse it in the transform step,
    # so the schema describes the file exactly as written.
    StructField("time_created", StringType(), True),
    StructField("user", YELP_USER_SCHEMA, True),

    # --- Fields our scraper adds in _get_reviews() ---
    StructField("business_id", StringType(), True),
    StructField("business_name", StringType(), True),
    StructField("source", StringType(), True),
])
