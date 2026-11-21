# Table-level definitions shared by every source's transform and the writer.
# Kept separate so a Reddit or App Store transform doesn't depend on the Yelp one.

# The columns written to the reviews table, in order. created_at is omitted
# on purpose: Postgres fills it in with its server default.
REVIEWS_COLUMNS = [
    "id", "source", "business_id", "business_name", "review_id",
    "text", "rating", "review_date", "scraped_at",
]

# Columns that are NOT NULL in the reviews table.
REQUIRED_COLUMNS = ["business_id", "business_name", "review_id", "text"]
