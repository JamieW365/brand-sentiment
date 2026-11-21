import json
from datetime import datetime, timezone

import pytest
from pyspark.errors import AnalysisException

from etl.jobs.yelp_reviews import parse_args, run, scraped_at_from_path
from etl.tests.db_helpers import PREFIX, fetch_rows


def write_jsonl(directory, lines: list[str]) -> str:
    """Write lines into a dated folder like the ingestion layer would, return the path."""
    path = directory / "raw" / "yelp_api" / "2026-10-01" / "data.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text("\n".join(lines) + "\n")
    path.write_text("".join(line + "\n" for line in lines))
    return str(path)


def review_line(i: str, **overrides) -> str:
    """One raw review as a JSON line, with any field overridable."""
    record = {
        "id": PREFIX + i,
        "text": f"review {i}",
        "rating": 4.0,
        "time_created": "2026-10-01 12:34:56",
        "business_id": "biz-1",
        "business_name": "Greggs",
        "source": "yelp_api",
    }
    return json.dumps({**record, **overrides})


class TestScrapedAtFromPath:

    @pytest.mark.parametrize(
        "path, expected",
        [
            ("s3://bucket/raw/yelp_api/2026-10-01/data.jsonl",
             datetime(2026, 10, 1, tzinfo=timezone.utc)),
            ("/tmp/x/raw/yelp_api/2025-12-31/data.jsonl",
             datetime(2025, 12, 31, tzinfo=timezone.utc)),
            # A date earlier in the path must not win over the partition date.
            ("s3://bucket/2025-01-01/raw/yelp_api/2026-10-01/data.jsonl",
             datetime(2026, 10, 1, tzinfo=timezone.utc)),
        ],
    )
    def test_parses_partition_date(self, path, expected):
        """The partition date becomes midnight UTC."""
        assert scraped_at_from_path(path) == expected

    @pytest.mark.parametrize(
        "path",
        [
            "s3://bucket/raw/yelp_api/data.jsonl",                # no date
            "s3://bucket/raw/yelp_api/2026-13-45/data.jsonl",     # impossible date
            "s3://bucket/2026-10-01/data.jsonl",                  # date not under raw/<source>/
        ],
    )
    def test_rejects_bad_paths(self, path):
        """A path without a valid date fails loudly rather than guessing."""
        with pytest.raises(ValueError):
            scraped_at_from_path(path)


class TestParseArgs:

    def test_database_url_falls_back_to_environment(self, monkeypatch):
        """The password can come from the environment instead of the command line."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h/db")
        assert parse_args(["--input-path", "x"]).database_url == "postgresql://u:p@h/db"

    def test_missing_database_url_exits(self, monkeypatch):
        """No URL anywhere is a usage error."""
        monkeypatch.delenv("DATABASE_URL", raising=False)
        with pytest.raises(SystemExit):
            parse_args(["--input-path", "x"])

    def test_unknown_glue_arguments_are_ignored(self):
        """Glue adds its own arguments; the job must not choke on them."""
        args = parse_args(
            ["--input-path", "x", "--database-url", "postgresql://u:p@h/db", "--JOB_NAME", "job"]
        )
        assert args.input_path == "x"


class TestRun:

    def test_loads_valid_rows_and_reports_counts(self, spark, dsn, tmp_path):
        """Invalid, duplicated and malformed lines are dropped and counted."""
        path = write_jsonl(tmp_path, [
            review_line("1"),
            review_line("2"),
            review_line("2"),              # duplicate within the file
            review_line("3", text=None),   # missing a required field
            "{this is not json",           # malformed line
        ])

        counts = run(spark, path, dsn)

        assert counts == {
            "rows_read": 5,
            "rows_dropped": 3,
            "rows_valid": 2,
            "rows_inserted": 2,
            "rows_skipped_existing": 0,
        }
        assert [r[0] for r in fetch_rows(dsn)] == [PREFIX + "1", PREFIX + "2"]

    def test_rerun_is_idempotent(self, spark, dsn, tmp_path):
        """Running the same file twice inserts nothing the second time."""
        path = write_jsonl(tmp_path, [review_line("1"), review_line("2")])
        run(spark, path, dsn)

        counts = run(spark, path, dsn)

        assert counts["rows_inserted"] == 0
        assert counts["rows_skipped_existing"] == 2
        assert len(fetch_rows(dsn)) == 2

    def test_scraped_at_comes_from_the_partition_date(self, spark, dsn, tmp_path):
        """Rows are stamped with midnight UTC of the date in the path."""
        run(spark, write_jsonl(tmp_path, [review_line("1")]), dsn)
        scraped_at = fetch_rows(dsn)[0][3]
        assert scraped_at == datetime(2026, 10, 1, tzinfo=timezone.utc)

    def test_missing_input_file_raises(self, spark, dsn, tmp_path):
        """A path that doesn't exist fails the job instead of loading nothing."""
        missing = str(tmp_path / "raw" / "yelp_api" / "2026-10-01" / "nope.jsonl")
        with pytest.raises(AnalysisException):
            run(spark, missing, dsn)

    def test_empty_input_file_loads_nothing(self, spark, dsn, tmp_path):
        """An empty file is a valid no-op: every count is zero."""
        counts = run(spark, write_jsonl(tmp_path, []), dsn)

        assert counts == {
            "rows_read": 0,
            "rows_dropped": 0,
            "rows_valid": 0,
            "rows_inserted": 0,
            "rows_skipped_existing": 0,
        }
