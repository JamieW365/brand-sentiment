from datetime import datetime, timezone  # For constructing the dated S3 partition key

from shared.aws.s3 import S3Writer as SharedS3Writer  # The shared boto3 wrapper
from shared.logging.logger import get_logger          # Shared structlog logger


class S3Writer:
    """Ingestion-specific S3 writer.

    Wraps shared.aws.s3.S3Writer to add:
    - Dated partition path construction (raw/{source}/{YYYY-MM-DD}/data.jsonl)
    - JSONL serialisation via write_jsonl()

    Keeping path logic here (not in shared/) means the shared S3Writer stays
    generic and reusable by the API, ETL, and any future service.
    """

    def __init__(self, source_name: str) -> None:
        # Source identifier used to build the S3 partition prefix.
        # Must match the source_name on BaseScraper for path consistency.
        self.source_name = source_name

        # Instantiate the shared writer — bucket name and boto3 client
        # are resolved internally from settings, so nothing is hardcoded here.
        self.s3 = SharedS3Writer()

        self.logger = get_logger(__name__)

    def write(self, records: list[dict]) -> str:
        """Write a batch of records to the dated S3 partition as JSONL.

        Returns the S3 key written so the calling Airflow task can log it
        or pass it downstream as an XCom value into the Glue ETL task.
        """
        # UTC throughout — avoids ambiguity when the job runs near midnight
        # or across machines in different timezones.
        # Example key: raw/yelp_api/2026-10-01/data.jsonl
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        s3_key = f"raw/{self.source_name}/{date_str}/data.jsonl"

        # Delegate serialisation and upload to the shared writer
        self.s3.write_jsonl(records=records, s3_key=s3_key)

        self.logger.info(
            "ingestion write complete",
            source=self.source_name,
            s3_key=s3_key,
            record_count=len(records),
        )

        # Return the key — Airflow XCom can capture this to pass the exact
        # S3 path into the downstream Glue ETL task without hardcoding it.
        return s3_key