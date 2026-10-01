import json
from functools import lru_cache
import boto3
from botocore.exceptions import ClientError
from shared.config.settings import get_settings
from shared.logging.logger import get_logger


logger = get_logger(__name__)


@lru_cache
def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=get_settings().aws_access_key_id,
        aws_secret_access_key=get_settings().aws_secret_access_key,
        region_name=get_settings().aws_region,
    )


class S3Writer:
    def __init__(self):
        self.bucket_name = get_settings().s3_bucket_name
        self.client = get_s3_client()

    def write_json(self, data: dict, s3_key: str) -> None:
        try:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(data, ensure_ascii=False),
                ContentType="application/json",
            )
            logger.info("wrote object to s3", bucket=self.bucket_name, key=s3_key)
        except ClientError as e:
            logger.error("failed to write to s3", bucket=self.bucket_name, key=s3_key, error=str(e))
            raise

    def write_jsonl(self, records: list[dict], s3_key: str) -> None:
        # Serialise each dict to a compact JSON string, one per line.
        # JSONL (newline-delimited JSON) is the standard format for PySpark ingestion —
        # each line is a self-contained JSON object, so Spark can split the file
        # across executors and parallelise reads without parsing the whole file first.
        jsonl_body = "\n".join(json.dumps(record, ensure_ascii=False) for record in records)

        try:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                # Encode to bytes — boto3's put_object requires a bytes-like body
                Body=jsonl_body.encode("utf-8"),
                ContentType="application/x-ndjson",  # MIME type for JSONL
            )
            # Structlog keyword args — these become structured fields in JSON output
            logger.info(
                "wrote jsonl to s3",
                bucket=self.bucket_name,
                key=s3_key,
                record_count=len(records),
            )
        except ClientError as e:
            logger.error(
                "failed to write jsonl to s3",
                bucket=self.bucket_name,
                key=s3_key,
                error=str(e),
            )
            raise

    def read_json(self, s3_key: str) -> dict:
        try:
            response = self.client.get_object(
                Bucket=self.bucket_name,
                Key=s3_key,
            )
            data = json.loads(response["Body"].read().decode("utf-8"))
            logger.info("read object from s3", bucket=self.bucket_name, key=s3_key)
            return data
        except ClientError as e:
            logger.error("failed to read from s3", bucket=self.bucket_name, key=s3_key, error=str(e))
            raise