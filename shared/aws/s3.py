import json
from functools import lru_cache
import boto3
from botocore.exceptions import ClientError
from shared.config.settings import get_settings  # import factory function, not module-level instance
from shared.logging.logger import get_logger


# Initialize module logging
logger = get_logger(__name__)


@lru_cache  # ensures the boto3 client is created once and reused across all S3Writer instances
def get_s3_client():  # factory function — returns a single shared authenticated boto3 S3 client
    return boto3.client(
        "s3",
        aws_access_key_id=get_settings().aws_access_key_id,       # AWS access key from settings
        aws_secret_access_key=get_settings().aws_secret_access_key, # AWS secret key from settings
        region_name=get_settings().aws_region,                      # AWS region from settings
    )


class S3Writer:
    def __init__(self):
        self.bucket_name = get_settings().s3_bucket_name  # retrieves bucket name from cached settings
        self.client = get_s3_client()  # retrieves the cached boto3 client rather than creating a new one

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