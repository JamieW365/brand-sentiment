import json
import boto3
from botocore.exceptions import ClientError
from shared.config.settings import settings
from shared.logging.logger import get_logger


# Initialize module logging
logger = get_logger(__name__)


class S3Writer:
    def __init__(self):
        self.bucket_name = settings.s3_bucket_name
        self.client = boto3.client(
            "s3",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
        )

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