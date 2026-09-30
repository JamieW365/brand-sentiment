from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache

class Settings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Database
    database_url: str

    # Redis
    redis_url: str

    # AWS
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str = "eu-west-2"
    s3_bucket_name: str

    # MLFlow
    mlflow_tracking_uri: str

    # NewsAPI
    newsapi_key: str

    # Reddit
    reddit_client_id: str
    reddit_client_secret: str
    reddit_user_agent: str

    # Yelp
    yelp_api_key: str


@lru_cache
def get_settings() -> Settings:
    return Settings()