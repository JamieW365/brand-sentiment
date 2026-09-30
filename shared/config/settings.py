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


@lru_cache  # Caches the return value of get_settings() after the first call,
            # meaning Settings() is only instantiated once across the entire application
def get_settings() -> Settings:  # factory function — callers invoke get_settings() to retrieve
                                 # the Settings instance rather than importing it directly
    return Settings()  # instantiates Settings(), triggering .env validation only on first call