from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    postgres_user: str
    postgres_password: str
    postgres_db: str 

    database_url: Optional[str] = None

    log_level: str = "INFO"
    parse_schedule_minutes: int = 5

    def model_post_init(self, __context):
        self.database_url = f'postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@db:5432/{self.postgres_db}'


settings = Settings()