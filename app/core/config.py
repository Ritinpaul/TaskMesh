from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql+asyncpg://taskmesh:taskmesh@localhost:5432/taskmesh"
    sync_database_url: str = "postgresql+psycopg://taskmesh:taskmesh@localhost:5432/taskmesh"

    redis_url: str = "redis://localhost:6379/0"
    task_stream_key: str = "taskmesh:tasks"
    task_consumer_group: str = "taskmesh-workers"
    task_consumer_name: str = "worker"

    worker_block_ms: int = 5000
    worker_batch_size: int = 20

    max_retry_attempts: int = 3
    retry_base_delay_ms: int = 200
    retry_max_delay_ms: int = 5000

    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout_ms: int = 10000

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
