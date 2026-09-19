from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    service_name: str = "seat-inventory-service"
    database_url: str = "postgresql+psycopg://gohub:gohub@postgres:5432/seat_inventory"
    redis_url: str = "redis://redis:6379/0"
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic: str = "seat.events"
    enable_kafka: bool = True
    seed_demo_data: bool = True
    hold_ttl_seconds: int = 480
    idempotency_ttl_seconds: int = 600
    expiry_poll_seconds: float = 1.0
    log_level: str = "INFO"
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache
def get_settings() -> Settings:
    return Settings()
