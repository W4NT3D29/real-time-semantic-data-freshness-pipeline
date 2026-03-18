"""Configuration management using Pydantic."""

import logging
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class Config(BaseSettings):
    """Application configuration from environment variables."""

    # PostgreSQL
    postgres_host: str = Field(default="postgres", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="stock_news_db", alias="POSTGRES_DB")
    postgres_user: str = Field(default="pipeline_user", alias="POSTGRES_USER")
    postgres_password: str = Field(default="pipeline_secret", alias="POSTGRES_PASSWORD")

    # Kafka / Redpanda
    kafka_brokers: str = Field(default="redpanda:29092", alias="KAFKA_BROKERS")
    schema_registry_url: str = Field(default="http://redpanda:8081", alias="SCHEMA_REGISTRY_URL")
    kafka_consumer_group: str = Field(default="stock-news-processor", alias="KAFKA_CONSUMER_GROUP")
    kafka_topic: str = Field(default="stock_news_events", alias="KAFKA_TOPIC")
    kafka_auto_offset_reset: str = Field(default="earliest", alias="KAFKA_AUTO_OFFSET_RESET")

    # Pinecone Vector Database
    pinecone_api_key: Optional[str] = Field(default=None, alias="PINECONE_API_KEY")
    pinecone_index_name: str = Field(default="stock-news", alias="PINECONE_INDEX_NAME")
    pinecone_dimension: int = Field(default=1536, alias="PINECONE_DIMENSION")
    pinecone_metric: str = Field(default="cosine", alias="PINECONE_METRIC")

    # OpenAI Embeddings
    openai_api_key: Optional[str] = Field(default=None, alias="OPENAI_API_KEY")
    openai_model: str = Field(default="text-embedding-3-small", alias="OPENAI_MODEL")
    openai_batch_size: int = Field(default=50, alias="OPENAI_BATCH_SIZE")
    openai_rate_limit_rpm: int = Field(default=3000, alias="OPENAI_RATE_LIMIT_RPM")

    # Local Embeddings (Ollama)
    use_local_embeddings: bool = Field(default=False, alias="USE_LOCAL_EMBEDDINGS")
    ollama_api_url: str = Field(default="http://ollama:11434", alias="OLLAMA_API_URL")
    ollama_model: str = Field(default="aroxima/gte-qwen2-1.5b-instruct:q4_k_m", alias="OLLAMA_MODEL")

    # Application
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    environment: str = Field(default="development", alias="ENVIRONMENT")

    # Stream Processor
    batch_size: int = Field(default=100, alias="BATCH_SIZE")
    batch_timeout_seconds: float = Field(default=5.0, alias="BATCH_TIMEOUT_SECONDS")
    embedding_retry_max_attempts: int = Field(default=3, alias="EMBEDDING_RETRY_MAX_ATTEMPTS")
    embedding_retry_backoff_base: float = Field(default=2.0, alias="EMBEDDING_RETRY_BACKOFF_BASE")
    graceful_shutdown_timeout: int = Field(default=30, alias="GRACEFUL_SHUTDOWN_TIMEOUT")

    # Monitoring
    prometheus_port: int = Field(default=9090, alias="PROMETHEUS_PORT")
    prometheus_enabled: bool = Field(default=False, alias="PROMETHEUS_ENABLED")

    # Debezium
    debezium_db_hostname: str = Field(default="postgres", alias="DEBEZIUM_DB_HOSTNAME")
    debezium_db_port: int = Field(default=5432, alias="DEBEZIUM_DB_PORT")
    debezium_db_user: str = Field(default="pipeline_user", alias="DEBEZIUM_DB_USER")
    debezium_db_password: str = Field(default="pipeline_secret", alias="DEBEZIUM_DB_PASSWORD")
    debezium_db_name: str = Field(default="stock_news_db", alias="DEBEZIUM_DB_NAME")

    class Config:
        """Pydantic config."""
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"

    @property
    def postgres_dsn(self) -> str:
        """PostgreSQL connection string."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def kafka_brokers_list(self) -> list[str]:
        """Parse Kafka brokers into a list."""
        return [b.strip() for b in self.kafka_brokers.split(",")]

    def __repr__(self) -> str:
        """Safe string representation (no secrets)."""
        data = self.model_dump()
        # Mask sensitive values
        if data.get("openai_api_key"):
            data["openai_api_key"] = "***MASKED***"
        if data.get("pinecone_api_key"):
            data["pinecone_api_key"] = "***MASKED***"
        if data.get("postgres_password"):
            data["postgres_password"] = "***MASKED***"
        if data.get("debezium_db_password"):
            data["debezium_db_password"] = "***MASKED***"
        return f"Config({data})"
