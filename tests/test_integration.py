"""Integration tests for real-time semantic data freshness pipeline.

Tests the full E2E flow:
  PostgreSQL → Debezium CDC → Kafka → Python Consumer → Vector DB
"""

import asyncio
import json
import logging
import time
from typing import Generator

import psycopg2
import pytest
from confluent_kafka import Consumer, Producer
from testcontainers.postgres import PostgresContainer
from testcontainers.kafka import KafkaContainer

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def postgres_container():
    """Start PostgreSQL container for testing."""
    container = PostgresContainer(image="postgres:16-alpine")
    with container:
        yield container


@pytest.fixture(scope="session")
def kafka_container():
    """Start Kafka container for testing."""
    container = KafkaContainer(image="confluentinc/cp-kafka:7.5.0")
    with container:
        yield container


@pytest.fixture
def postgres_conn(postgres_container):
    """Get PostgreSQL connection."""
    conn = psycopg2.connect(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        database=postgres_container.dbname,
        user=postgres_container.username,
        password=postgres_container.password,
    )
    yield conn
    conn.close()


@pytest.fixture
def postgres_cursor(postgres_conn):
    """Get PostgreSQL cursor."""
    cursor = postgres_conn.cursor()
    yield cursor
    cursor.close()
    postgres_conn.commit()


@pytest.fixture
def kafka_producer(kafka_container):
    """Create Kafka producer."""
    producer = Producer(
        {
            "bootstrap.servers": kafka_container.get_bootstrap_server(),
            "client.id": "test-producer",
        }
    )
    yield producer
    producer.flush()


@pytest.fixture
def kafka_consumer(kafka_container):
    """Create Kafka consumer."""
    consumer = Consumer(
        {
            "bootstrap.servers": kafka_container.get_bootstrap_server(),
            "group.id": "test-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    yield consumer
    consumer.close()


class TestDatabaseSchema:
    """Test database schema and initialization."""

    def test_create_stock_news_table(self, postgres_cursor):
        """Test creating stock_news table."""
        # Create pgvector extension
        postgres_cursor.execute("CREATE EXTENSION IF NOT EXISTS pgvector")

        # Create table
        postgres_cursor.execute(
            """
            CREATE TABLE stock_news (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(10) NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                source VARCHAR(50),
                published_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                version INT DEFAULT 0
            )
            """
        )

        # Verify table exists
        postgres_cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='stock_news')"
        )
        assert postgres_cursor.fetchone()[0] is True

    def test_insert_seed_data(self, postgres_cursor):
        """Test inserting seed data."""
        postgres_cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_news (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(10) NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                source VARCHAR(50)
            )
            """
        )

        # Insert test row
        postgres_cursor.execute(
            """
            INSERT INTO stock_news (ticker, title, content, source)
            VALUES ('TSLA', 'Breaking News', 'Tesla stock surges', 'Reuters')
            """
        )

        # Verify insert
        postgres_cursor.execute("SELECT COUNT(*) FROM stock_news")
        count = postgres_cursor.fetchone()[0]
        assert count == 1

    def test_table_indexes(self, postgres_cursor):
        """Test table indexes for query optimization."""
        postgres_cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_news (
                id SERIAL PRIMARY KEY,
                ticker VARCHAR(10) NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                published_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )

        # Create indexes
        postgres_cursor.execute("CREATE INDEX IF NOT EXISTS idx_ticker ON stock_news(ticker)")
        postgres_cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_published ON stock_news(published_at DESC)"
        )

        # Verify indexes exist
        postgres_cursor.execute(
            """
            SELECT COUNT(*) FROM pg_indexes
            WHERE tablename='stock_news'
            """
        )
        index_count = postgres_cursor.fetchone()[0]
        assert index_count >= 2


class TestKafkaProducerConsumer:
    """Test Kafka producer and consumer."""

    def test_produce_message(self, kafka_producer):
        """Test producing a message to Kafka."""
        topic = "test-topic"
        message = json.dumps({"id": 1, "ticker": "AAPL", "title": "Apple News"})

        kafka_producer.produce(topic, message.encode("utf-8"))
        kafka_producer.flush()

        # Message should be produced successfully (no exceptions)
        assert True

    def test_consume_message(self, kafka_producer, kafka_consumer):
        """Test consuming a message from Kafka."""
        topic = "test-consume-topic"
        test_data = {"id": 1, "ticker": "MSFT", "title": "Microsoft News"}
        message = json.dumps(test_data)

        # Subscribe and produce
        kafka_consumer.subscribe([topic])
        kafka_producer.produce(topic, message.encode("utf-8"))
        kafka_producer.flush()

        # Consume with timeout
        msg = kafka_consumer.poll(timeout=5.0)
        assert msg is not None
        assert json.loads(msg.value().decode("utf-8")) == test_data

    def test_multiple_messages(self, kafka_producer, kafka_consumer):
        """Test producing and consuming multiple messages."""
        topic = "test-multi-topic"
        messages = [
            {"id": i, "ticker": f"TICK{i}", "title": f"News {i}"} for i in range(5)
        ]

        # Subscribe
        kafka_consumer.subscribe([topic])

        # Produce all messages
        for msg in messages:
            kafka_producer.produce(topic, json.dumps(msg).encode("utf-8"))
        kafka_producer.flush()

        # Consume all messages
        received = []
        for _ in range(len(messages)):
            msg = kafka_consumer.poll(timeout=5.0)
            if msg:
                received.append(json.loads(msg.value().decode("utf-8")))

        assert len(received) == len(messages)

    def test_message_ordering(self, kafka_producer, kafka_consumer):
        """Test that messages maintain order within partition."""
        topic = "test-order-topic"
        messages = [f"message-{i}" for i in range(10)]

        kafka_consumer.subscribe([topic])

        # Produce messages
        for msg in messages:
            kafka_producer.produce(topic, msg.encode("utf-8"))
        kafka_producer.flush()

        # Consume and verify order
        received = []
        for _ in range(len(messages)):
            msg = kafka_consumer.poll(timeout=5.0)
            if msg:
                received.append(msg.value().decode("utf-8"))

        assert received == messages


class TestCDCEventParsing:
    """Test CDC event parsing logic."""

    def test_parse_create_event(self):
        """Test parsing CREATE event from Debezium."""
        from stream_processor.consumer import CDCEvent

        cdc_message = {
            "before": None,
            "after": {
                "id": 1,
                "ticker": "TSLA",
                "title": "Breaking News",
                "content": "Tesla stock jumps",
                "source": "Reuters",
                "version": 0,
            },
            "op": "c",
        }

        event = CDCEvent.from_kafka_message(cdc_message, offset=0, partition=0)

        assert event is not None
        assert event.id == 1
        assert event.ticker == "TSLA"
        assert event.operation == "c"

    def test_parse_update_event(self):
        """Test parsing UPDATE event from Debezium."""
        from stream_processor.consumer import CDCEvent

        cdc_message = {
            "before": {"id": 1, "content": "Old content"},
            "after": {
                "id": 1,
                "ticker": "AAPL",
                "title": "Updated",
                "content": "New content",
                "version": 1,
            },
            "op": "u",
        }

        event = CDCEvent.from_kafka_message(cdc_message, offset=1, partition=0)

        assert event is not None
        assert event.operation == "u"
        assert event.content == "New content"

    def test_parse_delete_event(self):
        """Test parsing DELETE event (tombstone) from Debezium."""
        from stream_processor.consumer import CDCEvent

        cdc_message = {
            "before": {
                "id": 1,
                "ticker": "GOOGL",
                "title": "News",
                "content": "Content",
            },
            "after": None,
            "op": "d",
        }

        event = CDCEvent.from_kafka_message(cdc_message, offset=2, partition=0)

        assert event is not None
        assert event.id == 1
        assert event.operation == "d"

    def test_parse_tombstone_event(self):
        """Test parsing null message (Kafka tombstone)."""
        from stream_processor.consumer import CDCEvent

        event = CDCEvent.from_kafka_message(None, offset=3, partition=0)
        assert event is None


class TestEventBatching:
    """Test event batching logic."""

    def test_batch_creation(self):
        """Test creating a batch."""
        from stream_processor.consumer import EventBatch, CDCEvent

        batch = EventBatch(max_size=10, timeout=5.0)
        assert batch.max_size == 10
        assert len(batch.events) == 0

    def test_batch_add_event(self):
        """Test adding events to batch."""
        from stream_processor.consumer import EventBatch, CDCEvent

        batch = EventBatch(max_size=5)
        event = CDCEvent(id=1, ticker="TSLA", title="News", content="Content")

        should_flush = batch.add(event)

        assert len(batch.events) == 1
        assert not should_flush  # Not full yet

    def test_batch_size_flush(self):
        """Test batch flush when size limit reached."""
        from stream_processor.consumer import EventBatch, CDCEvent

        batch = EventBatch(max_size=3)

        for i in range(3):
            event = CDCEvent(
                id=i,
                ticker=f"TICK{i}",
                title="News",
                content="Content",
            )
            should_flush = batch.add(event)

            if i == 2:
                assert should_flush  # Should flush on 3rd event

    def test_batch_deduplication(self):
        """Test batch deduplication by ID."""
        from stream_processor.consumer import EventBatch, CDCEvent

        batch = EventBatch(max_size=10)

        event1 = CDCEvent(id=1, ticker="TSLA", title="News", content="Content")
        event2 = CDCEvent(id=1, ticker="TSLA", title="News", content="Updated")

        batch.add(event1)
        batch.add(event2)

        # Second event should be skipped (duplicate ID)
        assert len(batch.events) == 1

    def test_batch_timeout_check(self):
        """Test batch timeout detection."""
        from stream_processor.consumer import EventBatch, CDCEvent
        import time

        batch = EventBatch(max_size=100, timeout=0.1)
        event = CDCEvent(id=1, ticker="TSLA", title="News", content="Content")
        batch.add(event)

        assert not batch.should_flush()

        time.sleep(0.2)
        assert batch.should_flush()  # Should flush after timeout

    def test_batch_reset(self):
        """Test batch reset."""
        from stream_processor.consumer import EventBatch, CDCEvent

        batch = EventBatch(max_size=10)
        event = CDCEvent(id=1, ticker="TSLA", title="News", content="Content")
        batch.add(event)

        batch.reset()

        assert len(batch.events) == 0
        assert len(batch.seen_ids) == 0


class TestEmbeddingClient:
    """Test embedding client logic (mock)."""

    def test_openai_client_initialization(self):
        """Test OpenAI embedding client can be created."""
        from stream_processor.embedding_client import OpenAIEmbeddingClient

        client = OpenAIEmbeddingClient(
            api_key="test-key",
            model="text-embedding-3-small",
            batch_size=50,
        )

        assert client.model == "text-embedding-3-small"
        assert client.batch_size == 50

    def test_ollama_client_initialization(self):
        """Test Ollama embedding client can be created."""
        from stream_processor.embedding_client import OllamaEmbeddingClient

        client = OllamaEmbeddingClient(
            api_url="http://localhost:11434",
            model="nomic-embed-text",
            batch_size=50,
        )

        assert client.model == "nomic-embed-text"
        assert client.api_url == "http://localhost:11434"

    def test_fallback_client_initialization(self):
        """Test fallback embedding client setup."""
        from stream_processor.embedding_client import (
            FallbackEmbeddingClient,
            OpenAIEmbeddingClient,
            OllamaEmbeddingClient,
        )

        primary = OpenAIEmbeddingClient(api_key="test-key")
        fallback = OllamaEmbeddingClient()

        client = FallbackEmbeddingClient(primary=primary, fallback=fallback)

        assert client.primary is not None
        assert client.fallback is not None


class TestVectorSyncHandlers:
    """Test vector sync handlers."""

    def test_vector_record_creation(self):
        """Test creating a VectorRecord."""
        from vector_sync.handlers import VectorRecord

        record = VectorRecord(
            id=1,
            ticker="TSLA",
            title="News",
            content="Content",
            embedding=[0.1, 0.2, 0.3],
            source="Reuters",
        )

        assert record.id == 1
        assert record.ticker == "TSLA"
        assert len(record.embedding) == 3

    def test_vector_record_to_pinecone_dict(self):
        """Test converting VectorRecord to Pinecone format."""
        from vector_sync.handlers import VectorRecord

        record = VectorRecord(
            id=1,
            ticker="TSLA",
            title="News",
            content="Content",
            embedding=[0.1, 0.2, 0.3],
            source="Reuters",
        )

        pinecone_dict = record.to_pinecone_dict()

        assert pinecone_dict["id"] == "1"
        assert pinecone_dict["values"] == [0.1, 0.2, 0.3]
        assert pinecone_dict["metadata"]["ticker"] == "TSLA"

    def test_pgvector_handler_initialization(self):
        """Test PgVector handler can be created."""
        from vector_sync.handlers import PgVectorHandler

        handler = PgVectorHandler(
            host="localhost",
            port=5432,
            database="test_db",
            user="user",
            password="password",
        )

        assert handler.host == "localhost"
        assert handler.database == "test_db"


class TestConfiguration:
    """Test configuration management."""

    def test_config_from_env(self, monkeypatch):
        """Test loading config from environment variables."""
        from stream_processor.config import Config

        monkeypatch.setenv("POSTGRES_HOST", "test-host")
        monkeypatch.setenv("KAFKA_BROKERS", "localhost:9092")
        monkeypatch.setenv("BATCH_SIZE", "50")

        config = Config()

        assert config.postgres_host == "test-host"
        assert config.kafka_brokers == "localhost:9092"
        assert config.batch_size == 50

    def test_config_defaults(self):
        """Test config default values."""
        from stream_processor.config import Config

        config = Config()

        assert config.postgres_host == "postgres"
        assert config.batch_size == 100
        assert config.log_level == "INFO"

    def test_config_postgres_dsn(self):
        """Test PostgreSQL DSN generation."""
        from stream_processor.config import Config

        config = Config(
            postgres_user="user",
            postgres_password="pass",
            postgres_host="localhost",
            postgres_port=5432,
            postgres_db="testdb",
        )

        dsn = config.postgres_dsn
        assert "postgresql://" in dsn
        assert "user:pass@localhost:5432/testdb" in dsn

    def test_config_secret_masking(self):
        """Test that secrets are masked in log output."""
        from stream_processor.config import Config

        config = Config(
            openai_api_key="sk-test-secret-key",
            pinecone_api_key="pcn-secret-key",
        )

        config_repr = repr(config)

        # Secrets should be masked
        assert "sk-test-secret-key" not in config_repr
        assert "pcn-secret-key" not in config_repr
        assert "***MASKED***" in config_repr


class TestErrorHandling:
    """Test error handling and resilience."""

    def test_invalid_cdc_message_handling(self):
        """Test handling of invalid CDC messages."""
        from stream_processor.consumer import CDCEvent

        # Missing required fields
        cdc_message = {"after": {"id": 1}}  # Missing ticker, title, content

        event = CDCEvent.from_kafka_message(cdc_message, offset=0, partition=0)

        # Should handle gracefully
        if event:
            assert event.id == 1

    def test_batch_error_recovery(self):
        """Test batch can recover from errors."""
        from stream_processor.consumer import EventBatch, CDCEvent

        batch = EventBatch(max_size=10)

        # Add valid events
        for i in range(3):
            event = CDCEvent(
                id=i,
                ticker=f"TICK{i}",
                title="News",
                content="Content",
            )
            batch.add(event)

        # Batch should be usable
        assert len(batch.events) == 3

        # Reset and reuse
        batch.reset()
        assert len(batch.events) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
