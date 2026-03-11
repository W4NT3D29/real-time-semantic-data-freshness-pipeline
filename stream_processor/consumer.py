"""Kafka consumer for CDC events with batching, deduplication, and embedding sync."""

import asyncio
import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

import psycopg2
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition
from tenacity import retry, stop_after_attempt, wait_exponential

from stream_processor.config import Config
from stream_processor.embedding_client import EmbeddingClient
from vector_sync.handlers import PgVectorHandler, PineconeHandler, VectorRecord

logger = logging.getLogger(__name__)


@dataclass
class CDCEvent:
    """Represents a CDC event from Debezium."""

    id: int
    ticker: str
    title: str
    content: str
    source: Optional[str] = None
    published_at: Optional[str] = None
    updated_at: Optional[str] = None
    version: int = 0
    operation: str = "c"  # c=create, u=update, d=delete
    offset: int = 0
    partition: int = 0
    timestamp: datetime = field(default_factory=datetime.utcnow)

    @classmethod
    def from_kafka_message(cls, msg: Dict[str, Any], offset: int, partition: int) -> Optional["CDCEvent"]:
        """Parse CDC event from Kafka message."""
        try:
            # Handle tombstone events (deletes)
            if msg is None or msg.get("after") is None:
                if msg and msg.get("before"):
                    return cls(
                        id=msg["before"].get("id"),
                        ticker=msg["before"].get("ticker", ""),
                        title=msg["before"].get("title", ""),
                        content=msg["before"].get("content", ""),
                        operation="d",
                        offset=offset,
                        partition=partition,
                    )
                return None

            after = msg.get("after", {})
            return cls(
                id=after.get("id"),
                ticker=after.get("ticker", ""),
                title=after.get("title", ""),
                content=after.get("content", ""),
                source=after.get("source"),
                published_at=after.get("published_at"),
                updated_at=after.get("updated_at"),
                version=after.get("version", 0),
                operation=msg.get("op", "c"),
                offset=offset,
                partition=partition,
            )
        except Exception as e:
            logger.error(f"Failed to parse CDC event: {e}", exc_info=True)
            return None


@dataclass
class EventBatch:
    """Batch of events to be processed together."""

    events: List[CDCEvent] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    max_size: int = 100
    timeout: float = 5.0
    seen_ids: Set[int] = field(default_factory=set)

    def add(self, event: CDCEvent) -> bool:
        """Add event to batch, return True if batch should be flushed."""
        # Deduplication: skip if we've seen this ID in this batch
        if event.id in self.seen_ids:
            logger.debug(f"Skipping duplicate event: id={event.id}")
            return False

        self.seen_ids.add(event.id)
        self.events.append(event)
        return len(self.events) >= self.max_size

    def should_flush(self) -> bool:
        """Check if batch should be flushed (timeout or size)."""
        if not self.events:
            return False
        if len(self.events) >= self.max_size:
            return True
        age = (datetime.utcnow() - self.created_at).total_seconds()
        return age >= self.timeout

    def reset(self) -> None:
        """Reset batch."""
        self.events.clear()
        self.seen_ids.clear()
        self.created_at = datetime.utcnow()


class KafkaConsumerLoop:
    """Main Kafka consumer loop with CDC event processing."""

    def __init__(
        self,
        config: Config,
        embedding_client: EmbeddingClient,
    ):
        """Initialize consumer loop.

        Args:
            config: Application configuration
            embedding_client: Embedding client for generating vectors
        """
        self.config = config
        self.embedding_client = embedding_client

        # Kafka consumer
        self.consumer = Consumer(
            {
                "bootstrap.servers": config.kafka_brokers,
                "group.id": config.kafka_consumer_group,
                "auto.offset.reset": config.kafka_auto_offset_reset,
                "enable.auto.commit": False,
                "isolation.level": "read_committed",
                "session.timeout.ms": 30000,
                "max.poll.interval.ms": 300000,
                "fetch.max.bytes": 52428800,
                "fetch.min.bytes": 1024,
            }
        )

        # Vector DB handlers
        self.pinecone_handler = (
            PineconeHandler(
                api_key=config.pinecone_api_key,
                index_name=config.pinecone_index_name,
                dimension=config.pinecone_dimension,
            )
            if config.pinecone_api_key
            else None
        )

        self.pgvector_handler = PgVectorHandler(
            host=config.postgres_host,
            port=config.postgres_port,
            database=config.postgres_db,
            user=config.postgres_user,
            password=config.postgres_password,
        )

        # Event batching
        self.batch = EventBatch(
            max_size=config.batch_size,
            timeout=config.batch_timeout_seconds,
        )

        # Metrics
        self.metrics = {
            "events_processed": 0,
            "events_embedded": 0,
            "events_deleted": 0,
            "batches_flushed": 0,
            "errors": 0,
            "last_flush": datetime.utcnow(),
        }

        self.running = True
        logger.info(f"KafkaConsumerLoop initialized", extra={"config": str(config)})

    def subscribe(self) -> None:
        """Subscribe to Kafka topic."""
        logger.info(f"Subscribing to topic: {self.config.kafka_topic}")
        self.consumer.subscribe([self.config.kafka_topic])

    async def process_batch(self, batch: EventBatch) -> None:
        """Process a batch of CDC events.

        Args:
            batch: Batch of events to process
        """
        if not batch.events:
            return

        logger.info(f"Processing batch of {len(batch.events)} events")

        try:
            # Separate events by operation
            creates_updates = [e for e in batch.events if e.operation in ("c", "u")]
            deletes = [e for e in batch.events if e.operation == "d"]

            # Process creates/updates
            if creates_updates:
                await self._process_creates_updates(creates_updates)

            # Process deletes
            if deletes:
                await self._process_deletes(deletes)

            self.metrics["batches_flushed"] += 1
            self.metrics["last_flush"] = datetime.utcnow()

            logger.info(
                f"Batch processed successfully",
                extra={
                    "creates_updates": len(creates_updates),
                    "deletes": len(deletes),
                    "total_processed": self.metrics["events_processed"],
                },
            )

        except Exception as e:
            logger.error(f"Failed to process batch: {e}", exc_info=True)
            self.metrics["errors"] += 1
            raise

    async def _process_creates_updates(self, events: List[CDCEvent]) -> None:
        """Process create and update events.

        Args:
            events: List of create/update events
        """
        # Extract content for embedding
        contents = [event.content for event in events]

        logger.debug(f"Embedding {len(contents)} texts")

        # Generate embeddings
        embeddings = await self.embedding_client.embed(contents)

        if len(embeddings) != len(events):
            raise ValueError(
                f"Embedding count mismatch: expected {len(events)}, got {len(embeddings)}"
            )

        # Create vector records
        records = [
            VectorRecord(
                id=event.id,
                ticker=event.ticker,
                title=event.title,
                content=event.content,
                embedding=embeddings[i],
                source=event.source,
                published_at=event.published_at,
                version=event.version,
            )
            for i, event in enumerate(events)
        ]

        # Upsert to Pinecone
        if self.pinecone_handler:
            await self.pinecone_handler.upsert(records)

        # Upsert to pgvector
        await self.pgvector_handler.upsert(records)

        self.metrics["events_embedded"] += len(events)
        self.metrics["events_processed"] += len(events)

    async def _process_deletes(self, events: List[CDCEvent]) -> None:
        """Process delete events (tombstones).

        Args:
            events: List of delete events
        """
        ids = [str(event.id) for event in events]

        logger.debug(f"Deleting {len(ids)} vectors from vector DBs")

        # Delete from Pinecone
        if self.pinecone_handler:
            await self.pinecone_handler.delete(ids)

        # Delete from pgvector
        await self.pgvector_handler.delete([int(id) for id in ids])

        self.metrics["events_deleted"] += len(events)
        self.metrics["events_processed"] += len(events)

    def commit_offsets(self) -> None:
        """Commit current offset."""
        try:
            self.consumer.commit(asynchronous=False)
            logger.debug("Offsets committed")
        except Exception as e:
            logger.error(f"Failed to commit offsets: {e}", exc_info=True)

    def get_metrics(self) -> Dict[str, Any]:
        """Get consumer metrics."""
        return {
            **self.metrics,
            "last_flush_ago_seconds": (datetime.utcnow() - self.metrics["last_flush"]).total_seconds(),
            "batch_size": len(self.batch.events),
        }

    async def run(self) -> None:
        """Main consumer loop."""
        logger.info("Starting Kafka consumer loop")
        self.subscribe()

        last_commit = datetime.utcnow()
        commit_interval = timedelta(seconds=30)

        try:
            while self.running:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    # Check if batch should be flushed due to timeout
                    if self.batch.should_flush():
                        await self.process_batch(self.batch)
                        self.batch.reset()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition")
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue

                # Parse CDC event
                try:
                    kafka_msg = json.loads(msg.value().decode("utf-8"))
                    event = CDCEvent.from_kafka_message(
                        kafka_msg,
                        offset=msg.offset(),
                        partition=msg.partition(),
                    )

                    if event is None:
                        continue

                    logger.debug(
                        f"Received event",
                        extra={
                            "id": event.id,
                            "op": event.operation,
                            "offset": event.offset,
                        },
                    )

                    # Add to batch
                    should_flush = self.batch.add(event)

                    if should_flush:
                        await self.process_batch(self.batch)
                        self.batch.reset()

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}", exc_info=True)
                    self.metrics["errors"] += 1
                    continue

                # Commit offsets periodically
                if (datetime.utcnow() - last_commit) > commit_interval:
                    self.commit_offsets()
                    last_commit = datetime.utcnow()

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Consumer loop error: {e}", exc_info=True)
            raise
        finally:
            # Final flush before closing
            if self.batch.events:
                await self.process_batch(self.batch)

            # Commit final offsets
            self.commit_offsets()

            # Close connections
            await self.embedding_client.close()
            self.pgvector_handler.close()
            self.consumer.close()

            logger.info("Consumer loop stopped", extra={"metrics": self.get_metrics()})

    async def shutdown(self) -> None:
        """Graceful shutdown."""
        logger.info("Initiating graceful shutdown")
        self.running = False
