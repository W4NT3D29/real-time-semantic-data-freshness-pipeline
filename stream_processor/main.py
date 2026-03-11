"""Stream processor main entry point - Kafka consumer loop."""

import asyncio
import logging
import signal
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

from stream_processor.config import Config
from stream_processor.consumer import KafkaConsumerLoop
from stream_processor.embedding_client import FallbackEmbeddingClient, OllamaEmbeddingClient, OpenAIEmbeddingClient
from stream_processor.logging_config import setup_logging

logger = logging.getLogger(__name__)


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP handler for health checks."""

    def do_GET(self):
        """Handle GET request."""
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "healthy"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        """Suppress logging."""
        pass


class StreamProcessor:
    """Main stream processor for handling CDC events."""

    def __init__(self, config: Config):
        self.config = config
        self.running = True
        self.consumer_loop: Optional[KafkaConsumerLoop] = None
        self.health_server: Optional[HTTPServer] = None
        logger.info("StreamProcessor initialized", extra={"config": str(config)})

    async def _init_embedding_client(self):
        """Initialize embedding client with fallback strategy."""
        logger.info(
            "Initializing embedding client",
            extra={"use_local": self.config.use_local_embeddings}
        )

        if self.config.use_local_embeddings:
            logger.info("Using local Ollama embeddings (zero-cost mode)")
            return OllamaEmbeddingClient(
                api_url=self.config.ollama_api_url,
                model=self.config.ollama_model,
                batch_size=self.config.batch_size,
            )
        else:
            if not self.config.openai_api_key:
                logger.warning(
                    "OpenAI API key not set. Falling back to Ollama. "
                    "Set OPENAI_API_KEY env var for production quality."
                )
                return OllamaEmbeddingClient(
                    api_url=self.config.ollama_api_url,
                    model=self.config.ollama_model,
                    batch_size=self.config.batch_size,
                )
            else:
                logger.info("Using OpenAI embeddings (production mode)")
                primary = OpenAIEmbeddingClient(
                    api_key=self.config.openai_api_key,
                    model=self.config.openai_model,
                    batch_size=self.config.batch_size,
                    max_retries=self.config.embedding_retry_max_attempts,
                    backoff_base=self.config.embedding_retry_backoff_base,
                )
                fallback = OllamaEmbeddingClient(
                    api_url=self.config.ollama_api_url,
                    model=self.config.ollama_model,
                    batch_size=self.config.batch_size,
                )
                return FallbackEmbeddingClient(primary=primary, fallback=fallback)

    def _start_health_server(self) -> None:
        """Start health check HTTP server."""
        try:
            self.health_server = HTTPServer(("0.0.0.0", self.config.prometheus_port), HealthCheckHandler)
            logger.info(f"Health check server started on port {self.config.prometheus_port}")
        except Exception as e:
            logger.warning(f"Failed to start health server: {e}")

    async def start(self) -> None:
        """Start the stream processor."""
        logger.info("Starting stream processor...")

        # Start health check server
        self._start_health_server()

        try:
            # Initialize embedding client
            embedding_client = await self._init_embedding_client()

            # Create consumer loop
            self.consumer_loop = KafkaConsumerLoop(
                config=self.config,
                embedding_client=embedding_client,
            )

            # Start consumer
            await self.consumer_loop.run()

        except Exception as e:
            logger.error(f"Stream processor error: {e}", exc_info=True)
            raise

    async def shutdown(self) -> None:
        """Graceful shutdown."""
        logger.info("Shutting down stream processor...")
        self.running = False

        if self.consumer_loop:
            await self.consumer_loop.shutdown()

        if self.health_server:
            self.health_server.shutdown()

    def _handle_signal(self, signum: int, frame) -> None:
        """Handle termination signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        asyncio.create_task(self.shutdown())


async def main() -> None:
    """Main entry point."""
    try:
        # Load configuration
        config = Config()

        # Setup logging
        setup_logging(level=config.log_level, json_format=True)

        # Create processor
        processor = StreamProcessor(config)

        # Register signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig, lambda s=sig: processor._handle_signal(s, None)
            )

        # Start processor
        await processor.start()

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
