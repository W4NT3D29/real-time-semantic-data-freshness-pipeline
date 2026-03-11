"""Embedding client with batching, retry logic, and fallback strategies."""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import List, Optional

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class EmbeddingClient(ABC):
    """Abstract base class for embedding clients."""

    @abstractmethod
    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a list of texts.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List of embeddings (each embedding is a list of floats)
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close any connections."""
        pass


class OpenAIEmbeddingClient(EmbeddingClient):
    """OpenAI embedding client with batching and retry logic."""

    def __init__(
        self,
        api_key: str,
        model: str = "text-embedding-3-small",
        batch_size: int = 50,
        max_retries: int = 3,
        backoff_base: float = 2.0,
    ):
        """Initialize OpenAI embedding client.
        
        Args:
            api_key: OpenAI API key
            model: Embedding model name
            batch_size: Batch size for embedding requests
            max_retries: Maximum number of retries
            backoff_base: Exponential backoff base
        """
        self.api_key = api_key
        self.model = model
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.client = httpx.AsyncClient(
            base_url="https://api.openai.com/v1",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=30.0,
        )

    @retry(
        retry=retry_if_exception_type(httpx.HTTPError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
    )
    async def _embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Embed a batch of texts with retry logic.
        
        Args:
            texts: Batch of texts to embed
            
        Returns:
            List of embeddings
        """
        logger.debug(f"Embedding batch of {len(texts)} texts with OpenAI")
        
        response = await self.client.post(
            "/embeddings",
            json={"input": texts, "model": self.model},
        )
        response.raise_for_status()
        
        data = response.json()
        # Extract embeddings, maintaining order
        embeddings = sorted(data["data"], key=lambda x: x["index"])
        return [item["embedding"] for item in embeddings]

    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for texts, batching requests.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List of embeddings in the same order as input
        """
        if not texts:
            return []
        
        all_embeddings = []
        
        # Process in batches
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i : i + self.batch_size]
            logger.debug(f"Processing batch {i // self.batch_size + 1} ({len(batch)} texts)")
            
            try:
                embeddings = await self._embed_batch(batch)
                all_embeddings.extend(embeddings)
            except httpx.HTTPError as e:
                logger.error(f"Failed to embed batch: {e}", exc_info=True)
                raise

        logger.info(f"Successfully embedded {len(all_embeddings)} texts")
        return all_embeddings

    async def close(self) -> None:
        """Close HTTP client."""
        await self.client.aclose()


class OllamaEmbeddingClient(EmbeddingClient):
    """Ollama embedding client for local, zero-cost embeddings."""

    def __init__(
        self,
        api_url: str = "http://ollama:11434",
        model: str = "nomic-embed-text",
        batch_size: int = 50,
    ):
        """Initialize Ollama embedding client.
        
        Args:
            api_url: Ollama API URL
            model: Model name
            batch_size: Batch size for requests
        """
        self.api_url = api_url
        self.model = model
        self.batch_size = batch_size
        self.client = httpx.AsyncClient(timeout=120.0)

    async def _embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Embed a batch using Ollama.
        
        Args:
            texts: Batch of texts
            
        Returns:
            List of embeddings
        """
        logger.debug(f"Embedding batch of {len(texts)} texts with Ollama")
        
        embeddings = []
        for text in texts:
            response = await self.client.post(
                f"{self.api_url}/api/embeddings",
                json={"model": self.model, "prompt": text},
            )
            response.raise_for_status()
            data = response.json()
            embeddings.append(data["embedding"])
        
        return embeddings

    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for texts using Ollama.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List of embeddings
        """
        if not texts:
            return []
        
        all_embeddings = []
        
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i : i + self.batch_size]
            logger.debug(f"Processing batch {i // self.batch_size + 1} ({len(batch)} texts)")
            
            try:
                embeddings = await self._embed_batch(batch)
                all_embeddings.extend(embeddings)
            except httpx.HTTPError as e:
                logger.error(f"Failed to embed batch with Ollama: {e}", exc_info=True)
                raise

        logger.info(f"Successfully embedded {len(all_embeddings)} texts with Ollama")
        return all_embeddings

    async def close(self) -> None:
        """Close HTTP client."""
        await self.client.aclose()


class FallbackEmbeddingClient(EmbeddingClient):
    """Embedding client with fallback strategy (OpenAI -> Ollama)."""

    def __init__(self, primary: EmbeddingClient, fallback: EmbeddingClient):
        """Initialize with primary and fallback clients.
        
        Args:
            primary: Primary embedding client (typically OpenAI)
            fallback: Fallback embedding client (typically Ollama)
        """
        self.primary = primary
        self.fallback = fallback

    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings with fallback.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List of embeddings
        """
        if not texts:
            return []
        
        try:
            logger.debug("Attempting to embed with primary client")
            return await self.primary.embed(texts)
        except Exception as e:
            logger.warning(f"Primary embedding client failed: {e}. Falling back...")
            try:
                return await self.fallback.embed(texts)
            except Exception as e2:
                logger.error(f"Fallback embedding client also failed: {e2}", exc_info=True)
                raise

    async def close(self) -> None:
        """Close both clients."""
        await self.primary.close()
        await self.fallback.close()
