"""Vector database sync handlers for Pinecone and pgvector."""

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx
import psycopg2
from pinecone import Pinecone, PodSpec

logger = logging.getLogger(__name__)


@dataclass
class VectorRecord:
    """A vector record to sync to vector databases."""

    id: int
    ticker: str
    title: str
    content: str
    embedding: List[float]
    source: Optional[str] = None
    published_at: Optional[str] = None
    version: int = 0

    def to_pinecone_dict(self) -> Dict[str, Any]:
        """Convert to Pinecone upsert format."""
        return {
            "id": str(self.id),
            "values": self.embedding,
            "metadata": {
                "ticker": self.ticker,
                "title": self.title,
                "content": self.content,
                "source": self.source or "Unknown",
                "published_at": self.published_at,
                "version": str(self.version),
            },
        }


class PineconeHandler:
    """Handler for syncing vectors to Pinecone."""

    def __init__(self, api_key: str, index_name: str, dimension: int = 1536):
        """Initialize Pinecone handler.
        
        Args:
            api_key: Pinecone API key
            index_name: Index name
            dimension: Vector dimension (typically 1536 for OpenAI)
        """
        self.api_key = api_key
        self.index_name = index_name
        self.dimension = dimension
        self.client = Pinecone(api_key=api_key)
        self._ensure_index_exists()

    def _ensure_index_exists(self) -> None:
        """Ensure Pinecone index exists."""
        try:
            index_list = self.client.list_indexes()
            
            if self.index_name not in index_list:
                logger.info(f"Creating Pinecone index: {self.index_name}")
                self.client.create_index(
                    name=self.index_name,
                    dimension=self.dimension,
                    metric="cosine",
                    spec=PodSpec(environment="gcp-starter"),
                )
                logger.info(f"Index {self.index_name} created successfully")
            else:
                logger.info(f"Index {self.index_name} already exists")
        except Exception as e:
            logger.error(f"Failed to ensure index exists: {e}", exc_info=True)
            raise

    async def upsert(self, records: List[VectorRecord]) -> None:
        """Upsert records to Pinecone.
        
        Args:
            records: List of VectorRecord objects
        """
        if not records:
            return

        try:
            index = self.client.Index(self.index_name)
            vectors = [record.to_pinecone_dict() for record in records]
            
            logger.debug(f"Upserting {len(vectors)} vectors to Pinecone")
            index.upsert(vectors=vectors)
            logger.info(f"Successfully upserted {len(vectors)} vectors to Pinecone")
            
        except Exception as e:
            logger.error(f"Failed to upsert to Pinecone: {e}", exc_info=True)
            raise

    async def delete(self, ids: List[str]) -> None:
        """Delete records from Pinecone by ID.
        
        Args:
            ids: List of record IDs to delete
        """
        if not ids:
            return

        try:
            index = self.client.Index(self.index_name)
            logger.debug(f"Deleting {len(ids)} vectors from Pinecone")
            index.delete(ids=[str(id) for id in ids])
            logger.info(f"Successfully deleted {len(ids)} vectors from Pinecone")
            
        except Exception as e:
            logger.error(f"Failed to delete from Pinecone: {e}", exc_info=True)
            raise

    async def query(
        self, query_embedding: List[float], top_k: int = 10
    ) -> List[Dict[str, Any]]:
        """Query Pinecone for similar vectors.
        
        Args:
            query_embedding: Query vector
            top_k: Number of top results
            
        Returns:
            List of matching results with metadata
        """
        try:
            index = self.client.Index(self.index_name)
            results = index.query(
                vector=query_embedding,
                top_k=top_k,
                include_metadata=True,
            )
            return results.get("matches", [])
        except Exception as e:
            logger.error(f"Failed to query Pinecone: {e}", exc_info=True)
            raise


class PgVectorHandler:
    """Handler for syncing vectors to pgvector (PostgreSQL)."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
    ):
        """Initialize pgvector handler.
        
        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            database: Database name
            user: Database user
            password: Database password
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = None

    def _connect(self) -> None:
        """Connect to PostgreSQL."""
        if self.conn is None:
            try:
                self.conn = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password,
                )
                logger.info(f"Connected to PostgreSQL at {self.host}:{self.port}")
            except Exception as e:
                logger.error(f"Failed to connect to PostgreSQL: {e}", exc_info=True)
                raise

    async def upsert(self, records: List[VectorRecord]) -> None:
        """Upsert embeddings to pgvector.
        
        Args:
            records: List of VectorRecord objects
        """
        if not records:
            return

        self._connect()

        try:
            cursor = self.conn.cursor()
            
            for record in records:
                # Convert embedding to pgvector format
                embedding_str = "[" + ",".join(str(x) for x in record.embedding) + "]"
                
                cursor.execute(
                    """
                    UPDATE stock_news 
                    SET embedding = %s::vector, version = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (embedding_str, record.version, record.id),
                )
            
            self.conn.commit()
            logger.info(f"Successfully upserted {len(records)} embeddings to pgvector")
            cursor.close()
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to upsert to pgvector: {e}", exc_info=True)
            raise

    async def delete(self, ids: List[int]) -> None:
        """Delete embeddings from pgvector.
        
        Args:
            ids: List of record IDs to delete
        """
        if not ids:
            return

        self._connect()

        try:
            cursor = self.conn.cursor()
            
            for id in ids:
                cursor.execute(
                    """
                    UPDATE stock_news 
                    SET embedding = NULL, deleted_at = NOW()
                    WHERE id = %s
                    """,
                    (id,),
                )
            
            self.conn.commit()
            logger.info(f"Successfully marked {len(ids)} embeddings as deleted in pgvector")
            cursor.close()
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to delete from pgvector: {e}", exc_info=True)
            raise

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from PostgreSQL")
