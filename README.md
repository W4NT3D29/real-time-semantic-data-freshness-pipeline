```markdown
# Real-Time Semantic Data Freshness Pipeline

**Sub-second vector sync from live PostgreSQL** — a production-grade portfolio project for AI infrastructure.

This is **not** another static RAG demo.  
When you update a stock news headline in Postgres, the vector database updates in **<500 ms**. The AI agent always has the freshest breaking news.

## Architecture

```mermaid
flowchart LR
    A[PostgreSQL 16<br/>stock_news table] -->|WAL + CDC| B[Debezium Connector]
    B -->|Avro events| C[Redpanda + Schema Registry]
    C -->|Topic: stock_news_events| D[Python Kafka Consumer]
    D -->|Batch embeddings<br/>OpenAI / Ollama| E[Vector Sink]
    E --> F[pgvector (local)<br/>+ Pinecone (optional)]
    G[Streamlit Dashboard] -->|Live queries + freshness timer| F
```

## Quick Start (under 2 minutes)

```bash
# 1. Clone & prepare
git clone https://github.com/yourusername/real-time-semantic-data-freshness-pipeline.git
cd real-time-semantic-data-freshness-pipeline

# 2. Copy config
cp .env.example .env
# → Add your OpenAI key (or set USE_LOCAL_EMBEDDINGS=true later)

# 3. Start everything
make up
```

**What happens automatically:**
- Postgres + 15 realistic stock news rows
- Debezium connector (auto-registered with initial snapshot)
- Redpanda, Schema Registry, Python consumer
- Streamlit dashboard

Open **http://localhost:8501** — you’re ready.

### Test the Magic
```bash
make insert-test
```
Watch the dashboard:  
“Last sync: **187 ms ago**” + new embedding appears instantly.

## Key Features

- ✅ Auto-initialized Debezium CDC (snapshot + continuous WAL)
- ✅ Exactly-once processing + idempotent upserts
- ✅ Tombstone delete handling
- ✅ Micro-batching (default 100 records) with retries
- ✅ Dual vector sink: **pgvector** (zero-cost local) + optional Pinecone
- ✅ Beautiful Streamlit freshness dashboard
- ✅ Clean Python consumer (type hints, structured logging)
- ✅ Full Makefile (`make up`, `make down`, `make logs`, `make ollama`)

## Project Structure

```
├── docker-compose.yml          # Core pipeline (everything in one file)
├── Makefile                    # One-command everything
├── .env.example
├── schemas/record.avsc         # Avro schema
├── connect/debezium-connector.json
├── stream_processor/           # Python Kafka consumer + embedding logic
├── vector_sync/handlers.py     # Upsert + delete logic
├── demo/app.py                 # Streamlit dashboard
├── init_db/init.sql            # Schema + seed data
└── monitoring/                 # Optional Prometheus + Grafana
```

## Zero-Cost Mode (Ollama)

```bash
make ollama          # Starts local embeddings
# Then set USE_LOCAL_EMBEDDINGS=true in .env and restart
```

Everything else stays **$0**.

## Production Scaling Path

The demo uses a lightweight Python consumer for instant local runs.  
In production I would swap it for **Apache Flink** (Java/Scala) for true exactly-once, stateful processing, and horizontal scaling.

## Ready to Run?

```bash
make up
open http://localhost:8501
```

Update one row in Postgres → watch the vector DB stay perfectly fresh.
```