.PHONY: help up down logs demo monitor clean test build deploy-connector connector-status

help:
	@echo "Real-Time Semantic Data Freshness Pipeline"
	@echo ""
	@echo "Available commands:"
	@echo "  make up                 Start the full pipeline (Postgres, Redpanda, Debezium, Python consumer, Streamlit)"
	@echo "  make down               Stop and remove all containers"
	@echo "  make logs               View logs from all services"
	@echo "  make logs-processor     View logs from stream processor"
	@echo "  make logs-streamlit     View logs from Streamlit"
	@echo "  make logs-debezium      View logs from Debezium connector"
	@echo "  make demo               Start in demo mode (no Ollama)"
	@echo "  make deploy-connector   Deploy CDC connector to Debezium"
	@echo "  make connector-status   Check Debezium connector status"
	@echo "  make monitor            Start optional monitoring stack (Prometheus + Grafana)"
	@echo "  make monitor-down       Stop monitoring stack"
	@echo "  make ollama             Start Ollama service for zero-cost embeddings"
	@echo "  make ollama-down        Stop Ollama service"
	@echo "  make build              Build Docker images"
	@echo "  make test               Run integration tests"
	@echo "  make clean              Clean up volumes and dangling images"
	@echo "  make shell-pg           Open psql shell to Postgres"
	@echo "  make shell-kafka        Open Kafka consumer shell"
	@echo "  make health             Check health of all services"
	@echo "  make insert-test        Insert test row to trigger CDC"

# Default compose file
COMPOSE_FILE := docker-compose.yml
COMPOSE_FLAGS := -f $(COMPOSE_FILE)

# ==================== CORE TARGETS ====================

up: build
	@echo "Starting Real-Time Semantic Data Freshness Pipeline..."
	docker compose $(COMPOSE_FLAGS) up -d
	@echo "Waiting for services to be healthy..."
	@sleep 10
	@make health
	@echo ""
	@echo "✅ Pipeline is ready!"
	@echo "   - Redpanda Console: http://localhost:8080"
	@echo "   - Debezium API: http://localhost:8083"
	@echo "   - Streamlit: http://localhost:8501"
	@echo "   - Postgres: localhost:5432"

down:
	@echo "Stopping pipeline..."
	docker compose $(COMPOSE_FLAGS) down --remove-orphans
	@echo "✅ Pipeline stopped"

build:
	@echo "Building Docker images..."
	docker compose $(COMPOSE_FLAGS) build --no-cache

logs:
	docker compose $(COMPOSE_FLAGS) logs -f

logs-processor:
	docker compose $(COMPOSE_FLAGS) logs -f stream-processor

logs-streamlit:
	docker compose $(COMPOSE_FLAGS) logs -f streamlit

logs-debezium:
	docker compose $(COMPOSE_FLAGS) logs -f debezium-connect-1

demo: up
	@echo ""
	@echo "✅ Demo is running!"
	@echo ""
	@echo "Next steps:"
	@echo "1. Open Streamlit dashboard: http://localhost:8501"
	@echo "2. In another terminal, run: make insert-test"
	@echo "3. Watch the freshness metric update in real-time!"

# ==================== OLLAMA / EMBEDDINGS ====================

ollama:
	@echo "Starting Ollama service (zero-cost embeddings)..."
	docker compose $(COMPOSE_FLAGS) --profile ollama up -d ollama
	@echo "Pulling nomic-embed-text model (this takes ~5-10 minutes)..."
	docker exec pipeline_ollama ollama pull nomic-embed-text
	@echo "✅ Ollama ready at http://localhost:11434"
	@echo "Set USE_LOCAL_EMBEDDINGS=true in .env to use it"

ollama-down:
	docker compose $(COMPOSE_FLAGS) --profile ollama down ollama

# ==================== DEBEZIUM CONNECTOR ====================

deploy-connector:
	@echo "Deploying Debezium CDC connector..."
	bash connect/deploy-connector.sh localhost:8083
	@echo ""
	@echo "✅ Connector deployment initiated!"

connector-status:
	@echo "Checking Debezium connector status..."
	curl -s http://localhost:8083/connectors/stock-news-connector/status | jq . || echo "Connector not found"

connector-logs:
	docker compose $(COMPOSE_FLAGS) logs -f debezium-connect-1

connector-delete:
	@echo "Deleting connector..."
	curl -X DELETE http://localhost:8083/connectors/stock-news-connector
	@echo "✅ Connector deleted"

# ==================== MONITORING (OPTIONAL) ====================

monitor:
	@echo "Starting optional monitoring stack..."
	docker compose -f docker-compose.yml -f monitoring/docker-compose.monitoring.yml up -d
	@echo "✅ Monitoring stack started!"
	@echo "   - Prometheus: http://localhost:9090"
	@echo "   - Grafana: http://localhost:3000 (admin/admin)"

monitor-down:
	docker compose -f docker-compose.yml -f monitoring/docker-compose.monitoring.yml down

# ==================== TESTING & DIAGNOSTICS ====================

health:
	@echo "Checking service health..."
	@docker ps --format "table {{.Names}}\t{{.Status}}" | grep pipeline

test:
	@echo "Running integration tests..."
	pytest -v tests/ --tb=short

test-watch:
	@echo "Running tests in watch mode..."
	pytest -v tests/ --tb=short -s

test-coverage:
	@echo "Running tests with coverage..."
	pytest -v tests/ --cov=stream_processor --cov=vector_sync --cov-report=html --cov-report=term-missing

test-quick:
	@echo "Running quick unit tests (no containers)..."
	pytest -v tests/test_integration.py::TestConfiguration -s
	pytest -v tests/test_integration.py::TestCDCEventParsing -s
	pytest -v tests/test_integration.py::TestEventBatching -s
	pytest -v tests/test_integration.py::TestErrorHandling -s

test-docker:
	@echo "Running containerized integration tests..."
	pytest -v tests/test_integration.py::TestDatabaseSchema -s
	pytest -v tests/test_integration.py::TestKafkaProducerConsumer -s

shell-pg:
	docker exec -it pipeline_postgres psql -U pipeline_user -d stock_news_db

shell-kafka:
	docker exec -it pipeline_redpanda rpk topic consume stock_news_events --format=json

insert-test:
	@echo "Inserting test row to trigger CDC..."
	docker exec pipeline_postgres psql -U pipeline_user -d stock_news_db -c \
		"INSERT INTO stock_news (ticker, title, content, source) VALUES ('TEST', 'Breaking News!', 'This is a test insertion to trigger CDC and prove real-time freshness.', 'Test Source');"
	@echo "✅ Test row inserted. Check Streamlit dashboard for real-time update!"

# ==================== CLEANUP ====================

clean:
	@echo "Cleaning up..."
	docker compose $(COMPOSE_FLAGS) down -v
	docker system prune -f
	rm -rf __pycache__ .pytest_cache .mypy_cache
	@echo "✅ Cleanup complete"

volume-inspect:
	@echo "Postgres data volume contents:"
	docker volume inspect pipeline_postgres_data

restart:
	docker compose $(COMPOSE_FLAGS) restart

ps:
	docker compose $(COMPOSE_FLAGS) ps

# ==================== DEVELOPMENT ====================

dev-setup:
	cp .env.example .env
	@echo "✅ Development environment initialized"
	@echo "Review .env and update secrets if needed"

format:
	black stream_processor/ vector_sync/ demo/ tests/
	isort stream_processor/ vector_sync/ demo/ tests/

lint:
	ruff check stream_processor/ vector_sync/ demo/ tests/
	mypy stream_processor/ vector_sync/ demo/

typecheck:
	mypy stream_processor/ vector_sync/ demo/
