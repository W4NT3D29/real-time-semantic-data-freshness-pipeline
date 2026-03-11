FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY stream_processor/ ./stream_processor/
COPY vector_sync/ ./vector_sync/

# Health check endpoint exposed via a simple HTTP server
EXPOSE 9090

# Run the stream processor
CMD ["python", "-m", "stream_processor.main"]
