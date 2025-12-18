# Use Python 3.11 slim image for smaller size
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY *.py ./

# Create directory for Streamlit config
RUN mkdir -p /root/.streamlit

# Copy Streamlit config file (more reliable than echo)
COPY .streamlit/config.toml /root/.streamlit/config.toml

# Create startup script to handle PORT env var from Render
# Also set environment variables for Streamlit
RUN echo '#!/bin/bash\n\
set -e\n\
PORT=${PORT:-8501}\n\
export STREAMLIT_SERVER_PORT=$PORT\n\
export STREAMLIT_SERVER_ADDRESS=0.0.0.0\n\
export STREAMLIT_SERVER_HEADLESS=true\n\
export STREAMLIT_BROWSER_GATHER_USAGE_STATS=false\n\
export STREAMLIT_SERVER_ENABLE_CORS=true\n\
export STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=true\n\
echo "Starting Streamlit on port $PORT..."\n\
exec streamlit run ai_data_analyst.py --server.port=$PORT --server.address=0.0.0.0 --server.headless=true --server.enableCORS=true\n\
' > /app/start.sh && chmod +x /app/start.sh

# Health check (uses PORT env var or defaults to 8501)
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import os, requests; port = os.getenv('PORT', '8501'); requests.get(f'http://localhost:{port}/_stcore/health')" || exit 1

# Expose default port (Render will map to PORT env var)
EXPOSE 8501

# Run startup script
CMD ["/app/start.sh"]

