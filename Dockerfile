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

# Create Streamlit config file for Render.com
RUN echo "[server]\n\
port = 8501\n\
address = 0.0.0.0\n\
headless = true\n\
enableCORS = false\n\
enableXsrfProtection = false\n\
" > /root/.streamlit/config.toml

# Create startup script to handle PORT env var from Render
RUN echo '#!/bin/bash\n\
PORT=${PORT:-8501}\n\
exec streamlit run ai_data_analyst.py --server.port=$PORT --server.address=0.0.0.0\n\
' > /app/start.sh && chmod +x /app/start.sh

# Health check (uses PORT env var or defaults to 8501)
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import os, requests; port = os.getenv('PORT', '8501'); requests.get(f'http://localhost:{port}/_stcore/health')" || exit 1

# Expose default port (Render will map to PORT env var)
EXPOSE 8501

# Run startup script
CMD ["/app/start.sh"]

