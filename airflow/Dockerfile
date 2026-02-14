# =============================================================================
# airflow/Dockerfile -- Production Airflow image with DAGs, crawlers, and deps
# =============================================================================
# For development, the base docker-compose.yml mounts dags/ and crawlers/ as
# volumes. In production (docker-compose.prod.yml), no source mounts exist --
# everything is baked into this image.
#
# Build context should be the project root:
#   docker build -t crawler-airflow:TAG -f airflow/Dockerfile .
# =============================================================================

FROM apache/airflow:3.1.7-python3.12

USER root

# Install system dependencies + Playwright Chromium dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    tini \
    # Playwright Chromium dependencies
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libatspi2.0-0 \
    libwayland-client0 \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    openai \
    pymongo \
    requests \
    beautifulsoup4 \
    selenium \
    pdfplumber \
    pandas \
    openpyxl \
    lxml \
    httpx \
    croniter \
    playwright \
    boto3 \
    google-cloud-storage \
    psycopg2-binary

# Install Playwright Chromium browser only (smaller image)
ENV PLAYWRIGHT_BROWSERS_PATH=/home/airflow/.cache/ms-playwright
RUN playwright install chromium

# ---------------------------------------------------------------------------
# Bake application code into the image for production.
# In development these paths are overridden by volume mounts.
# ---------------------------------------------------------------------------
COPY airflow/dags/ /opt/airflow/dags/
COPY airflow/plugins/ /opt/airflow/plugins/
COPY airflow/config/ /opt/airflow/config/
COPY crawlers/ /opt/airflow/crawlers/
