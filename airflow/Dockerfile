FROM apache/airflow:3.1.7-python3.12

USER root

# Install system dependencies + Playwright browser dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
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
    playwright

# Install Playwright Chromium browser only (smaller image)
ENV PLAYWRIGHT_BROWSERS_PATH=/home/airflow/.cache/ms-playwright
RUN playwright install chromium
