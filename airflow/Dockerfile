FROM apache/airflow:2.10.3-python3.12

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
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
    croniter
