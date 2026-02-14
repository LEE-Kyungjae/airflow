# =============================================================================
# api/Dockerfile -- Multi-stage build for FastAPI crawler API
# =============================================================================
# Stages:
#   base        -- shared Python runtime with system deps
#   dependencies -- install pip packages (cached layer)
#   production  -- final slim image with non-root user
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1: base -- common runtime
# ---------------------------------------------------------------------------
FROM python:3.14-slim AS base

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        tini \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Stage 2: dependencies -- install Python packages
# ---------------------------------------------------------------------------
FROM base AS dependencies

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---------------------------------------------------------------------------
# Stage 3: production -- final image
# ---------------------------------------------------------------------------
FROM base AS production

# Copy installed packages from dependency stage
COPY --from=dependencies /install /usr/local

# Copy application code
COPY app/ ./app/

# Create non-root user
RUN groupadd --gid 1001 appgroup \
    && useradd --uid 1001 --gid appgroup --create-home --shell /bin/false appuser \
    && chown -R appuser:appgroup /app

USER appuser

EXPOSE 8000

# Use tini as PID 1 for proper signal handling
ENTRYPOINT ["tini", "--"]

HEALTHCHECK --interval=15s --timeout=5s --start-period=15s --retries=5 \
    CMD curl --fail --silent http://localhost:8000/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2", "--loop", "uvloop", "--http", "httptools"]
