# =============================================================================
# frontend/Dockerfile -- Multi-stage build for Vite React SPA
# =============================================================================
# Stages:
#   deps       -- install node_modules (cached layer)
#   builder    -- compile TypeScript and produce static assets
#   production -- serve with hardened nginx as non-root
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1: deps -- dependency installation
# ---------------------------------------------------------------------------
FROM node:20-alpine AS deps

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts

# ---------------------------------------------------------------------------
# Stage 2: builder -- build static assets
# ---------------------------------------------------------------------------
FROM deps AS builder

COPY . .

# Build-time environment variables (API URL baked into static JS)
ARG VITE_API_BASE_URL=/api
ENV VITE_API_BASE_URL=${VITE_API_BASE_URL}

RUN npm run build

# ---------------------------------------------------------------------------
# Stage 3: production -- nginx serving static files
# ---------------------------------------------------------------------------
FROM nginx:1.27-alpine AS production

# Remove default nginx config and html
RUN rm -rf /etc/nginx/conf.d/default.conf /usr/share/nginx/html/*

# Copy hardened nginx config
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy built assets
COPY --from=builder /app/dist /usr/share/nginx/html

# Create non-root user and adjust permissions
RUN addgroup -g 1001 -S appgroup \
    && adduser -u 1001 -S appuser -G appgroup \
    && chown -R appuser:appgroup /usr/share/nginx/html \
    && chown -R appuser:appgroup /var/cache/nginx \
    && chown -R appuser:appgroup /var/log/nginx \
    && touch /run/nginx.pid \
    && chown appuser:appgroup /run/nginx.pid \
    # Allow the non-root user to write to the tmp directories
    && mkdir -p /tmp/nginx \
    && chown -R appuser:appgroup /tmp/nginx

USER appuser

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/nginx-health || exit 1

CMD ["nginx", "-g", "daemon off;"]
