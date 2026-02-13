#!/usr/bin/env bash
# =============================================================================
# scripts/deploy.sh -- Build, tag, and deploy with rollback capability
# =============================================================================
# Usage:
#   ./scripts/deploy.sh              # build + deploy current commit
#   ./scripts/deploy.sh --build-only # build images without deploying
#   ./scripts/deploy.sh --rollback   # restore previous deployment
#   ./scripts/deploy.sh --status     # show current deployment status
#
# Environment variables (optional overrides):
#   REGISTRY          -- container registry prefix (e.g. "ghcr.io/org/")
#   COMPOSE_PROJECT   -- compose project name (default: airflow-crawler-system)
#   HEALTH_TIMEOUT    -- seconds to wait for health checks (default: 120)
#   HEALTH_INTERVAL   -- seconds between health polls (default: 5)
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${PROJECT_DIR}/docker-compose.yml"
COMPOSE_PROD_FILE="${PROJECT_DIR}/docker-compose.prod.yml"
STATE_DIR="${PROJECT_DIR}/.deploy"

REGISTRY="${REGISTRY:-}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-airflow-crawler-system}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-120}"
HEALTH_INTERVAL="${HEALTH_INTERVAL:-5}"

# Image names
SERVICES=(crawler-api crawler-frontend crawler-airflow)

# Compose command
COMPOSE_CMD="docker compose -p ${COMPOSE_PROJECT} -f ${COMPOSE_FILE} -f ${COMPOSE_PROD_FILE}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly NC='\033[0m' # No Color

log_info()  { echo -e "${BLUE}[INFO]${NC}  $(date '+%H:%M:%S') $*"; }
log_ok()    { echo -e "${GREEN}[OK]${NC}    $(date '+%H:%M:%S') $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $(date '+%H:%M:%S') $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $(date '+%H:%M:%S') $*" >&2; }

die() { log_error "$@"; exit 1; }

ensure_state_dir() {
    mkdir -p "${STATE_DIR}"
}

get_git_sha() {
    git -C "${PROJECT_DIR}" rev-parse --short HEAD 2>/dev/null || echo "unknown"
}

get_timestamp() {
    date '+%Y%m%d-%H%M%S'
}

# ---------------------------------------------------------------------------
# Tag management
# ---------------------------------------------------------------------------
generate_tag() {
    local sha timestamp
    sha="$(get_git_sha)"
    timestamp="$(get_timestamp)"
    echo "${timestamp}-${sha}"
}

save_current_tags() {
    ensure_state_dir
    local tag_file="${STATE_DIR}/previous-tags"

    log_info "Saving current image tags for rollback..."

    # Save currently running image tags
    local running_tags=""
    for service in api frontend; do
        local image
        image=$(docker compose -p "${COMPOSE_PROJECT}" \
            -f "${COMPOSE_FILE}" -f "${COMPOSE_PROD_FILE}" \
            ps --format json "${service}" 2>/dev/null \
            | python3 -c "import sys,json; data=json.load(sys.stdin); print(data.get('Image',''))" 2>/dev/null \
            || echo "")
        if [[ -n "${image}" ]]; then
            running_tags+="${service}=${image}\n"
        fi
    done

    if [[ -n "${running_tags}" ]]; then
        echo -e "${running_tags}" > "${tag_file}"
        log_ok "Saved tags to ${tag_file}"
    else
        # Fallback: save the IMAGE_TAG env var if set
        if [[ -f "${STATE_DIR}/current-tag" ]]; then
            cp "${STATE_DIR}/current-tag" "${STATE_DIR}/previous-tag"
            log_ok "Saved previous tag from state file"
        else
            log_warn "No running containers found to save tags from"
        fi
    fi
}

save_deployed_tag() {
    ensure_state_dir
    local tag="$1"
    echo "${tag}" > "${STATE_DIR}/current-tag"
    echo "$(date -Iseconds) ${tag}" >> "${STATE_DIR}/deploy-history"
    log_ok "Recorded deployment: ${tag}"
}

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
build_images() {
    local tag="$1"
    log_info "Building images with tag: ${tag}"

    log_info "Building API image..."
    docker build \
        --target production \
        --tag "${REGISTRY}crawler-api:${tag}" \
        --tag "${REGISTRY}crawler-api:latest" \
        --file "${PROJECT_DIR}/api/Dockerfile" \
        "${PROJECT_DIR}/api"
    log_ok "API image built"

    log_info "Building frontend image..."
    docker build \
        --target production \
        --tag "${REGISTRY}crawler-frontend:${tag}" \
        --tag "${REGISTRY}crawler-frontend:latest" \
        --file "${PROJECT_DIR}/frontend/Dockerfile" \
        "${PROJECT_DIR}/frontend"
    log_ok "Frontend image built"

    log_info "Building Airflow image..."
    # Airflow build context is the project root (needs airflow/dags + crawlers/)
    docker build \
        --tag "${REGISTRY}crawler-airflow:${tag}" \
        --tag "${REGISTRY}crawler-airflow:latest" \
        --file "${PROJECT_DIR}/airflow/Dockerfile" \
        "${PROJECT_DIR}"
    log_ok "Airflow image built"

    log_ok "All images built successfully: ${tag}"
}

# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------
wait_for_healthy() {
    local service="$1"
    local url="$2"
    local elapsed=0

    log_info "Waiting for ${service} to become healthy (timeout: ${HEALTH_TIMEOUT}s)..."

    while [[ ${elapsed} -lt ${HEALTH_TIMEOUT} ]]; do
        local status
        status=$(docker compose -p "${COMPOSE_PROJECT}" \
            -f "${COMPOSE_FILE}" -f "${COMPOSE_PROD_FILE}" \
            ps --format json "${service}" 2>/dev/null \
            | python3 -c "import sys,json; data=json.load(sys.stdin); print(data.get('Health',''))" 2>/dev/null \
            || echo "")

        if [[ "${status}" == "healthy" ]]; then
            log_ok "${service} is healthy (${elapsed}s)"
            return 0
        fi

        # Also check if container exited
        local state
        state=$(docker compose -p "${COMPOSE_PROJECT}" \
            -f "${COMPOSE_FILE}" -f "${COMPOSE_PROD_FILE}" \
            ps --format json "${service}" 2>/dev/null \
            | python3 -c "import sys,json; data=json.load(sys.stdin); print(data.get('State',''))" 2>/dev/null \
            || echo "")

        if [[ "${state}" == "exited" || "${state}" == "dead" ]]; then
            log_error "${service} container has exited unexpectedly"
            return 1
        fi

        sleep "${HEALTH_INTERVAL}"
        elapsed=$((elapsed + HEALTH_INTERVAL))
    done

    log_error "${service} did not become healthy within ${HEALTH_TIMEOUT}s"
    return 1
}

check_all_health() {
    local failed=0

    # Core services that must be healthy
    local -A health_services=(
        ["postgres"]="internal"
        ["mongodb"]="internal"
        ["api"]="http://localhost:8000/health"
        ["frontend"]="http://localhost:8080/nginx-health"
        ["airflow-webserver"]="http://localhost:8080/health"
    )

    for service in "${!health_services[@]}"; do
        if ! wait_for_healthy "${service}" "${health_services[$service]}"; then
            log_error "Health check failed for: ${service}"
            failed=1
        fi
    done

    return ${failed}
}

# ---------------------------------------------------------------------------
# Deploy
# ---------------------------------------------------------------------------
deploy() {
    local tag="$1"

    log_info "Deploying with IMAGE_TAG=${tag}"

    # Save current state for rollback
    save_current_tags

    # Export the tag for compose interpolation
    export IMAGE_TAG="${tag}"

    # Pull/use local images and recreate containers
    log_info "Starting services..."
    ${COMPOSE_CMD} up -d --remove-orphans

    log_info "Waiting for services to stabilize..."
    sleep 10

    # Run health checks
    if check_all_health; then
        save_deployed_tag "${tag}"
        log_ok "============================================="
        log_ok "Deployment successful: ${tag}"
        log_ok "============================================="
        return 0
    else
        log_error "============================================="
        log_error "Deployment FAILED health checks: ${tag}"
        log_error "============================================="
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------
rollback() {
    ensure_state_dir
    local previous_tag_file="${STATE_DIR}/previous-tag"
    local history_file="${STATE_DIR}/deploy-history"

    if [[ -f "${previous_tag_file}" ]]; then
        local prev_tag
        prev_tag="$(cat "${previous_tag_file}")"
        log_warn "Rolling back to previous tag: ${prev_tag}"
        export IMAGE_TAG="${prev_tag}"
    elif [[ -f "${history_file}" ]]; then
        # Get the second-to-last entry from history
        local prev_tag
        prev_tag="$(tail -2 "${history_file}" | head -1 | awk '{print $2}')"
        if [[ -z "${prev_tag}" ]]; then
            die "No previous deployment found in history"
        fi
        log_warn "Rolling back to: ${prev_tag} (from deploy history)"
        export IMAGE_TAG="${prev_tag}"
    else
        die "No rollback state found. Cannot determine previous deployment."
    fi

    log_warn "Starting rollback..."
    ${COMPOSE_CMD} up -d --remove-orphans

    log_info "Waiting for services to stabilize after rollback..."
    sleep 10

    if check_all_health; then
        log_ok "============================================="
        log_ok "Rollback successful: ${IMAGE_TAG}"
        log_ok "============================================="
        echo "$(date -Iseconds) ROLLBACK:${IMAGE_TAG}" >> "${STATE_DIR}/deploy-history"
    else
        log_error "============================================="
        log_error "Rollback ALSO FAILED. Manual intervention required."
        log_error "============================================="
        show_status
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------
show_status() {
    echo ""
    log_info "=== Deployment Status ==="
    echo ""

    # Show running containers
    ${COMPOSE_CMD} ps 2>/dev/null || true

    echo ""

    # Show recent history
    if [[ -f "${STATE_DIR}/deploy-history" ]]; then
        log_info "=== Recent Deployments (last 10) ==="
        tail -10 "${STATE_DIR}/deploy-history"
    fi

    echo ""

    # Show current tag
    if [[ -f "${STATE_DIR}/current-tag" ]]; then
        log_info "Current tag: $(cat "${STATE_DIR}/current-tag")"
    fi
}

# ---------------------------------------------------------------------------
# Preflight checks
# ---------------------------------------------------------------------------
preflight() {
    log_info "Running preflight checks..."

    # Check docker
    if ! command -v docker &>/dev/null; then
        die "docker is not installed or not in PATH"
    fi

    # Check docker compose
    if ! docker compose version &>/dev/null; then
        die "docker compose plugin is not available"
    fi

    # Check compose files exist
    if [[ ! -f "${COMPOSE_FILE}" ]]; then
        die "Missing ${COMPOSE_FILE}"
    fi
    if [[ ! -f "${COMPOSE_PROD_FILE}" ]]; then
        die "Missing ${COMPOSE_PROD_FILE}"
    fi

    # Check .env file exists
    if [[ ! -f "${PROJECT_DIR}/.env" ]]; then
        log_warn ".env file not found. Ensure all required environment variables are set."
    fi

    # Verify required env vars for production
    local required_vars=(
        POSTGRES_USER POSTGRES_PASSWORD POSTGRES_DB
        MONGO_ROOT_USERNAME MONGO_ROOT_PASSWORD
        AIRFLOW_FERNET_KEY
    )

    local missing=0
    for var in "${required_vars[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            log_error "Required environment variable not set: ${var}"
            missing=1
        fi
    done

    if [[ ${missing} -eq 1 ]]; then
        die "Missing required environment variables. Check your .env file."
    fi

    log_ok "Preflight checks passed"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    local action="${1:-deploy}"

    case "${action}" in
        --build-only|build)
            log_info "Build-only mode"
            local tag
            tag="$(generate_tag)"
            build_images "${tag}"
            ;;

        --rollback|rollback)
            log_info "Rollback mode"
            # Load .env if present
            if [[ -f "${PROJECT_DIR}/.env" ]]; then
                set -a; source "${PROJECT_DIR}/.env"; set +a
            fi
            rollback
            ;;

        --status|status)
            # Load .env if present
            if [[ -f "${PROJECT_DIR}/.env" ]]; then
                set -a; source "${PROJECT_DIR}/.env"; set +a
            fi
            show_status
            ;;

        --help|-h|help)
            echo "Usage: $0 [--build-only|--rollback|--status|--help]"
            echo ""
            echo "  (default)      Build images and deploy to production"
            echo "  --build-only   Build and tag images without deploying"
            echo "  --rollback     Restore the previous deployment"
            echo "  --status       Show current deployment status"
            echo "  --help         Show this help message"
            ;;

        *)
            # Load .env if present
            if [[ -f "${PROJECT_DIR}/.env" ]]; then
                set -a; source "${PROJECT_DIR}/.env"; set +a
            fi

            preflight

            local tag
            tag="$(generate_tag)"

            build_images "${tag}"

            if deploy "${tag}"; then
                exit 0
            else
                log_warn "Deploy failed. Attempting automatic rollback..."
                rollback
                exit 1
            fi
            ;;
    esac
}

main "$@"
