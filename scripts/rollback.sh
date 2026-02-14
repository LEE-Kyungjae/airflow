#!/usr/bin/env bash
# =============================================================================
# scripts/rollback.sh -- Deployment rollback with E2E smoke verification
# =============================================================================
# Usage:
#   ./scripts/rollback.sh                    # rollback to previous version
#   ./scripts/rollback.sh --to <tag>         # rollback to specific version
#   ./scripts/rollback.sh --list             # list deployment history
#   ./scripts/rollback.sh --verify           # run smoke tests on current deployment
#   ./scripts/rollback.sh --dry-run          # show what would happen without executing
#
# Environment variables:
#   COMPOSE_PROJECT   -- compose project name (default: airflow-crawler-system)
#   HEALTH_TIMEOUT    -- seconds to wait for health checks (default: 120)
#   SMOKE_TEST        -- run E2E smoke tests after rollback (default: true)
#   API_URL           -- API base URL for smoke tests (default: http://localhost:8000)
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
HISTORY_FILE="${STATE_DIR}/deploy-history"
ROLLBACK_LOG="${STATE_DIR}/rollback.log"

COMPOSE_PROJECT="${COMPOSE_PROJECT:-airflow-crawler-system}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-120}"
HEALTH_INTERVAL="${HEALTH_INTERVAL:-5}"
SMOKE_TEST="${SMOKE_TEST:-true}"
API_URL="${API_URL:-http://localhost:8000}"
DRY_RUN="${DRY_RUN:-false}"

COMPOSE_CMD="docker compose -p ${COMPOSE_PROJECT} -f ${COMPOSE_FILE}"
if [[ -f "${COMPOSE_PROD_FILE}" ]]; then
    COMPOSE_CMD="${COMPOSE_CMD} -f ${COMPOSE_PROD_FILE}"
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly CYAN='\033[0;36m'
readonly NC='\033[0m'

log_info()  { echo -e "${BLUE}[INFO]${NC}  $(date '+%H:%M:%S') $*"; }
log_ok()    { echo -e "${GREEN}[OK]${NC}    $(date '+%H:%M:%S') $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $(date '+%H:%M:%S') $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $(date '+%H:%M:%S') $*" >&2; }
log_step()  { echo -e "${CYAN}[STEP]${NC}  $(date '+%H:%M:%S') $*"; }

die() { log_error "$@"; exit 1; }

ensure_state_dir() {
    mkdir -p "${STATE_DIR}"
}

append_log() {
    ensure_state_dir
    echo "$(date -Iseconds) $*" >> "${ROLLBACK_LOG}"
}

# ---------------------------------------------------------------------------
# History management
# ---------------------------------------------------------------------------
list_history() {
    ensure_state_dir

    echo ""
    echo -e "${CYAN}=== Deployment History ===${NC}"
    echo ""

    if [[ ! -f "${HISTORY_FILE}" ]]; then
        echo "  No deployment history found."
        echo ""
        return 0
    fi

    local count=0
    local current_tag=""

    if [[ -f "${STATE_DIR}/current-tag" ]]; then
        current_tag="$(cat "${STATE_DIR}/current-tag")"
    fi

    echo -e "  ${BLUE}#   Timestamp                    Tag                           Type${NC}"
    echo "  --- --------------------------  ----------------------------  --------"

    while IFS= read -r line; do
        count=$((count + 1))
        local ts tag type
        ts=$(echo "${line}" | awk '{print $1}')
        tag=$(echo "${line}" | awk '{print $2}')

        if [[ "${tag}" == ROLLBACK:* ]]; then
            type="ROLLBACK"
            tag="${tag#ROLLBACK:}"
        else
            type="deploy"
        fi

        local marker=""
        if [[ "${tag}" == "${current_tag}" ]]; then
            marker=" ← current"
        fi

        printf "  %-3d %-26s  %-28s  %-8s%s\n" "${count}" "${ts}" "${tag}" "${type}" "${marker}"
    done < "${HISTORY_FILE}"

    echo ""
    echo "  Total deployments: ${count}"
    echo ""
}

get_previous_tag() {
    if [[ ! -f "${HISTORY_FILE}" ]]; then
        die "No deployment history found. Cannot determine previous version."
    fi

    local current_tag=""
    if [[ -f "${STATE_DIR}/current-tag" ]]; then
        current_tag="$(cat "${STATE_DIR}/current-tag")"
    fi

    # Find the most recent non-current, non-rollback deploy entry
    local prev_tag=""
    while IFS= read -r line; do
        local tag
        tag=$(echo "${line}" | awk '{print $2}')

        # Skip rollback entries
        if [[ "${tag}" == ROLLBACK:* ]]; then
            continue
        fi

        # Skip current tag
        if [[ "${tag}" == "${current_tag}" ]]; then
            continue
        fi

        prev_tag="${tag}"
    done < "${HISTORY_FILE}"

    # If no previous deploy found, try the second-to-last entry
    if [[ -z "${prev_tag}" ]]; then
        prev_tag=$(tail -2 "${HISTORY_FILE}" | head -1 | awk '{print $2}')
        prev_tag="${prev_tag#ROLLBACK:}"
    fi

    if [[ -z "${prev_tag}" ]]; then
        die "Cannot determine previous deployment tag from history."
    fi

    echo "${prev_tag}"
}

# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------
wait_for_healthy() {
    local elapsed=0

    log_info "Waiting for API health check (timeout: ${HEALTH_TIMEOUT}s)..."

    while [[ ${elapsed} -lt ${HEALTH_TIMEOUT} ]]; do
        if curl -sf "${API_URL}/health" > /dev/null 2>&1; then
            log_ok "API is healthy (${elapsed}s)"
            return 0
        fi

        sleep "${HEALTH_INTERVAL}"
        elapsed=$((elapsed + HEALTH_INTERVAL))
    done

    log_error "API did not become healthy within ${HEALTH_TIMEOUT}s"
    return 1
}

# ---------------------------------------------------------------------------
# Smoke tests
# ---------------------------------------------------------------------------
run_smoke_tests() {
    if [[ "${SMOKE_TEST}" != "true" ]]; then
        log_info "Smoke tests disabled (SMOKE_TEST=${SMOKE_TEST})"
        return 0
    fi

    log_step "Running post-rollback smoke tests..."

    local failed=0

    # Test 1: Health endpoint
    log_info "Smoke: GET /health"
    local health_resp
    health_resp=$(curl -sf "${API_URL}/health" 2>/dev/null) || { log_error "Smoke FAIL: /health unreachable"; failed=1; }

    if [[ ${failed} -eq 0 ]]; then
        local status
        status=$(echo "${health_resp}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null || echo "")
        if [[ "${status}" != "healthy" ]]; then
            log_error "Smoke FAIL: /health status='${status}' (expected 'healthy')"
            failed=1
        else
            log_ok "Smoke PASS: /health → healthy"
        fi
    fi

    # Test 2: Root endpoint
    log_info "Smoke: GET /"
    local root_resp
    root_resp=$(curl -sf "${API_URL}/" 2>/dev/null) || { log_error "Smoke FAIL: / unreachable"; failed=1; }
    if [[ ${failed} -eq 0 ]] && echo "${root_resp}" | python3 -c "import sys,json; d=json.load(sys.stdin); assert 'version' in d" 2>/dev/null; then
        log_ok "Smoke PASS: / → has version"
    fi

    # Test 3: OpenAPI schema
    log_info "Smoke: GET /openapi.json"
    local openapi_resp
    openapi_resp=$(curl -sf "${API_URL}/openapi.json" 2>/dev/null) || { log_warn "Smoke WARN: /openapi.json unreachable"; }
    if echo "${openapi_resp}" | python3 -c "import sys,json; d=json.load(sys.stdin); assert 'openapi' in d and 'paths' in d" 2>/dev/null; then
        log_ok "Smoke PASS: /openapi.json → valid schema"
    fi

    # Test 4: Dashboard endpoint
    log_info "Smoke: GET /api/dashboard/stats"
    local dash_status
    dash_status=$(curl -o /dev/null -s -w "%{http_code}" "${API_URL}/api/dashboard/stats" 2>/dev/null || echo "000")
    if [[ "${dash_status}" -ge 200 && "${dash_status}" -lt 500 ]]; then
        log_ok "Smoke PASS: /api/dashboard/stats → HTTP ${dash_status}"
    else
        log_warn "Smoke WARN: /api/dashboard/stats → HTTP ${dash_status}"
    fi

    # Test 5: Sources endpoint
    log_info "Smoke: GET /api/sources/"
    local src_status
    src_status=$(curl -o /dev/null -s -w "%{http_code}" "${API_URL}/api/sources/" 2>/dev/null || echo "000")
    if [[ "${src_status}" -ge 200 && "${src_status}" -lt 500 ]]; then
        log_ok "Smoke PASS: /api/sources/ → HTTP ${src_status}"
    else
        log_warn "Smoke WARN: /api/sources/ → HTTP ${src_status}"
    fi

    # Test 6: Error handling (404)
    log_info "Smoke: GET /api/sources/000000000000000000000000 (expect 4xx)"
    local err_status
    err_status=$(curl -o /dev/null -s -w "%{http_code}" "${API_URL}/api/sources/000000000000000000000000" 2>/dev/null || echo "000")
    if [[ "${err_status}" -ge 400 && "${err_status}" -lt 500 ]]; then
        log_ok "Smoke PASS: 404 handling → HTTP ${err_status}"
    else
        log_warn "Smoke WARN: 404 handling → HTTP ${err_status} (expected 4xx)"
    fi

    echo ""
    if [[ ${failed} -eq 0 ]]; then
        log_ok "All smoke tests passed"
        return 0
    else
        log_error "Some smoke tests failed"
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Rollback execution
# ---------------------------------------------------------------------------
do_rollback() {
    local target_tag="$1"

    log_step "Rolling back to: ${target_tag}"
    append_log "ROLLBACK_START target=${target_tag}"

    if [[ "${DRY_RUN}" == "true" ]]; then
        log_warn "[DRY RUN] Would rollback to tag: ${target_tag}"
        log_warn "[DRY RUN] Would run: IMAGE_TAG=${target_tag} ${COMPOSE_CMD} up -d --remove-orphans"
        log_warn "[DRY RUN] Would verify health and run smoke tests"
        return 0
    fi

    # Export tag for compose interpolation
    export IMAGE_TAG="${target_tag}"

    # Restart services with previous images
    log_info "Restarting services with IMAGE_TAG=${target_tag}..."
    ${COMPOSE_CMD} up -d --remove-orphans

    # Wait for services
    log_info "Waiting for services to stabilize..."
    sleep 10

    # Health check
    if ! wait_for_healthy; then
        log_error "Rollback failed: services not healthy"
        append_log "ROLLBACK_FAIL target=${target_tag} reason=health_check"
        return 1
    fi

    # Run smoke tests
    if ! run_smoke_tests; then
        log_error "Rollback failed: smoke tests failed"
        append_log "ROLLBACK_FAIL target=${target_tag} reason=smoke_tests"
        return 1
    fi

    # Record rollback
    ensure_state_dir
    echo "${target_tag}" > "${STATE_DIR}/current-tag"
    echo "$(date -Iseconds) ROLLBACK:${target_tag}" >> "${HISTORY_FILE}"
    append_log "ROLLBACK_SUCCESS target=${target_tag}"

    log_ok "============================================="
    log_ok "Rollback successful: ${target_tag}"
    log_ok "============================================="
}

# ---------------------------------------------------------------------------
# Verify current deployment
# ---------------------------------------------------------------------------
verify_deployment() {
    log_step "Verifying current deployment..."

    if ! wait_for_healthy; then
        log_error "Current deployment is unhealthy"
        return 1
    fi

    if ! run_smoke_tests; then
        log_error "Current deployment failed smoke tests"
        return 1
    fi

    log_ok "Current deployment is verified and healthy"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Deployment rollback with E2E smoke verification.

Options:
  (default)         Rollback to the previous deployment
  --to <tag>        Rollback to a specific version tag
  --list            List deployment history
  --verify          Verify current deployment with smoke tests
  --dry-run         Show what would happen without executing
  --no-smoke        Skip smoke tests after rollback
  --help            Show this help message

Environment:
  COMPOSE_PROJECT   Compose project name (default: airflow-crawler-system)
  HEALTH_TIMEOUT    Health check timeout in seconds (default: 120)
  SMOKE_TEST        Run smoke tests (default: true)
  API_URL           API base URL (default: http://localhost:8000)

Examples:
  $(basename "$0")                     # rollback to previous version
  $(basename "$0") --to 20260213-abc   # rollback to specific tag
  $(basename "$0") --list              # show deployment history
  $(basename "$0") --verify            # check current deployment
  $(basename "$0") --dry-run           # preview rollback
EOF
}

main() {
    local action="rollback"
    local target_tag=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --to)
                shift
                target_tag="${1:-}"
                [[ -z "${target_tag}" ]] && die "--to requires a tag argument"
                ;;
            --list)
                action="list"
                ;;
            --verify)
                action="verify"
                ;;
            --dry-run)
                DRY_RUN="true"
                ;;
            --no-smoke)
                SMOKE_TEST="false"
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            *)
                die "Unknown option: $1 (use --help for usage)"
                ;;
        esac
        shift
    done

    # Load .env if present
    if [[ -f "${PROJECT_DIR}/.env" ]]; then
        set -a; source "${PROJECT_DIR}/.env"; set +a
    fi

    case "${action}" in
        list)
            list_history
            ;;
        verify)
            verify_deployment
            ;;
        rollback)
            if [[ -z "${target_tag}" ]]; then
                target_tag="$(get_previous_tag)"
            fi
            do_rollback "${target_tag}"
            ;;
    esac
}

main "$@"
