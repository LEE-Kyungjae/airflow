#!/bin/bash

# Crawler System Backup Script
# Creates backups of MongoDB data and crawler code

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BACKUP_DIR="${PROJECT_DIR}/backups"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

echo "=========================================="
echo "  Crawler System Backup"
echo "=========================================="

# Create backup directory
mkdir -p "$BACKUP_DIR"

# MongoDB backup
echo "Backing up MongoDB..."
MONGO_BACKUP_DIR="${BACKUP_DIR}/mongodb_${TIMESTAMP}"
mkdir -p "$MONGO_BACKUP_DIR"

docker-compose -f "${PROJECT_DIR}/docker-compose.yml" exec -T mongodb mongodump \
    --db=crawler_system \
    --out=/tmp/backup

docker cp $(docker-compose -f "${PROJECT_DIR}/docker-compose.yml" ps -q mongodb):/tmp/backup/crawler_system "${MONGO_BACKUP_DIR}/"

echo "✓ MongoDB backed up to: ${MONGO_BACKUP_DIR}"

# Backup dynamic DAGs
echo "Backing up dynamic DAGs..."
DAGS_BACKUP="${BACKUP_DIR}/dags_${TIMESTAMP}.tar.gz"
tar -czf "$DAGS_BACKUP" -C "${PROJECT_DIR}/airflow/dags" dynamic_crawlers 2>/dev/null || echo "No dynamic crawlers to backup"

echo "✓ DAGs backed up to: ${DAGS_BACKUP}"

# Backup environment
echo "Backing up configuration..."
CONFIG_BACKUP="${BACKUP_DIR}/config_${TIMESTAMP}"
mkdir -p "$CONFIG_BACKUP"
cp "${PROJECT_DIR}/.env" "${CONFIG_BACKUP}/.env" 2>/dev/null || true
cp "${PROJECT_DIR}/docker-compose.yml" "${CONFIG_BACKUP}/"

echo "✓ Configuration backed up to: ${CONFIG_BACKUP}"

# Cleanup old backups (keep last 7)
echo "Cleaning up old backups..."
cd "$BACKUP_DIR"
ls -dt mongodb_* 2>/dev/null | tail -n +8 | xargs rm -rf 2>/dev/null || true
ls -t dags_*.tar.gz 2>/dev/null | tail -n +8 | xargs rm -f 2>/dev/null || true
ls -dt config_* 2>/dev/null | tail -n +8 | xargs rm -rf 2>/dev/null || true

echo ""
echo "=========================================="
echo "  Backup Complete!"
echo "=========================================="
echo "Backup location: ${BACKUP_DIR}"
echo ""
