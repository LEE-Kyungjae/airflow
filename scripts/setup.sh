#!/bin/bash

# Crawler System Setup Script
# This script initializes the Airflow-based crawler system

set -e

echo "=========================================="
echo "  Crawler System Setup"
echo "=========================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker first."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "Error: docker-compose is not installed."
    exit 1
fi

# Navigate to project directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

echo "Project directory: $PROJECT_DIR"

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "Creating .env from .env.example..."
    cp .env.example .env
    echo "Please edit .env file with your API keys and settings."
    echo "  - OPENAI_API_KEY: Your OpenAI API key"
    echo "  - SMTP settings for email alerts"
fi

# Set AIRFLOW_UID if not set (for Linux)
if [ -z "$AIRFLOW_UID" ]; then
    export AIRFLOW_UID=$(id -u)
    echo "AIRFLOW_UID=$AIRFLOW_UID" >> .env
fi

# Create necessary directories
echo "Creating directories..."
mkdir -p airflow/dags/dynamic_crawlers
mkdir -p airflow/plugins
mkdir -p airflow/config

# Pull Docker images
echo "Pulling Docker images..."
docker-compose pull

# Build custom images
echo "Building API image..."
docker-compose build api

# Initialize Airflow
echo "Initializing Airflow..."
docker-compose up airflow-init

# Start services
echo "Starting services..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 30

# Check service health
echo "Checking service health..."

# Check MongoDB
if docker-compose exec -T mongodb mongosh --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
    echo "✓ MongoDB is running"
else
    echo "✗ MongoDB is not responding"
fi

# Check Airflow
if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "✓ Airflow is running"
else
    echo "✗ Airflow is not responding (may still be starting)"
fi

# Check API
if curl -s http://localhost:8000/health > /dev/null 2>&1; then
    echo "✓ API is running"
else
    echo "✗ API is not responding"
fi

echo ""
echo "=========================================="
echo "  Setup Complete!"
echo "=========================================="
echo ""
echo "Access the services:"
echo "  - Airflow UI: http://localhost:8080 (airflow/airflow)"
echo "  - API Docs:   http://localhost:8000/docs"
echo "  - MongoDB:    localhost:27017"
echo ""
echo "To view logs:"
echo "  docker-compose logs -f"
echo ""
echo "To stop services:"
echo "  docker-compose down"
echo ""
