# Development Environment Startup Script (PowerShell)
# Usage: .\scripts\start_dev.ps1

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  ETL Pipeline - Development Environment" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Check if MongoDB is running
Write-Host "[1/5] Checking MongoDB connection..." -ForegroundColor Yellow
try {
    $mongoCheck = mongosh --eval "db.runCommand({ping:1})" --quiet 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "MongoDB not running"
    }
    Write-Host "  ✅ MongoDB is running" -ForegroundColor Green
} catch {
    Write-Host "  ❌ MongoDB is not running on localhost:27017" -ForegroundColor Red
    Write-Host "  Please start MongoDB first:" -ForegroundColor Yellow
    Write-Host "    - Windows: net start MongoDB" -ForegroundColor Gray
    Write-Host "    - Or run: mongod" -ForegroundColor Gray
    exit 1
}

# Initialize MongoDB collections
Write-Host ""
Write-Host "[2/5] Initializing MongoDB collections..." -ForegroundColor Yellow
try {
    mongosh crawler_system_dev mongodb/init-scripts/init.js --quiet
    Write-Host "  ✅ Collections initialized" -ForegroundColor Green
} catch {
    Write-Host "  ⚠️ Warning: Could not run init script (collections may already exist)" -ForegroundColor Yellow
}

# Start Docker services
Write-Host ""
Write-Host "[3/5] Starting Docker services..." -ForegroundColor Yellow
try {
    docker-compose -f docker-compose.dev.yml --env-file .env.dev up -d
    if ($LASTEXITCODE -ne 0) {
        throw "Docker compose failed"
    }
    Write-Host "  ✅ Docker services starting..." -ForegroundColor Green
} catch {
    Write-Host "  ❌ Failed to start Docker services" -ForegroundColor Red
    exit 1
}

# Wait for services
Write-Host ""
Write-Host "[4/5] Waiting for services to be ready (30s)..." -ForegroundColor Yellow
$spinner = @('|', '/', '-', '\')
for ($i = 0; $i -lt 30; $i++) {
    Write-Host -NoNewline "`r  $($spinner[$i % 4]) $i/30 seconds..."
    Start-Sleep -Seconds 1
}
Write-Host "`r  ✅ Wait complete                    " -ForegroundColor Green

# Test the setup
Write-Host ""
Write-Host "[5/5] Testing setup..." -ForegroundColor Yellow
python scripts/test_dev_setup.py --init-data

Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host "  Development Environment Ready!" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Airflow UI:  " -NoNewline; Write-Host "http://localhost:18080" -ForegroundColor Cyan
Write-Host "  API:         " -NoNewline; Write-Host "http://localhost:8000" -ForegroundColor Cyan
Write-Host "  API Docs:    " -NoNewline; Write-Host "http://localhost:8000/docs" -ForegroundColor Cyan
Write-Host "  MongoDB:     " -NoNewline; Write-Host "localhost:27017" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Credentials:" -ForegroundColor Yellow
Write-Host "    Airflow: airflow / airflow" -ForegroundColor Gray
Write-Host ""
Write-Host "  To stop: " -NoNewline
Write-Host "docker-compose -f docker-compose.dev.yml down" -ForegroundColor Yellow
Write-Host ""
