@echo off
REM Development Environment Startup Script
REM Usage: scripts\start_dev.bat

echo ============================================
echo   ETL Pipeline - Development Environment
echo ============================================
echo.

REM Check if MongoDB is running
echo [1/5] Checking MongoDB connection...
mongosh --eval "db.runCommand({ping:1})" --quiet >nul 2>&1
if %errorlevel% neq 0 (
    echo   ❌ MongoDB is not running on localhost:27017
    echo   Please start MongoDB first:
    echo     - Windows: net start MongoDB
    echo     - Or run mongod manually
    exit /b 1
)
echo   ✅ MongoDB is running

REM Initialize MongoDB collections if needed
echo.
echo [2/5] Initializing MongoDB collections...
mongosh crawler_system_dev mongodb/init-scripts/init.js --quiet
if %errorlevel% neq 0 (
    echo   ⚠️ Warning: Could not run init script (collections may already exist)
) else (
    echo   ✅ Collections initialized
)

REM Start Docker services
echo.
echo [3/5] Starting Docker services...
docker-compose -f docker-compose.dev.yml --env-file .env.dev up -d
if %errorlevel% neq 0 (
    echo   ❌ Failed to start Docker services
    exit /b 1
)
echo   ✅ Docker services starting...

REM Wait for services to be ready
echo.
echo [4/5] Waiting for services to be ready...
timeout /t 30 /nobreak >nul

REM Test the setup
echo.
echo [5/5] Testing setup...
python scripts/test_dev_setup.py --init-data

echo.
echo ============================================
echo   Development Environment Ready!
echo ============================================
echo.
echo   Airflow UI:  http://localhost:18080
echo   API:         http://localhost:8000
echo   API Docs:    http://localhost:8000/docs
echo   MongoDB:     localhost:27017
echo.
echo   To stop: docker-compose -f docker-compose.dev.yml down
echo.
