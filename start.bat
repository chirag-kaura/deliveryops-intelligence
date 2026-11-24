@echo off
setlocal
cd /d "%~dp0"
set "PYTHONPATH=%~dp0src"

echo ============================================================================
echo   DeliveryOps Intelligence v0.6.0 - E-Commerce + Full 5-Tool Cloud Launcher
echo ============================================================================

if not exist ".venv\Scripts\python.exe" goto :missing_venv
if not exist ".env" copy /Y ".env.example" ".env" >nul 2>&1
if not exist "logs" mkdir "logs"

set "API_PORT=8001"
set "UI_PORT=8501"

if /I "%~1"=="--local-only" goto :skip_docker
echo [1/5] Starting 5 Cloud Tools - Postgres+Adminer, Redpanda+Console, MinIO, Jaeger, LocalStack...
docker compose --profile all up -d
if errorlevel 1 echo [WARN] Docker Compose unavailable - continuing with local fallbacks.
goto :run_dag

:skip_docker
echo [1/5] Running in --local-only mode - skipping Docker containers.

:run_dag
echo [2/5] Materializing 5-Asset Medallion DAG...
.venv\Scripts\python.exe scripts\run_platform.py
if errorlevel 1 goto :dag_failed

if /I "%~1"=="--local-only" goto :start_servers
echo [3/5] Syncing Postgres 16, Redpanda, MinIO S3, Jaeger OTLP, and LocalStack AWS...
.venv\Scripts\python.exe scripts\sync_live_containers.py

:start_servers
echo [4/5] Starting FastAPI (:8001) and Streamlit E-Commerce Workbench (:8501)...
taskkill /FI "WINDOWTITLE eq DeliveryOps-FastAPI*" /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq DeliveryOps-Streamlit*" /T /F >nul 2>&1
start "DeliveryOps-FastAPI" /min cmd /c ".venv\Scripts\python.exe -m uvicorn deliveryops.api.app:app --host 127.0.0.1 --port %API_PORT% > logs\fastapi.log 2>&1"
start "DeliveryOps-Streamlit" /min cmd /c ".venv\Scripts\python.exe -m streamlit run src\deliveryops\dashboard\app.py --server.address 127.0.0.1 --server.port %UI_PORT% --server.headless true > logs\streamlit.log 2>&1"

echo [INFO] Waiting for UIs to respond...
.venv\Scripts\python.exe scripts\wait_for_services.py

if /I "%~1"=="--no-browser" goto :skip_browser
if /I "%~2"=="--no-browser" goto :skip_browser
echo [5/5] Opening E-Commerce Storefront and all 5 Tool UIs in your browser...
start "" "http://127.0.0.1:%UI_PORT%"
start "" "http://127.0.0.1:%API_PORT%/docs"
if /I "%~1"=="--local-only" goto :skip_browser
start "" "http://127.0.0.1:8082/?pgsql=postgres&username=deliveryops&db=deliveryops&ns=public"
start "" "http://127.0.0.1:8081/topics"
start "" "http://127.0.0.1:9001"
start "" "http://127.0.0.1:16686"
start "" "http://127.0.0.1:4566/deliveryops-cloud-archive"

:skip_browser
echo.
echo ============================================================================
echo   DeliveryOps Intelligence - ALL SERVICE UIs ARE LIVE
echo   - E-Commerce & Pipeline UI  : http://127.0.0.1:%UI_PORT%
echo   - FastAPI Swagger UI        : http://127.0.0.1:%API_PORT%/docs
echo   - 1. Postgres 16 Adminer UI : http://127.0.0.1:8082
echo   - 2. Redpanda Console UI    : http://127.0.0.1:8081/topics
echo   - 3. MinIO S3 Console       : http://127.0.0.1:9001 (minioadmin / minioadmin_local_dev)
echo   - 4. Jaeger Trace UI        : http://127.0.0.1:16686
echo   - 5. LocalStack AWS S3      : http://127.0.0.1:4566/deliveryops-cloud-archive
echo   - To Stop Everything        : stop.bat
echo ============================================================================
endlocal
exit /b 0

:missing_venv
echo [ERROR] Virtual environment not found at .venv\Scripts\python.exe
echo         Create it with: py -3.11 -m venv .venv
endlocal
exit /b 1

:dag_failed
echo [ERROR] Platform asset materialization failed.
endlocal
exit /b 1
