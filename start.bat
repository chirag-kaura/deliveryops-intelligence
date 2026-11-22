@echo off
setlocal
cd /d "%~dp0"
set "PYTHONPATH=%~dp0src"

echo ============================================================================
echo   DeliveryOps Intelligence v0.6.0 - One-Click Platform Launcher
echo ============================================================================

if not exist ".venv\Scripts\python.exe" goto :missing_venv
if not exist ".env" copy /Y ".env.example" ".env" >nul 2>&1
if not exist "logs" mkdir "logs"

set "API_PORT=8001"
set "UI_PORT=8501"

if /I "%~1"=="--with-docker" goto :start_docker
echo [1/4] Running in Hybrid Local-First Mode - 0 MB extra Docker RAM.
echo       Tip: Run "start.bat --with-docker" to also start Docker containers.
goto :run_dag

:start_docker
echo [1/4] Starting DeliveryOps Docker Compose core profile...
docker compose --profile core up -d

:run_dag
echo [2/4] Materializing 5-Asset Medallion DAG...
.venv\Scripts\python.exe scripts\run_platform.py
if errorlevel 1 goto :dag_failed

echo [3/4] Starting FastAPI Serving Layer on http://127.0.0.1:%API_PORT% ...
start "DeliveryOps-FastAPI" /min cmd /c ".venv\Scripts\python.exe -m uvicorn deliveryops.api.app:app --host 127.0.0.1 --port %API_PORT% > logs\fastapi.log 2>&1"

echo [4/4] Starting Streamlit Control Plane Dashboard on http://127.0.0.1:%UI_PORT% ...
start "DeliveryOps-Streamlit" /min cmd /c ".venv\Scripts\python.exe -m streamlit run src\deliveryops\dashboard\app.py --server.address 127.0.0.1 --server.port %UI_PORT% --server.headless true > logs\streamlit.log 2>&1"

echo [INFO] Waiting for FastAPI and Streamlit servers to bind ports...
.venv\Scripts\python.exe scripts\wait_for_services.py

if /I "%~1"=="--no-browser" goto :skip_browser
if /I "%~2"=="--no-browser" goto :skip_browser
start "" "http://127.0.0.1:%UI_PORT%"
start "" "http://127.0.0.1:%API_PORT%/docs"

:skip_browser
echo.
echo ============================================================================
echo   DeliveryOps Intelligence is RUNNING
echo   - Streamlit Dashboard : http://127.0.0.1:%UI_PORT%
echo   - FastAPI Swagger UI  : http://127.0.0.1:%API_PORT%/docs
echo   - FastAPI Health      : http://127.0.0.1:%API_PORT%/health
echo   - Infra Status Probe  : http://127.0.0.1:%API_PORT%/infra/status
echo   - OTel Trace Spans    : http://127.0.0.1:%API_PORT%/observability/traces
echo   - Logs                : logs\fastapi.log , logs\streamlit.log
echo   - To Stop Platform    : stop.bat
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
