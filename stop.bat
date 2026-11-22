@echo off
setlocal EnableDelayedExpansion
cd /d "%~dp0"

echo ============================================================================
echo   DeliveryOps Intelligence - Platform Shutdown
echo ============================================================================

echo [1/3] Stopping FastAPI Serving Layer (DeliveryOps-FastAPI / port 8001)...
taskkill /FI "WINDOWTITLE eq DeliveryOps-FastAPI*" /T /F >nul 2>&1
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8001 " ^| findstr "LISTENING"') do (
    taskkill /PID %%a /F >nul 2>&1
)

echo [2/3] Stopping Streamlit Dashboard (DeliveryOps-Streamlit / port 8501)...
taskkill /FI "WINDOWTITLE eq DeliveryOps-Streamlit*" /T /F >nul 2>&1
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8501 " ^| findstr "LISTENING"') do (
    taskkill /PID %%a /F >nul 2>&1
)

echo [3/3] Stopping any running DeliveryOps Docker Compose services (if active)...
docker compose --profile all stop >nul 2>&1

echo.
echo [OK] All DeliveryOps Intelligence services have been stopped cleanly.
echo ============================================================================
endlocal
