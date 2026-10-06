@echo off
echo ===================================================
echo 🛑 Stopping Crypto Streaming Pipeline
echo ===================================================

echo [1/2] Shutting down Docker containers...
docker-compose down

echo [2/2] Closing pipeline terminal windows...
taskkill /FI "WindowTitle eq Binance Producer*" /T /F >nul 2>&1
taskkill /FI "WindowTitle eq PySpark Processor*" /T /F >nul 2>&1
taskkill /FI "WindowTitle eq Streamlit Dashboard*" /T /F >nul 2>&1

echo ===================================================
echo Pipeline successfully stopped and cleaned up!
echo ===================================================
pause