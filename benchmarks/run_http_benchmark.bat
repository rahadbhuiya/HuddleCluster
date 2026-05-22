@echo off
echo ============================================
echo   HuddleCluster Real HTTP Benchmark Setup
echo ============================================
echo.

echo [1/3] Installing dependencies...
pip install fastapi uvicorn httpx matplotlib numpy scipy -q
if %errorlevel% neq 0 (
    echo ERROR: pip install failed. Make sure Python venv is active.
    pause
    exit /b 1
)
echo Done.
echo.

echo [2/3] Verifying upstream server...
python -c "from fastapi import FastAPI; print('FastAPI OK')"
if %errorlevel% neq 0 (
    echo ERROR: FastAPI import failed.
    pause
    exit /b 1
)
echo.

echo [3/3] Running Real HTTP Benchmark...
echo This will take ~10 minutes.
echo.
python benchmark_http.py

echo.
echo Done! Check http_benchmark_results.png
pause
