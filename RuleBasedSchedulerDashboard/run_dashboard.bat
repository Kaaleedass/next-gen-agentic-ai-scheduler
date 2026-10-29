@echo off
setlocal
cd /d "%~dp0"
echo Scheduler Studio - CrewAI Log and Output Monitor
echo.
python -c "import flask, dotenv, openai, crewai" >nul 2>nul
if errorlevel 1 (
  echo Required packages are missing. Installing requirements.txt...
  python -m pip install -r requirements.txt
  if errorlevel 1 (
    echo Dependency installation failed. Check pip output and your internet connection.
    pause
    exit /b 1
  )
)
if not exist .env (
  echo .env is missing. Copy .env.example to .env and add OPENAI_API_KEY.
  echo The deterministic monitor can still show status, but GPT commands/CrewAI analysis need the key.
  pause
)
python scheduler_dashboard_api.py
pause
