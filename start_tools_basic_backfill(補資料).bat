@echo off
cd /d %~dp0

call venv\Scripts\activate

python tools_basic_backfill.py --world "¬D¾ÔªÌ" --concurrency 60

pause
