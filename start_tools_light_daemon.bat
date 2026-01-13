@echo off
cd /d %~dp0

call venv\Scripts\activate

python tools_light_daemon.py

pause
