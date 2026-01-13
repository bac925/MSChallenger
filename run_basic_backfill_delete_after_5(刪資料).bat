@echo off
cd /d %~dp0
call venv\Scripts\activate

python tools_basic_backfill.py --retry-pending --include-not-found --delete-after-attempts 5 --apply-delete --concurrency 60

pause
