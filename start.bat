@echo off
REM ==================================================
REM MoriChallenger 啟動腳本
REM ==================================================

REM 切換到此 bat 所在目錄（避免從其他路徑啟動出錯）
cd /d %~dp0

REM 啟動虛擬環境
call venv\Scripts\activate

REM 顯示目前 Python 路徑（除錯用，可保留）
where python

REM 啟動 Streamlit
streamlit run app.py

REM 若 Streamlit 關閉，停在視窗顯示訊息
pause
