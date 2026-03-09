@echo off
REM ############################################################################################################
REM ##                                 ETL Pipeline Tool                                                      ##
REM ##                                                                                                        ##
REM ## This Script is developed by Shankar N G                                                                ##
REM ## It is to trigger the Batch Process of the ETL Pipeline Tool                                            ##
REM ## The Batch Process checks all tables and columns, makes a live call and extracts events                 ##
REM ## Filters and transforms the columns and inserts the data into the database                              ##
REM ## This process can be scheduled via Task Scheduler for automated recurring runs                          ##
REM ## Please edit the APP_DIR path to point to where the application is installed                            ##
REM ## Make sure the virtual environment and requirements are set up before running                            ##
REM ## This Script is for Windows Command Prompt / Task Scheduler                                             ##
REM ############################################################################################################

SET APP_DIR=E:\PythonProjects\ETLPipeline
SET LOG_DIR=%APP_DIR%\logs

IF NOT EXIST "%LOG_DIR%" mkdir "%LOG_DIR%"

FOR /F "tokens=1-6 delims=/:. " %%A IN ("%DATE% %TIME%") DO (
    SET LOG_FILE=%LOG_DIR%\etl_batchprocess_%%C%%B%%A_%%D%%E%%F.log
)

echo ==============================================
echo   Starting ETL Pipeline Tool
echo   Component  : Batch Process
echo   Started at : %DATE% %TIME%
echo   Logs       : %LOG_FILE%
echo ==============================================

IF NOT EXIST "%APP_DIR%\etlpipeline\batchprocess\app" (
    echo ERROR: Batch process directory not found: %APP_DIR%\etlpipeline\batchprocess\app
    pause
    exit /b 1
)
cd /d "%APP_DIR%\etlpipeline\batchprocess\app"

IF EXIST "%APP_DIR%\.venv\Scripts\activate.bat" (
    call "%APP_DIR%\.venv\Scripts\activate.bat"
    echo Virtual environment activated.
) ELSE (
    echo WARNING: Virtual environment not found. Running with system Python.
)

START "ETLPipeline-BatchProcess" /B python -m main >> "%LOG_FILE%" 2>&1

echo ==============================================
echo   ETL Batch Process is running in the background
echo   To stop: End python.exe in Task Manager
echo   Logs: %LOG_FILE%
echo ==============================================

IF EXIST "%APP_DIR%\.venv\Scripts\deactivate.bat" (
    call "%APP_DIR%\.venv\Scripts\deactivate.bat"
)

pause
