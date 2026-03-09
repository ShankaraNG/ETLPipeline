@echo off
REM ############################################################################################################
REM ##                                 ETL Pipeline Tool                                                      ##
REM ##                                                                                                        ##
REM ## This Script is developed by Shankar N G                                                                ##
REM ## It is to trigger the Flask Application of the ETL Pipeline Tool                                        ##
REM ## The Flask App is used for adhoc runs and on demand ETL executions                                      ##
REM ## It checks all tables and columns, extracts events, filters and transforms data                         ##
REM ## Creates Excel files on user request and inserts data into the database                                 ##
REM ## Please edit the APP_DIR path to point to where the application is installed                            ##
REM ## Make sure the virtual environment and requirements are set up before running                            ##
REM ## This Script is for Windows Command Prompt / Task Scheduler                                             ##
REM ############################################################################################################

SET APP_DIR=E:\PythonProjects\ETLPipeline
SET LOG_DIR=%APP_DIR%\logs

IF NOT EXIST "%LOG_DIR%" mkdir "%LOG_DIR%"

FOR /F "tokens=1-6 delims=/:. " %%A IN ("%DATE% %TIME%") DO (
    SET LOG_FILE=%LOG_DIR%\etl_flaskapp_%%C%%B%%A_%%D%%E%%F.log
)

echo ==============================================
echo   Starting ETL Pipeline Tool
echo   Component  : Flask Web Application (Adhoc)
echo   Started at : %DATE% %TIME%
echo   Logs       : %LOG_FILE%
echo ==============================================

IF NOT EXIST "%APP_DIR%\etlpipeline\flaskApp\app" (
    echo ERROR: Flask app directory not found: %APP_DIR%\etlpipeline\flaskApp\app
    pause
    exit /b 1
)
cd /d "%APP_DIR%\etlpipeline\flaskApp\app"

IF EXIST "%APP_DIR%\.venv\Scripts\activate.bat" (
    call "%APP_DIR%\.venv\Scripts\activate.bat"
    echo Virtual environment activated.
) ELSE (
    echo WARNING: Virtual environment not found. Running with system Python.
)

START "ETLPipeline-FlaskApp" /B python -m app >> "%LOG_FILE%" 2>&1

echo ==============================================
echo   ETL Flask App is running in the background
echo   To stop: End python.exe in Task Manager
echo   Logs: %LOG_FILE%
echo ==============================================

IF EXIST "%APP_DIR%\.venv\Scripts\deactivate.bat" (
    call "%APP_DIR%\.venv\Scripts\deactivate.bat"
)

pause
