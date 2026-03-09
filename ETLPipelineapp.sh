#!/bin/bash
############################################################################################################
##                                 ETL Pipeline Tool                                                      ##
##                                                                                                        ##
## This Script is developed by Shankar N G                                                                ##
## It is to trigger the Flask Application of the ETL Pipeline Tool                                        ##
## The Flask App is used for adhoc runs and on demand ETL executions                                      ##
## It checks all tables and columns, extracts events, filters and transforms data                         ##
## Creates Excel files on user request and inserts data into the database                                 ##
## Please edit the APP_DIR path to point to where the application is installed                            ##
## Make sure the virtual environment and requirements are set up before running                            ##
## This Script is for Linux Terminal                                                                      ##
############################################################################################################

APP_DIR=~/PythonProjects/ETLPipelineTool
LOG_DIR=$APP_DIR/logs
LOG_FILE=$LOG_DIR/etl_flaskapp_$(date +%Y%m%d_%H%M%S).log

mkdir -p "$LOG_DIR"

echo "=============================================="
echo "  Starting ETL Pipeline Tool"
echo "  Component  : Flask Web Application (Adhoc)"
echo "  Started at : $(date)"
echo "  Logs       : $LOG_FILE"
echo "=============================================="

cd "$APP_DIR/etlpipeline/flaskApp/app" || { echo "ERROR: Flask app directory not found: $APP_DIR/etlpipeline/flaskApp/app"; exit 1; }

if [ -f "$APP_DIR/.venv/bin/activate" ]; then
    source "$APP_DIR/.venv/bin/activate"
    echo "Virtual environment activated."
else
    echo "WARNING: Virtual environment not found. Running with system Python."
fi

nohup python -m app >> "$LOG_FILE" 2>&1 &

APP_PID=$!
echo "ETL Flask App started with PID: $APP_PID"
echo "To stop the application, run: kill $APP_PID"
echo "To view logs, run: tail -f $LOG_FILE"

echo $APP_PID > "$APP_DIR/etl_flaskapp.pid"
echo "PID saved to: $APP_DIR/etl_flaskapp.pid"

deactivate 2>/dev/null

echo "=============================================="
echo "  ETL Flask App is running in the background"
echo "=============================================="
