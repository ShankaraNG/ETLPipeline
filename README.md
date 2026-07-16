# ETLPipeline

A Python-based ETL (Extract, Transform, Load) pipeline that extracts data from a source SQL database, transforms it to the required format, and loads it into a target SQL database — developed by Shankara Narayana N G.

## Overview
The ETL Pipeline automates the full data movement process between SQL databases:

Extract — Connects to the source database and extracts the required data
Transform — Processes the extracted data by renaming columns and dropping unnecessary ones to match the target schema
Load — Inserts the transformed data into the target database

The pipeline supports any SQL-based source and target database, making it flexible across different environments and database configurations.
It can be triggered in two ways:

Batch Process — Runs in the background and can be scheduled (e.g. via cron or Task Scheduler) for automated recurring runs
Adhoc Run — Triggered on demand via the Flask web application for one-off or manual executions


Make sure to verify the system configuration and all values in the configuration properties file before starting the pipeline. Contact Shankar for more information.


## Prerequisites

Python 3.x installed
SQL client installed and configured for both source and target databases
Source and target database connection details available
Dependencies installed from requirements.txt


## Installation
1. Clone the repository
bashgit clone <repository-url>
cd etlpipeline
2. Install dependencies
bashpip install -r requirements.txt

## Running the Application
Batch Process
Used for scheduled or background runs.
bashcd etlpipeline/batchprocess/app
python -m main
Flask App — Adhoc Runs
Used for on-demand, manual triggered runs via the web interface.
bashcd etlpipeline/flaskApp/app
python -m app

## Configuration
Before running the pipeline, ensure both the source and target database configurations are correctly set up in the configuration properties file. This includes:

Source Database — Hostname, port, service name, username, password
Target Database — Hostname, port, service name, username, password
Transformation rules — Column renames and columns to drop

Verify all configuration values carefully before triggering a run to avoid data issues.

## How the Pipeline Works
1. Extract
The pipeline reads the source database configuration and establishes a connection. It then runs the extraction query and pulls the data from the source.
2. Transform
The extracted data is processed:

Column renaming — Columns are renamed to match the target database schema
Column dropping — Unnecessary columns are removed before loading

3. Load
The transformed data is inserted into the target database using the target configuration. The pipeline connects to the target, maps the data to the correct schema, and performs the insert.

## Scheduling the Batch Process
Linux — Cron Job
bashcrontab -e
Add a line such as:
0 6 * * * cd ~/etlpipeline/batchprocess/app && python -m main
This runs the batch process every day at 6:00 AM.
Windows — Task Scheduler

Open Task Scheduler
Create a new Basic Task
Set the trigger (e.g. daily or hourly)
Set the action to run the batch process script


## Author
Shankar N G
For further details on setup, configuration, or usage — please contact Shankar directly.

## License
This project is for internal use. Please refer to your organization's usage policy.
