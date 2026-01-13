import os
import sys
import pandas as pd
import subprocess
import math
from datetime import datetime
from dateutil import parser
import warnings
import services.intro as intro
import services.mailing as mailing
import services.sqlcheck as sqlcheck
import services.extractionScript as extractionScript
import services.fileWrite as fileWrite
import services.insertScript as insertScript
from concurrent.futures import ThreadPoolExecutor, as_completed
warnings.filterwarnings("ignore", category=UserWarning)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dao.oracleDao import OracleDAO
import config_loader as cnfloader
import logger as logging

def preProcessAndInsertOfData(filePath, threadId):
    try:
        config = cnfloader.load_properties()
        logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Starting Thread: {threadId}")
        logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Thread {threadId} started Processing the file: {filePath}")
        filterCheck = config.get('filterCheck', "false").strip().upper() == "TRUE"
        if filterCheck:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "Filter Check is True")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Proceeding to filter the columns from the given data source for the file: {filePath}")
            result = None
            result = fileWrite.fileModifier(filePath)
            if not result or result != "SUCCESSFUL":
                raise Exception((400, f"Failed to Filter the Data File: {filePath}"))
            
            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Columns have been filtered for the data file: {filePath}")
            
        renameCheck = config.get('renameCheck', "false").strip().upper() == "TRUE"

        if renameCheck:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"The Rename Check mark is True")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Proceeding to Rename the columns for data file: {filePath}")
            renameFrom = config.get('renameFrom', "")
            renameTo = config.get('renameTo', "")
            if not renameFrom or not renameTo:
                raise Exception((404, f"Renamefrom and Renameto is missing in the configuration failed for the data file: {filePath}"))
            result = None
            result = fileWrite.fileRename(filePath, renameFrom, renameTo)
            if not result or result != "SUCCESSFUL":
                raise Exception((400, f"Failed to Rename the Data File: {filePath}"))
            
            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Columns have been Renamed for the data file: {filePath}")
        else:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"The Rename Check mark is False")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Proceeding to Check if the columns in data file match that of the target columns for the file: {filePath}")
            insertColumns = config.get('insertColumns', "")
            if not insertColumns:
                raise Exception((400, f"No Insert Column present to do the check in the file"))

            if isinstance(insertColumns, str):
                expectedCols = [c.strip().upper() for c in insertColumns.split(",") if c.strip()]
            elif isinstance(insertColumns, (list, tuple)):
                expectedCols = [str(c).strip().upper() for c in insertColumns if str(c).strip()]
            else:
                raise ValueError((400, "insertColumns must be string or list/tuple"))
            
            df = pd.read_csv(filePath, nrows=0)
            csvCols = [c.strip().upper() for c in df.columns]

            if csvCols != expectedCols:
                raise ValueError((400, "The Columns present in the CSV File to that of the Insert Tables do not match Please check"))

            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Checking of the columns in data file to that of the target columns has been completed for the file: {filePath}")

        logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Proceeding to Insert the Data into the target database for the file: {filePath}")

        dbHostTarget = config.get('dbHost2')
        portTarget = int(config.get('port2'))
        serviceNameTarget = config.get('serviceName2')
        userNameTarget = config.get('userName2')
        passwordTarget = config.get('password2')

        if not dbHostTarget or not portTarget or not serviceNameTarget or not userNameTarget or not passwordTarget:
            raise ValueError((404, "The Database Target configuration is missing please check"))
        
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Successfully retrieved the database Target configurations")

        targetTableName = config.get("insertTableName", "").strip()
        targetSelectColumns = config.get("insertColumns", "")
        if not targetTableName or not targetSelectColumns:
            raise ValueError((404, "The Database Target Table Name and Columns are missing in the configuration file please check"))

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Fetched the Target Table and Column Configurations")

        result = None
        result = insertScript.InsertingIntoTheDB(filePath, targetTableName, targetSelectColumns, dbHostTarget, portTarget, serviceNameTarget, userNameTarget, passwordTarget)

        if not result or result != "SUCCESSFUL":
            raise Exception((400, f"Failed to inserted the data into the Target Database for the file: {filePath}"))
        

        logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Data insertion has been completed for the Target Database for the File: {filePath}")
        
        file = config.get('file', "false").strip().upper() == "TRUE"

        if file:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Proceeding to insert the data into the final file for the File: {filePath}")
            outputDirectory = config.get('workingDirectory')
            processedDirectory = config.get('processedDirectory')
            if not os.path.isdir(outputDirectory):
                raise ValueError((400, "No Output Directory found for the final file"))
            processedFilePath = os.path.join(outputDirectory, processedDirectory)
            if not os.path.isdir(outputDirectory):
                os.makedirs(outputDirectory, exist_ok=True)
            finalFileName = config.get('finalFileName')
            if not finalFileName:
                raise Exception((400, "Final File has not been found"))
            result = None
            result = fileWrite.dataWriter(filePath, processedFilePath, finalFileName, targetSelectColumns)
            if not result or result != "SUCCESSFUL":
                raise Exception((400, f"Failed to inserted the data into Final File: {filePath}"))
            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Data Inserted into the final path for the file: {filePath}")
        
        return filePath, "SUCCESS"
    except Exception as e:
        config = cnfloader.load_properties()
        if isinstance(e.args[0], tuple) and len(e.args[0]) == 2:
            code, message = e.args[0]
        else:
            code, message = 500, str(e)
        logging.logger('ERROR', 'ETL Pipeline Tool', code, message)
        if code in [400, 404]:
            sendmails = config.get('Sendmail', "false").strip().upper() == "TRUE"
            if(sendmails):
                mailing.sendbatchemail(f'ETL Pipeline has failed with the below mentioned error\n{message}')
            sys.exit(1)
        return filePath, "FAILED"



def startOfPipeLine():
    try:
        config = cnfloader.load_properties()
        startmessage = intro.intro()
        loadno = logging.loadnoupdate()
        logging.startinglogger(startmessage)
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Starting a new ETL Pipeline run")
        logging.logger('INFO', 'ETL Pipeline Tool', 200, f"ETL Pipeline run {loadno}")
        fileRequired = config.get('file', "false").strip().upper() == "TRUE"
        if fileRequired:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The Final File Creating Check Mark is True")
        else:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The Final File Creating Check Mark is False")

        sendMailCheck = config.get('Sendmail', "false").strip().upper() == "TRUE"
        if sendMailCheck:
            mailing.sendbatchemail(f'New Pipeline has been started')
        
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Getting the Database configuration of the source")

        dbHostSource = config.get('dbHost1')
        portSource = int(config.get('port1'))
        serviceNameSource = config.get('serviceName1')
        userNameSource = config.get('userName1')
        passwordSource = config.get('password1')

        if not dbHostSource or not portSource or not serviceNameSource or not userNameSource or not passwordSource:
            raise ValueError((404, "The Database source configuration is missing please check"))
        
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Successfully retrieved the database source configurations")
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Getting the Target Database Configuration")

        dbHostTarget = config.get('dbHost2')
        portTarget = int(config.get('port2'))
        serviceNameTarget = config.get('serviceName2')
        userNameTarget = config.get('userName2')
        passwordTarget = config.get('password2')

        if not dbHostTarget or not portTarget or not serviceNameTarget or not userNameTarget or not passwordTarget:
            raise ValueError((404, "The Database Target configuration is missing please check"))
        
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Successfully retrieved the database Target configurations")

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Getting the Source Table and Column Configurations")

        sourceTableName = config.get("sourceTableName", "").strip()
        sourceSelectColumns = config.get("selectColumns", "")
        if not sourceTableName or not sourceSelectColumns:
            raise ValueError((404, "The Database Source Table Name and Columns are missing in the configuration file please check"))
        
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Fetched the Source Table and Column Configurations")
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Getting the Target Table and Column Configurations")

        targetTableName = config.get("insertTableName", "").strip()
        targetSelectColumns = config.get("insertColumns", "")
        if not targetTableName or not targetSelectColumns:
            raise ValueError((404, "The Database Target Table Name and Columns are missing in the configuration file please check"))

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Fetched the Target Table and Column Configurations")

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Validating if the source Table exists")
        result = None
        result = sqlcheck.tableExists(dbHostSource, portSource, serviceNameSource, userNameSource, passwordSource, sourceTableName)
        if not result:
            raise Exception((400, "The Database Source Table Does not exists"))
        
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Database source Table exists")

        result = None
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Validating if the source Coloumns exists in the table")

        result = sqlcheck.validateColumns(dbHostSource, portSource, serviceNameSource, userNameSource, passwordSource, sourceTableName, sourceSelectColumns)

        if result:
            raise Exception((400, f"These columns do not exists in the source table: {result}"))
        
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "All the Source Coloumns exists in the source table")

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Validating if the Target Table exists")
        result = None

        result = sqlcheck.tableExists(dbHostTarget, portTarget, serviceNameTarget, userNameTarget, passwordTarget, targetTableName)
        
        if not result:
            raise Exception((400, "The Database Target Table Does not exists"))
        
        result = None
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Database Target Table exists")

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Validating if the Target Coloumns exists in the table")
        result = None
              
        result = sqlcheck.validateColumns(dbHostTarget, portTarget, serviceNameTarget, userNameTarget, passwordTarget, targetTableName, targetSelectColumns)
        if result:
            raise Exception((400, f"These columns do not exists in the Target table: {result}"))
        
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "All the Target Coloumns exists in the Target table")

        result = None

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Proceeding to Extract the from the Source Table")

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Retrieving the working file directory and configurations")

        initialLoadCheck = config.get('initialLoad', "false").strip().upper() == "TRUE"
        workingDirectory = config.get('workingDirectory')
        incomingDirectory = config.get('incomingDirectory')
        workingFile = config.get('workingFile')

        if not workingDirectory or not incomingDirectory:
            raise ValueError((404, "The Working Directory and Incoming Directory is missing"))

        if not workingFile:
            raise ValueError((404, "The Working File is missing"))

        if not os.path.isdir(workingDirectory):
            os.makedirs(workingDirectory, exist_ok=True)

        incomingFileDirectory = os.path.join(workingDirectory, incomingDirectory)

        if not os.path.isdir(incomingFileDirectory):
            os.makedirs(incomingFileDirectory, exist_ok=True)

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Successfully retrieved the working file directory and configurations")

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Working on getting the Last File check path and the incremental check configurations")

        incrementalCheck = config.get('incrementalCheck', 'false').strip().upper() == 'TRUE'

        lastRunCheck = config.get('lastRunCheck')

        if not lastRunCheck:
            raise ValueError((404, "The lastRunCheck File is missing"))
        
        result = None


        if initialLoadCheck and incrementalCheck:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The Initial Load check is True")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "Proceeding to Run the ETL Pipeline in Initial Load Mode")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "Forming the SQL string")
            connString = f"{userNameSource}/{passwordSource}@{dbHostSource}:{portSource}/{serviceNameSource}"
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "SQL String has been successfully formed")
            result = extractionScript.initiatextractSourceToFile(sourceTableName,sourceSelectColumns, incrementalCheck, incomingFileDirectory, workingFile, connString, lastRunCheck)
            if not result or result == 0:
                logging.logger('INFO', 'ETL Pipeline Tool', 200, "No New Data to Retrieve the ETL Pipeline is shutting down")
                sys.exit(1)
            
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The ETL Pipeline Data has been extracted Successfully")

        elif not initialLoadCheck and not incrementalCheck:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The Initial Load check is False")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The Incremental Check is False")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "Proceeding to Run the ETL Pipeline in Default Load Mode")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "Forming the SQL string")
            connString = f"{userNameSource}/{passwordSource}@{dbHostSource}:{portSource}/{serviceNameSource}"
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "SQL String has been successfully formed")
            result = extractionScript.initiatextractSourceToFile(sourceTableName,sourceSelectColumns, incrementalCheck, incomingFileDirectory, workingFile, connString, lastRunCheck)
            if not result or result == 0:
                logging.logger('INFO', 'ETL Pipeline Tool', 200, "No New Data to Retrieve the ETL Pipeline is shutting down")
                sys.exit(1)
            
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The ETL Pipeline Data has been extracted Successfully")


        elif initialLoadCheck and not incrementalCheck:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The Initial Load check is True")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The Incremental Check is False")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "Proceeding to Run the ETL Pipeline in Initial Load Mode")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "Forming the SQL string")
            connString = f"{userNameSource}/{passwordSource}@{dbHostSource}:{portSource}/{serviceNameSource}"
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "SQL String has been successfully formed")
            result = extractionScript.initiatextractSourceToFile(sourceTableName,sourceSelectColumns, incrementalCheck, incomingFileDirectory, workingFile, connString, lastRunCheck)
            if not result or result == 0:
                logging.logger('INFO', 'ETL Pipeline Tool', 200, "No New Data to Retrieve the ETL Pipeline is shutting down")
                sys.exit(1)
            
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The ETL Pipeline Data has been extracted Successfully")
                     
        elif not initialLoadCheck and incrementalCheck:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The Initial Load check is False")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "Proceeding to Run the ETL Pipeline in Incremental Mode")
            
            result = extractionScript.extractSourceToFile(dbHostSource, portSource, serviceNameSource, userNameSource, passwordSource, sourceTableName, sourceSelectColumns, incomingFileDirectory, workingFile, incrementalCheck, lastRunCheck)

            if not result or result == 0:
                logging.logger('INFO', 'ETL Pipeline Tool', 200, "No New Data to Retrieve the ETL Pipeline is shutting down")
                sys.exit(1)
            
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The ETL Pipeline Data has been extracted Successfully")

        else:
            raise Exception((400, "Invalid Argument for the Inital Load Check and Incremental Load check")) 

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Proceeding to Pre Processing")
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Splitting the Input Data File")

        inputFile = os.path.join(incomingFileDirectory, workingFile)

        if not os.path.isfile(inputFile):
            raise ValueError((404, "No Input Data File Found. Please check if the extraction script worked"))

        data_df = pd.read_csv(inputFile)
        if len(data_df) == 0 or data_df is None:
            raise ValueError((404, "No Data Found in the File. Error has occured please check the input file"))

        datafile, datafilename = fileWrite.datasplitter(inputFile)

        if not datafile or not datafilename:
            raise Exception((400, "No Data File or File Name returned post splitting the Input File. Please check the working directory"))

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Splitting of the Input Data file has been completed")

        maxWorkers = len(datafile)

        results = []

        with ThreadPoolExecutor(max_workers=maxWorkers) as executor:
            ThreadOfFile = {executor.submit(preProcessAndInsertOfData, file, idx + 1): file for idx, file in enumerate(datafile)}
            
            for future in as_completed(ThreadOfFile):
                result = future.result()
                results.append(result)

        failed  = [f for f, status in results if status == "FAILED"]

        if failed:
            raise RuntimeError((400, f"Some files failed to process: {failed}"))
        
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "ETL pipeline has completed processing the data")
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Cleaning up the Working Directories")

        for p in datafile:
            if p and os.path.isfile(p):
                os.remove(p)
                logging.logger("INFO", "ETL Pipeline Tool", 200,f"Removed existing data file: {p}")

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Clean up has been completed")
        logging.logger('INFO', 'ETL Pipeline Tool', 200, f"ETL pipeline has been completed for the load no: {loadno}")
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Shutting Down the ETL pipeline")
        
    except Exception as e:
        config = cnfloader.load_properties()
        if isinstance(e.args[0], tuple) and len(e.args[0]) == 2:
            code, message = e.args[0]
        else:
            code, message = 500, str(e)
        logging.logger('ERROR', 'ETL Pipeline Tool', code, message)
        if code in [400, 404]:
            sendmails = config.get('Sendmail', "false").strip().upper() == "TRUE"
            if(sendmails):
                mailing.sendbatchemail(f'ETL Pipeline has failed with the below mentioned error\n{message}')
            sys.exit(1)
        return None
    
        


    