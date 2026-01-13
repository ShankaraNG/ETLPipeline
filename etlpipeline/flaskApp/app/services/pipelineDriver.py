import os
import sys
import pandas as pd
import subprocess
import math
from datetime import datetime
from dateutil import parser
import warnings
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

def preProcessAndInsertOfData(filePath, threadId, configmapFilePath):
    try:
        config = cnfloader.load_properties()
        configMap = cnfloader.load_File_properties(configmapFilePath)
        logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Starting Thread: {threadId}")
        logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Thread {threadId} started Processing the file: {filePath}")
        filterCheck = configMap.get('filterCheck', "false").strip().upper() == "TRUE"
        if filterCheck:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "Filter Check is True")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Proceeding to filter the columns from the given data source for the file: {filePath}")
            result = None
            result = fileWrite.fileModifier(filePath, configmapFilePath)
            if result != "SUCCESSFUL":
                raise Exception((400, f"Failed to Filter the Data File: {filePath}"))
            
            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Columns have been filtered for the data file: {filePath}")
            
        renameCheck = configMap.get('renameCheck', "false").strip().upper() == "TRUE"

        if renameCheck:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"The Rename Check mark is True")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Proceeding to Rename the columns for data file: {filePath}")
            renameFrom = configMap.get('renameFrom', "")
            renameTo = configMap.get('renameTo', "")
            if not renameFrom or not renameTo:
                raise Exception((404, f"Renamefrom and Renameto is missing in the configuration failed for the data file: {filePath}"))
            result = None
            result = fileWrite.fileRename(filePath, renameFrom, renameTo, configmapFilePath)
            if result != "SUCCESSFUL":
                raise Exception((400, f"Failed to Rename the Data File: {filePath}"))
            
            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Columns have been Renamed for the data file: {filePath}")
        else:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"The Rename Check mark is False")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Proceeding to Check if the columns in data file match that of the target columns for the file: {filePath}")
            insertColumns = configMap.get('insertColumns', "")
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

        dbHostTarget = configMap.get('dbHost2')
        portTarget = int(configMap.get('port2'))
        serviceNameTarget = configMap.get('serviceName2')
        userNameTarget = configMap.get('userName2')
        passwordTarget = configMap.get('password2')

        if not dbHostTarget or not portTarget or not serviceNameTarget or not userNameTarget or not passwordTarget:
            raise ValueError((404, "The Database Target configuration is missing please check"))
        
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Successfully retrieved the database Target configurations")

        targetTableName = configMap.get("insertTableName", "").strip()
        targetSelectColumns = configMap.get("insertColumns", "")
        if not targetTableName or not targetSelectColumns:
            raise ValueError((404, "The Database Target Table Name and Columns are missing in the configuration file please check"))

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Fetched the Target Table and Column Configurations")

        result = None
        result = insertScript.InsertingIntoTheDB(filePath, targetTableName, targetSelectColumns, dbHostTarget, portTarget, serviceNameTarget, userNameTarget, passwordTarget, configmapFilePath)

        if not result or result != "SUCCESSFUL":
            raise Exception((400, f"Failed to inserted the data into the Target Database for the file: {filePath}"))
        

        logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Data insertion has been completed for the Target Database for the File: {filePath}")
        
        file = configMap.get('file', "false").strip().upper() == "TRUE"

        if file:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, f"Proceeding to insert the data into the final file for the File: {filePath}")
            outputDirectory = config.get('workingDirectory')
            processedDirectory = config.get('processedDirectory')
            if not os.path.isdir(outputDirectory):
                raise ValueError((400, "No Output Directory found for the final file"))
            processedFilePath = os.path.join(outputDirectory, processedDirectory)
            if not os.path.isdir(outputDirectory):
                os.makedirs(outputDirectory, exist_ok=True)
            finalFileName = configMap.get('finalFileName')
            if not finalFileName:
                raise Exception((400, "Final File has not been found"))
            result = None
            result = fileWrite.dataWriter(filePath, processedFilePath, finalFileName, targetSelectColumns, configmapFilePath)
            if result != "SUCCESSFUL":
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
                mailing.sendbatchemail(f'ETL Pipeline has failed with the below mentioned error\n{message}', configmapFilePath)
        return filePath, "FAILED"



def startOfPipeLine(loadno, configmapFilePath):
    try:
        config = cnfloader.load_properties()
        configMap = cnfloader.load_File_properties(configmapFilePath)
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Starting a new ETL Pipeline run")
        logging.logger('INFO', 'ETL Pipeline Tool', 200, f"ETL Pipeline run {loadno}")
        fileRequired = configMap.get('file', "false").strip().upper() == "TRUE"
        if fileRequired:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The Final File Creating Check Mark is True")
        else:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The Final File Creating Check Mark is False")

        sendMailCheck = config.get('Sendmail', "false").strip().upper() == "TRUE"
        if sendMailCheck:
            mailing.sendbatchemail(f'New Pipeline has been started', configmapFilePath)
        
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Getting the Database configuration of the source")

        dbHostSource = configMap.get('dbHost1')
        portSource = int(configMap.get('port1'))
        serviceNameSource = configMap.get('serviceName1')
        userNameSource = configMap.get('userName1')
        passwordSource = configMap.get('password1')

        if not dbHostSource or not portSource or not serviceNameSource or not userNameSource or not passwordSource:
            raise ValueError((404, "The Database source configuration is missing please check"))
        
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Successfully retrieved the database source configurations")
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Getting the Target Database Configuration")

        dbHostTarget = configMap.get('dbHost2')
        portTarget = int(configMap.get('port2'))
        serviceNameTarget = configMap.get('serviceName2')
        userNameTarget = configMap.get('userName2')
        passwordTarget = configMap.get('password2')

        if not dbHostTarget or not portTarget or not serviceNameTarget or not userNameTarget or not passwordTarget:
            raise ValueError((404, "The Database Target configuration is missing please check"))
        
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Successfully retrieved the database Target configurations")

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Getting the Source Table and Column Configurations")

        sourceTableName = configMap.get("sourceTableName", "").strip()
        sourceSelectColumns = configMap.get("selectColumns", "")
        if not sourceTableName or not sourceSelectColumns:
            raise ValueError((404, "The Database Source Table Name and Columns are missing in the configuration file please check"))
        
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Fetched the Source Table and Column Configurations")
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Getting the Target Table and Column Configurations")

        targetTableName = configMap.get("insertTableName", "").strip()
        targetSelectColumns = configMap.get("insertColumns", "")
        if not targetTableName or not targetSelectColumns:
            raise ValueError((404, "The Database Target Table Name and Columns are missing in the configuration file please check"))

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Fetched the Target Table and Column Configurations")

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Validating if the source Table exists")
        result = None
        result = sqlcheck.tableExists(dbHostSource, portSource, serviceNameSource, userNameSource, passwordSource, sourceTableName, configmapFilePath)
        if not result:
            raise Exception((400, "The Database Source Table Does not exists"))
        
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Database source Table exists")

        result = None
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Validating if the source Coloumns exists in the table")

        result = sqlcheck.validateColumns(dbHostSource, portSource, serviceNameSource, userNameSource, passwordSource, sourceTableName, sourceSelectColumns, configmapFilePath)

        if result:
            raise Exception((400, f"These columns do not exists in the source table: {result}"))
        
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "All the Source Coloumns exists in the source table")

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Validating if the Target Table exists")
        result = None

        result = sqlcheck.tableExists(dbHostTarget, portTarget, serviceNameTarget, userNameTarget, passwordTarget, targetTableName, configmapFilePath)
        
        if not result:
            raise Exception((400, "The Database Target Table Does not exists"))
        
        result = None
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Database Target Table exists")

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Validating if the Target Coloumns exists in the table")
        result = None
              
        result = sqlcheck.validateColumns(dbHostTarget, portTarget, serviceNameTarget, userNameTarget, passwordTarget, targetTableName, targetSelectColumns, configmapFilePath)
        if result:
            raise Exception((400, f"These columns do not exists in the Target table: {result}"))
        
        logging.logger('INFO', 'ETL Pipeline Tool', 200, "All the Target Coloumns exists in the Target table")

        result = None

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Proceeding to Extract the from the Source Table")

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Retrieving the working file directory and configurations")

        initialLoadCheck = configMap.get('initialLoad', "false").strip().upper() == "TRUE"
        workingDirectory = config.get('workingDirectory')
        incomingDirectory = config.get('incomingDirectory')
        workingFile = configMap.get('finalFileName')

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

        incrementalCheck = configMap.get('incrementalCheck', 'false').strip().upper() == 'TRUE'
        
        result = None


        if initialLoadCheck and incrementalCheck:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The Initial Load check is True")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "Proceeding to Run the ETL Pipeline in Initial Load Mode")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "Forming the SQL string")
            connString = f"{userNameSource}/{passwordSource}@{dbHostSource}:{portSource}/{serviceNameSource}"
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "SQL String has been successfully formed")
            result = extractionScript.initiatextractSourceToFile(sourceTableName,sourceSelectColumns, incrementalCheck, incomingFileDirectory, workingFile, connString, configmapFilePath)
            if result == 0:
                logging.logger('INFO', 'ETL Pipeline Tool', 200, "No New Data to Retrieve the ETL Pipeline is shutting down")
                return "SUCCESSFUL"
            elif result is None:
                logging.logger('INFO', 'ETL Pipeline Tool', 200, "ETL PipeLine Failed")
                raise Exception((400, "ETL Pipeline Failed"))
            
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The ETL Pipeline Data has been extracted Successfully")

        elif not initialLoadCheck and not incrementalCheck:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The Initial Load check is False")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The Incremental Check is False")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "Proceeding to Run the ETL Pipeline in Default Load Mode")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "Forming the SQL string")
            connString = f"{userNameSource}/{passwordSource}@{dbHostSource}:{portSource}/{serviceNameSource}"
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "SQL String has been successfully formed")
            result = extractionScript.initiatextractSourceToFile(sourceTableName,sourceSelectColumns, incrementalCheck, incomingFileDirectory, workingFile, connString, configmapFilePath)
            if result == 0:
                logging.logger('INFO', 'ETL Pipeline Tool', 200, "No New Data to Retrieve the ETL Pipeline is shutting down")
                return "SUCCESSFUL"
            elif result is None:
                logging.logger('INFO', 'ETL Pipeline Tool', 200, "ETL PipeLine Failed")
                raise Exception((400, "ETL Pipeline Failed"))
            
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The ETL Pipeline Data has been extracted Successfully")


        elif initialLoadCheck and not incrementalCheck:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The Initial Load check is True")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The Incremental Check is False")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "Proceeding to Run the ETL Pipeline in Initial Load Mode")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "Forming the SQL string")
            connString = f"{userNameSource}/{passwordSource}@{dbHostSource}:{portSource}/{serviceNameSource}"
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "SQL String has been successfully formed")
            result = extractionScript.initiatextractSourceToFile(sourceTableName,sourceSelectColumns, incrementalCheck, incomingFileDirectory, workingFile, connString, configmapFilePath)
            if result == 0:
                logging.logger('INFO', 'ETL Pipeline Tool', 200, "No New Data to Retrieve the ETL Pipeline is shutting down")
                return "SUCCESSFUL"
            elif result is None:
                logging.logger('INFO', 'ETL Pipeline Tool', 200, "ETL PipeLine Failed")
                raise Exception((400, "ETL Pipeline Failed"))
            
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The ETL Pipeline Data has been extracted Successfully")
                     
        elif not initialLoadCheck and incrementalCheck:
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "The Initial Load check is False")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "Proceeding to Run the ETL Pipeline in Incremental Mode")
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "Forming the SQL string")
            connString = f"{userNameSource}/{passwordSource}@{dbHostSource}:{portSource}/{serviceNameSource}"
            logging.logger('INFO', 'ETL Pipeline Tool', 200, "SQL String has been successfully formed")
            result = extractionScript.initiatextractSourceToFile(sourceTableName,sourceSelectColumns, incrementalCheck, incomingFileDirectory, workingFile, connString, configmapFilePath)
            if result == 0:
                logging.logger('INFO', 'ETL Pipeline Tool', 200, "No New Data to Retrieve the ETL Pipeline is shutting down")
                return "SUCCESSFUL"
            elif result is None:
                logging.logger('INFO', 'ETL Pipeline Tool', 200, "ETL PipeLine Failed")
                raise Exception((400, "ETL Pipeline Failed"))
            
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

        datafile, datafilename = fileWrite.datasplitter(inputFile, configmapFilePath, loadno)

        if datafile is None or datafilename is None:
            raise Exception((400, "No Data File or File Name returned post splitting the Input File. Please check the working directory"))

        logging.logger('INFO', 'ETL Pipeline Tool', 200, "Splitting of the Input Data file has been completed")

        maxWorkers = len(datafile)

        results = []

        with ThreadPoolExecutor(max_workers=maxWorkers) as executor:
            ThreadOfFile = {executor.submit(preProcessAndInsertOfData, file, idx + 1, configmapFilePath): file for idx, file in enumerate(datafile)}
            
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

        return "SUCCESSFUL"
        
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
                mailing.sendbatchemail(f'ETL Pipeline has failed with the below mentioned error\n{message}', configmapFilePath)
        return None
    
        


    