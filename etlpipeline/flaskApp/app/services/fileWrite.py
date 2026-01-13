import os
import sys
import pandas as pd
import subprocess
import math
import services.mailing as mailing
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dao.oracleDao import OracleDAO
import config_loader as cnfloader
import logger as logging


def datasplitter(dataFile, configmapFilePath, loadNo):
    try:
        data_df = pd.read_csv(dataFile)
        lengthofdata = len(data_df)
        config = cnfloader.load_properties()
        configMap = cnfloader.load_File_properties(configmapFilePath)
        dataTemp = config.get('workingDirectory')
        outputDirectory = config.get('workingOnDirectory')
        if not dataTemp or not outputDirectory:
            raise ValueError((400, "Missing 'workingDirectory' or 'workingOnDirectory' in config"))
        pathToFile = os.path.join(dataTemp, outputDirectory)
        if not os.path.isdir(pathToFile):
            raise ValueError((404, f"Directory not found at: {pathToFile}"))
        totalcountoffiles = int(config.get('noOfThreads'))
        if totalcountoffiles <= 0:
            raise ValueError((400, "Invalid number of threads (nofothread). Must be greater than 0"))
        if totalcountoffiles >= lengthofdata:
            totalcountoffiles = lengthofdata 
        start = 0
        end = lengthofdata//totalcountoffiles
        datafile = []
        datafilename = []
        selectColumns = configMap.get("selectColumns", "")
        if isinstance(selectColumns, str):
            column_list = [c.strip().upper() for c in selectColumns.split(",") if c.strip()]

        elif isinstance(selectColumns, (list, tuple)):
            column_list = [str(c).strip().upper() for c in selectColumns if c is not None and str(c).strip()]

        else:
            raise ValueError((400,"The selectColumns must be a string or a list/tuple"))
        
        #column_list = [col.strip().upper() for col in selectColumns.split(',')]

        for i in range(totalcountoffiles):
            if(i!=totalcountoffiles-1):
                tempdata = data_df[start:end]
                start = end
                end = start + lengthofdata//totalcountoffiles
            else:
                tempdata = data_df[start:]
            filename = f'file_{loadNo}_{i+1}.csv'
            file = os.path.join(pathToFile, filename)
            tempdata.to_csv(file, columns=column_list, index=False)
            datafile.append(file)
            datafilename.append(filename)
        return datafile, datafilename
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
        return None, None


def fileModifier(dataFile, configmapFilePath):
    try:
        config = cnfloader.load_properties()
        configMap = cnfloader.load_File_properties(configmapFilePath)
        if not os.path.isfile(dataFile):
            raise ValueError((400, f"Missing data file at: {dataFile}"))
        
        keepColumns = configMap.get("keepOnlyColumnsFromSource", "")
        
        if not keepColumns:
            raise ValueError((400, "filtercheck is true but keepOnlyColumnsFromSource is empty"))
        
        if isinstance(keepColumns, str):
            columnsToKeep = [c.strip().upper() for c in keepColumns.split(",") if c.strip()]

        elif isinstance(keepColumns, (list, tuple)):
            columnsToKeep = [str(c).strip().upper() for c in keepColumns if c is not None and str(c).strip()]

        else:
            raise ValueError((400,"The Columns to keep must be a string or a list/tuple"))
        
        #columnsToKeep = [c.strip().upper() for c in keepColumns.split(",") if c.strip()]
        df = pd.read_csv(dataFile)
        missingCols = set(columnsToKeep) - set(df.columns)
        if missingCols:
            raise ValueError((400, f"Missing columns in source file: {missingCols}"))
        
        df = df[columnsToKeep]
        df.to_csv(dataFile, index=False)

        logging.logger("INFO", "ETL Pipeline Tool", 200, f"Filtered columns in file {dataFile}: {columnsToKeep}")
        
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
        return "FAILED"

def fileRename(dataFile, renameFrom, renameTo, configmapFilePath):
    try:
        # renameFrom = config.get("renameFrom", "")
        # renameTo = config.get("renameTo", "")

        if not renameFrom or not renameTo:
            raise ValueError((400, "renameFrom or renameTo is empty"))
        
        if isinstance(renameFrom, str):
            fromCols = [c.strip().upper() for c in renameFrom.split(",") if c.strip()]

        elif isinstance(renameFrom, (list, tuple)):
            fromCols = [str(c).strip().upper() for c in renameFrom if c is not None and str(c).strip()]

        else:
            raise ValueError((400,"The renameFrom Columns must be a string or a list/tuple"))

        if isinstance(renameTo, str):
            toCols = [c.strip().upper() for c in renameTo.split(",") if c.strip()]

        elif isinstance(renameTo, (list, tuple)):
            toCols = [str(c).strip().upper() for c in renameTo if c is not None and str(c).strip()]

        else:
            raise ValueError((400,"The toCols Columns must be a string or a list/tuple"))       
       

        # fromCols = [c.strip().upper() for c in renameFrom.split(",") if c.strip()]
        # toCols   = [c.strip().upper() for c in renameTo.split(",") if c.strip()]

        if len(fromCols) != len(toCols):
            raise ValueError((400, f"renameFrom and renameTo count mismatch: {len(fromCols)} != {len(toCols)}"))

        renameMap = dict(zip(fromCols, toCols))

        df = pd.read_csv(dataFile)

        missing_cols = set(renameMap.keys()) - set(df.columns)
        if missing_cols:
            raise ValueError((400,f"Columns to rename not found in file: {missing_cols}"))

        df.rename(columns=renameMap, inplace=True)

        df.to_csv(dataFile, index=False)

        logging.logger("INFO", "ETL Pipeline Tool", 200, f"Renamed columns in {dataFile}: {renameMap}")

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
        return "FAILED"


def dataWriter(dataFile, outputDirectory, FileName, insertColoumns, configmapFilePath):
    try:
        if not os.path.isfile(dataFile):
            raise ValueError((400, f"The Given Data file is missing in the path: {dataFile}"))
        if not os.path.isdir(outputDirectory):
            os.makedirs(outputDirectory)
        filePath = os.path.join(outputDirectory,FileName)
        if isinstance(insertColoumns, str):
            selectCol = [c.strip().upper() for c in insertColoumns.split(",") if c.strip()]

        elif isinstance(insertColoumns, (list, tuple)):
            selectCol = [str(c).strip().upper() for c in insertColoumns if c is not None and str(c).strip()]

        else:
            raise ValueError((400,"insertColumns must be a string or a list/tuple"))
        
        if not os.path.isfile(filePath):
            with open(filePath, "w") as f:
                f.write(",".join(selectCol) + "\n")
                
        rowsWritten = 0
        with open(dataFile, "r", encoding="utf-8") as src, \
            open(filePath, "a", encoding="utf-8") as tgt:
            next(src)
            
            for line in src:
                if line.strip():
                    tgt.write(line)
                    rowsWritten += 1

        if rowsWritten == 0:
            raise ValueError((400, f"No data rows found in source file: {dataFile}"))
        
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
        return "FAILED"


# dataFile = "E:\\pythonscripts\\etlpipeline\\batchprocess\\workingdirectory\\shankar.csv"
# datafile, datafilename = datasplitter(dataFile)
# print(datafile, datafilename)

# dataFile2 = "E:\\pythonscripts\\etlpipeline\\batchprocess\\workingdirectory\\working\\file_1.csv"
# modifiedPath = fileModifier(dataFile2)
# print(modifiedPath)

# dataFile3 = "E:\\pythonscripts\\etlpipeline\\batchprocess\\workingdirectory\\working\\file_1.csv"
# config = cnfloader.load_properties()
# rename_from = config.get('renameFrom')
# rename_to = config.get('renameTo')
# modifiedPath = fileRename(dataFile3, rename_from, rename_to)
# print(modifiedPath)