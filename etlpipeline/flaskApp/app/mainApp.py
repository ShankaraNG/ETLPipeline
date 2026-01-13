import services.pipelineDriver as pipelinedriver
import services.configParser as configParser
import sys
import os
import logger as logging

def RunETLMain(file, finalFileName, initialLoad, dbHost1, port1, serviceName1, userName1, password1, dbHost2, port2, serviceName2, userName2, password2, selectColumns, sourceTableName, incrementalCheck, tsCheckColumn, filterCheck, keepOnlyColumnsFromSource, renameCheck, renameFrom, renameTo, insertColumns, insertTableName, emaildistributionlist, triggeredBy):
    try:
        loggingCheck = False
        filePath, loadno = configParser.writeETLConfig(file, finalFileName, initialLoad, dbHost1, port1, serviceName1, userName1, password1, dbHost2, port2, serviceName2, userName2, password2, selectColumns, sourceTableName, incrementalCheck, tsCheckColumn, filterCheck, keepOnlyColumnsFromSource, renameCheck, renameFrom, renameTo, insertColumns, insertTableName, emaildistributionlist)
        if not os.path.isfile(filePath):
            raise Exception((400, "No Configuration file found"))
        if not loadno:
            raise Exception((400, "No Load No found"))
        
        logging.appendPipelineEntry(loadno, file, initialLoad, dbHost1, port1, serviceName1, userName1, sourceTableName, dbHost2, port2, serviceName2, userName2, insertTableName, incrementalCheck, filterCheck, renameCheck, triggeredBy, status="RUNNING")
        loggingCheck = True
        result = None
        result = pipelinedriver.startOfPipeLine(loadno, filePath)
        if result and result == "SUCCESSFUL":
            new_status = "COMPLETED"
            logging.updatePipelineStatus(loadno, new_status)
        else:
            new_status = "FAILED"
            logging.updatePipelineStatus(loadno, new_status)
        
        return "SUCCESSFUL"

    except Exception as e:
        if isinstance(e.args[0], tuple) and len(e.args[0]) == 2:
            code, message = e.args[0]
        else:
            code, message = 500, str(e)
        logging.logger('ERROR', 'ETL Pipeline Tool', code, message)
        if loggingCheck:
            new_status = "FAILED"
            logging.updatePipelineStatus(loadno, new_status)

        return "FAILED"

