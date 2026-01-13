import sys
import os
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config_loader as cnfloader


def logger(information, type, exceptioncode, data):
    try:
        config = cnfloader.load_properties()
        logfile = config.get('loggingFilePath')
        if not os.path.exists(logfile):
            raise ValueError("Unable to find the log path")
        pipeline = 'TRIGGERED_ETL'
        log = '{date} {information} [{type}] [{pipeline}] {exceptioncode} {data}'.format(date=datetime.now().strftime('%d %B %Y %H:%M:%S,%f')[:-3],information = information, type= type, exceptioncode = exceptioncode, data = data, pipeline=pipeline)
        print(log) 
        with open(logfile, 'a', encoding='utf-8') as f:
            f.write(log + '\n')
    except Exception as e:
        print(e)

def startinglogger(data):
    try:
        print(data)
        config = cnfloader.load_properties()
        logfile = config.get('loggingFilePath')
        if not os.path.exists(logfile):
            with open(logfile, "w", encoding="utf-8"):
                pass
        with open(logfile, 'a', encoding='utf-8') as f:
            f.write(data + '\n')
    except Exception as e:
        print(e)


def loadnoupdate():
    try:
        config = cnfloader.load_properties()
        loadnofile = config.get('pipelineloadfile')
        if not loadnofile:
            raise ValueError((400, 'No File Path Found'))
        with open(loadnofile, 'r') as file:
            LoadNos = file.read().strip()
            
        if LoadNos and LoadNos.isdigit():
            LoadNo = int(LoadNos)
            LoadNo = LoadNo + 1
            with open(loadnofile, 'w') as file:
                file.write(str(LoadNo))

            return LoadNo
        else:
            raise ValueError((400, 'No Load No Found'))
    except Exception as e:
        print(e)
        return None
    
def appendPipelineEntry(pipeline_no, fileCheckMark, initialLoad, sourcedb, sourceport, sourceServiceName, userName, sourceTable, targetDbHost, targetPort, targetServiceName, targetUserName, targetTable, incrementalCheck, filterCheck, renameCheck, triggeredBy, status):
    try:
        # -------- Load & validate config --------
        config = cnfloader.load_properties()
        file_path = config.get("runHistoryFile")

        if not file_path:
            raise ValueError("runHistoryFile is missing in configuration")

        data = (
            f"{pipeline_no}|{fileCheckMark}|{initialLoad}|"
            f"{sourcedb}|{sourceport}|{sourceServiceName}|"
            f"{userName}|{sourceTable}|"
            f"{targetDbHost}|{targetPort}|{targetServiceName}|"
            f"{targetUserName}|{targetTable}|"
            f"{incrementalCheck}|{filterCheck}|{renameCheck}|"
            f"{triggeredBy}|{status}"
        )

        with open(file_path, "a+", encoding="utf-8") as file:
            file.seek(0)
            content = file.read()
            if content and not content.endswith("\n"):
                file.write("\n")
            file.write(data + "\n")
    except Exception as e:
        print(e)

def updatePipelineStatus(pipeline_no, new_status):
    try:
        # -------- Load & validate config --------
        config = cnfloader.load_properties()
        runHistoryFile = config.get("runHistoryFile")

        if not runHistoryFile:
            raise ValueError("runHistoryFile is missing in configuration")

        updated = False

        # -------- Read file --------
        with open(runHistoryFile, "r", encoding="utf-8") as f:
            lines = f.readlines()

        if not lines:
            return False

        header = lines[0].rstrip("\n")
        updated_lines = [header]

        # -------- Process rows --------
        for line in lines[1:]:
            line = line.rstrip("\n")
            columns = line.split("|")

            # Column 0 = Pipe Line No (string comparison)
            if columns and columns[0] == str(pipeline_no):
                # Last column = Status
                columns[-1] = new_status
                updated = True

            updated_lines.append("|".join(columns))

        # -------- Write back only if updated --------
        if updated:
            with open(runHistoryFile, "w", encoding="utf-8") as f:
                f.write("\n".join(updated_lines))
    except Exception as e:
        print(e)


