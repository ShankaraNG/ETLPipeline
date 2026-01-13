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
        pipeline = 'SCHEDULED_ETL'
        log = '{date} {information} [{type}] [{pipeline}] {exceptioncode} {data}'.format(date=datetime.now().strftime('%d %B %Y %H:%M:%S,%f')[:-3],information = information, type= type, exceptioncode = exceptioncode, data = data, pipeline=pipeline)
        print(log) 
        with open(logfile, 'a', encoding='utf-8') as f:
            f.write(log + '\n')
    except Exception as e:
        print(e)
        sys.exit(1)

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
        sys.exit(1)


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

