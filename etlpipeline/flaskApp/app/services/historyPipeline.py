import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import config_loader as cnfloader
import logger as logging

def readPipelineHistory():
    try:
        config = cnfloader.load_properties()
        historyFile = config.get('runHistoryFile')
        if not os.path.isfile(historyFile):
            raise ValueError((400, "History File has not been found"))

        if not os.path.exists(historyFile):
            return [], []

        with open(historyFile, "r") as f:
            lines = f.readlines()

        if len(lines) < 2:
            return [], []

        header = lines[0].strip().split("|")

        rows = []
        for line in lines[1:]:
            cols = line.strip().split("|")
            if cols and cols[0].isdigit():
                rows.append(cols)

        rows.sort(key=lambda x: int(x[0]), reverse=True)

        return header, rows
    except Exception as e:
        config = cnfloader.load_properties()
        if isinstance(e.args[0], tuple) and len(e.args[0]) == 2:
            code, message = e.args[0]
        else:
            code, message = 500, str(e)
        logging.logger('ERROR', 'ETL Pipeline Tool', code, message)
        return None, None        
