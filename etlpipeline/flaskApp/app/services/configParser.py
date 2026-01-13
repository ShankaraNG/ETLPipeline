import os
import configparser
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dao.oracleDao import OracleDAO
import config_loader as cnfloader
import logger as logging


def writeETLConfig(file, finalFileName, initialLoad, dbHost1, port1, serviceName1, userName1, password1, dbHost2, port2, serviceName2, userName2, password2, selectColumns, sourceTableName, incrementalCheck, tsCheckColumn, filterCheck, keepOnlyColumnsFromSource, renameCheck, renameFrom, renameTo, insertColumns, insertTableName, emaildistributionlist):

    config = configparser.ConfigParser()
    conf = cnfloader.load_properties()
    loadno = logging.loadnoupdate()
    outputdir = conf.get('configurationDirectory')
    if not os.path.isdir(outputdir):
        raise ValueError((400, "The Given Path for the configuration properties is throwing error"))

    fileName = f"configuration_{loadno}.properties"
    filePath = os.path.join(outputdir, fileName)

    config["DEFAULT"] = {
        "file": file,
        "finalFileName": finalFileName,
        "initialLoad": initialLoad,
        "dbHost1": dbHost1,
        "port1": port1,
        "serviceName1": serviceName1,
        "userName1": userName1,
        "password1": password1,
        "dbHost2": dbHost2,
        "port2": port2,
        "serviceName2": serviceName2,
        "userName2": userName2,
        "password2": password2,
        "selectColumns": selectColumns,
        "sourceTableName": sourceTableName,
        "incrementalCheck": incrementalCheck,
        "tsCheckColumn": tsCheckColumn,
        "filterCheck": filterCheck,
        "keepOnlyColumnsFromSource": keepOnlyColumnsFromSource,
        "renameCheck": renameCheck,
        "renameFrom": renameFrom,
        "renameTo": renameTo,
        "insertColumns": insertColumns,
        "insertTableName": insertTableName,
        "emaildistributionlist": emaildistributionlist
    }

    with open(filePath, "w") as f:
        config.write(f)

    return filePath, loadno


# path = writeETLConfig(
#         file="true",
#         finalFileName="shankar.csv",
#         initialLoad="true",

#         dbHost1="localhost",
#         port1="1521",
#         serviceName1="ORCLPDB",
#         userName1="pipeline_test",
#         password1="Shankar123",

#         dbHost2="localhost",
#         port2="1521",
#         serviceName2="ORCLPDB",
#         userName2="pipeline_test",
#         password2="Shankar123",

#         selectColumns="EMP_ID,EMP_NAME,JOINED_ON",
#         sourceTableName="EMPLOYEES",

#         incrementalCheck="true",
#         tsCheckColumn="joined_on",

#         filterCheck="true",
#         keepOnlyColumnsFromSource="EMP_ID,EMP_NAME,JOINED_ON",

#         renameCheck="true",
#         renameFrom="EMP_ID,EMP_NAME,JOINED_ON",
#         renameTo="EMP_ID,EMP_NAME,JOINED_ON",

#         insertColumns="EMP_ID,EMP_NAME,JOINED_ON",
#         insertTableName="EMPLOYEES_TARGET",

#         emaildistributionlist="shankaranarayana92@gmail.com"
#     )
