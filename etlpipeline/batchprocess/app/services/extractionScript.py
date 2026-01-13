import os
import sys
import pandas as pd
import subprocess
import math
from datetime import datetime
from dateutil import parser
import warnings
import services.mailing as mailing
warnings.filterwarnings("ignore", category=UserWarning)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dao.oracleDao import OracleDAO
import config_loader as cnfloader
import logger as logging


def extractSourceToFile(dbHost, Port, serviceName, userName, password, sourceTable, selectColumns, outputDirectory, outputFile, incrementalCheck, lastRunFile):

    try:

        dao = OracleDAO(host=dbHost, port=int(Port), service_name=serviceName, username=userName, password=password )
        config = cnfloader.load_properties()
        os.makedirs(outputDirectory, exist_ok=True)
        outputPath = os.path.join(outputDirectory, outputFile)

        conn = dao.getConnection()
        if not conn:
            sys.exit(1)
        cursor = conn.cursor()

        # Read last run timestamp
        LRunT = None
        if os.path.exists(lastRunFile):
            with open(lastRunFile, "r") as f:
                #LRunT = f.read().strip() or None
                raw = f.read().strip()
                if raw:
                    LRunT = parser.parse(raw, dayfirst=True)

        timeStampCheck = config.get("tsCheckColumn")
        if not timeStampCheck:
            raise ValueError("Time stamp check coloum not present")
        sourceTable = sourceTable.upper()

        # Get current max timestamp
        cursor.execute(f"SELECT MAX({timeStampCheck}) FROM {sourceTable}")
        currentMaxTs = cursor.fetchone()[0]

        if isinstance(selectColumns, str):
            selectCol = [c.strip().upper() for c in selectColumns.split(",") if c.strip()]
        elif isinstance(selectColumns, (list, tuple)):
            selectCol = [c.strip().upper() for c in selectColumns if c.strip()]
        else:
            raise ValueError((400,"selectColoumns must be string or list"))

        if incrementalCheck and LRunT and currentMaxTs:
            query = f"""
                SELECT {", ".join(selectCol)}
                FROM {sourceTable}
                WHERE {timeStampCheck} > :lrt
                  AND {timeStampCheck} <=  :cmt
            """
            params = {"lrt": LRunT, "cmt": currentMaxTs}
            df = pd.DataFrame()
            df = pd.read_sql(query, conn, params=params)
            df.to_csv(outputPath, index=False, date_format='%Y-%m-%d %H:%M:%S')
        
        # Update last run file AFTER success
        if currentMaxTs:
            with open(lastRunFile, "w") as f:
                f.write(currentMaxTs.strftime('%Y-%m-%d %H:%M:%S'))

        logging.logger("INFO", "ETL Pipeline Tool", 200,f"Extracted {len(df)} records to {outputPath}")

        return len(df)

    except Exception as e:
        config = cnfloader.load_properties()
        if isinstance(e.args[0], tuple) and len(e.args[0]) == 2:
            code, message = e.args[0]
        else:
            code, message = 500, str(e)
        logging.logger('ERROR', 'ETL Pipeline Tool', code, message)
        sys.exit(1)
        return None

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()       


def runSqlPlus(connStr, sqlQuery):
    try:
        config = cnfloader.load_properties()
        sqlPath = config.get("sqlPlusPath")
        process = subprocess.run(
            [sqlPath, "-s", connStr],
            input=sqlQuery,
            text=True,
            capture_output=True
        )
        if process.returncode != 0:
            raise RuntimeError(process.stderr)
        return process.stdout.strip()
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


def initiatextractSourceToFile(sourceTable,selectColumns, incrementalCheck, outputDirectory, outputFile, connString, lastRunFile):
    try:
        config = cnfloader.load_properties()
        if isinstance(selectColumns, str):
            selectCol = [c.strip().upper() for c in selectColumns.split(",") if c.strip()]
        elif isinstance(selectColumns, (list, tuple)):
            selectCol = [c.strip().upper() for c in selectColumns if c.strip()]
        else:
            raise ValueError((400,"selectColoumns must be string or list"))
        columnList = ",".join(selectCol)
        outputPath = os.path.join(outputDirectory, outputFile)
        sourceTable = sourceTable.upper()
        

        # 1️⃣ Get total row count
        countSql = f"""
    SET HEADING OFF FEEDBACK OFF PAGESIZE 0
    SELECT COUNT(*) FROM {sourceTable};
    EXIT
    """
        
        if incrementalCheck:
            timeStampCheck = config.get("tsCheckColumn")
            if incrementalCheck and not timeStampCheck:
                raise ValueError((400, "tsCheckColumn must be configured"))
            MaxSql = f"""SET HEADING OFF FEEDBACK OFF PAGESIZE 0
            SELECT TO_CHAR(MAX({timeStampCheck}), 'DD-MON-YYYY HH24:MI:SS') FROM {sourceTable};
            EXIT
            """
        
        result = runSqlPlus(connString, countSql)
        if not result:
            raise RuntimeError("Failed to get row count from SQL*Plus")
        totalRows = int(result)
        logging.logger("INFO", "ETL Pipeline Tool", 200, f"Total rows in source table: {totalRows}")

        if(incrementalCheck):
            batchSize=int(config.get('batchSize'))
            if not batchSize:
                raise ValueError((400,"There is no Batch size present or given"))
            
            if(totalRows>batchSize):

                totalBatches = math.ceil(totalRows / batchSize)
                logging.logger("INFO", "ETL Pipeline Tool", 200, f"Total batches: {totalBatches}")

                # Ensure output directory exists
                os.makedirs(outputDirectory, exist_ok=True)

                # Remove existing file (fresh initial load)
                if os.path.exists(outputPath):
                    os.remove(outputPath)

                # Write CSV header once
                with open(outputPath, "w") as f:
                    f.write(",".join(selectCol) + "\n")

                for batch in range(totalBatches):
                    start = batch * batchSize + 1
                    end = min((batch + 1) * batchSize, totalRows)

                    logging.logger("INFO", "ETL Pipeline Tool", 200, f"Extracting batch rows from {start} to {end})")

            #         batchSql = f"""
            # SET HEADING OFF
            # SET FEEDBACK OFF
            # SET PAGESIZE 0
            # SET LINESIZE 32767
            # SET TRIMSPOOL ON
            # SET TRIMOUT ON
            # SET TERMOUT OFF
            # SET TAB OFF
            # SET COLSEP ','

            # SELECT {columnList}
            # FROM (
            #     SELECT {columnList},
            #         ROW_NUMBER() OVER (ORDER BY {incrementalColumn}) rn
            #     FROM {sourceTable}
            # )
            # WHERE rn BETWEEN {start} AND {end}
            # ORDER BY rn;

            # EXIT
            # """

                    batchSql = f"""
            SET MARKUP CSV ON DELIMITER ',' QUOTE OFF
            SET HEADING OFF
            SET FEEDBACK OFF
            SET TERMOUT OFF
            SET PAGESIZE 0

            SELECT {columnList}
            FROM (
                SELECT {columnList},
                    ROW_NUMBER() OVER (ORDER BY {timeStampCheck}) rn
                FROM {sourceTable}
            )
            WHERE rn BETWEEN {start} AND {end}
            ORDER BY rn;

            EXIT
            """
                    data = runSqlPlus(connString, batchSql)

                    if data:
                        with open(outputPath, "a") as f:
                            f.write(data + "\n")

                logging.logger("INFO", "ETL Pipeline Tool", 200, f"CSV extraction completed successfully: {outputPath}")

                if incrementalCheck:
                    MaxTime = runSqlPlus(connString, MaxSql)

                    if not MaxTime:
                        raise ValueError("The Max Time could not be retrieved")
                    else:
                        os.makedirs(os.path.dirname(lastRunFile), exist_ok=True)
                        with open(lastRunFile, "w") as f:
                            f.write(str(MaxTime))
                
                logging.logger("INFO", "ETL Pipeline Tool", 200, f"Extracting of the data has been completed")
                logging.logger("INFO", "ETL Pipeline Tool", 200, f"The Total No of rows extracted is {totalRows}")
                return totalRows

            elif(totalRows<=batchSize):
                # Ensure output directory exists
                os.makedirs(outputDirectory, exist_ok=True)

                # Remove existing file (fresh initial load)
                if os.path.exists(outputPath):
                    os.remove(outputPath)

                with open(outputPath, "w") as f:
                    #f.write(",".join(selectCol) + "\n")
                    pass

            #     ExtractSql = f"""
            # SET HEADING OFF
            # SET FEEDBACK OFF
            # SET PAGESIZE 0
            # SET LINESIZE 32767
            # SET TRIMSPOOL ON
            # SET TRIMOUT ON
            # SET TERMOUT OFF
            # SET TAB OFF
            # SET COLSEP ','

            # SELECT {columnList}
            # FROM {sourceTable}
            # ORDER BY {incrementalColumn};

            # EXIT
            # """

                ExtractSql = f"""
            SET MARKUP CSV ON DELIMITER ',' QUOTE OFF
            SET HEADING ON
            SET FEEDBACK OFF
            SET TERMOUT OFF
            SET PAGESIZE 0


            SELECT {columnList}
            FROM {sourceTable}
            ORDER BY {timeStampCheck};

            EXIT
            """


                data = runSqlPlus(connString, ExtractSql)

                if data:
                    with open(outputPath, "a") as f:
                        f.write(data + "\n")
                
                if incrementalCheck:
                    MaxTime = runSqlPlus(connString, MaxSql)

                    if not MaxTime:
                        raise ValueError("The Max Time could not be retrieved")
                    else:
                        os.makedirs(os.path.dirname(lastRunFile), exist_ok=True)
                        with open(lastRunFile, "w") as f:
                            f.write(str(MaxTime))

                logging.logger("INFO", "ETL Pipeline Tool", 200, f"Extracting of the data has been completed")
                logging.logger("INFO", "ETL Pipeline Tool", 200, f"The Total No of rows extracted is {totalRows}")
                return totalRows

            else:
                print("exception the total number of rows were not found")
                return None
        else:
            os.makedirs(outputDirectory, exist_ok=True)
            if os.path.exists(outputPath):
                os.remove(outputPath)
            
            with open(outputPath, "w") as f:
                pass
            
            
            ESql = f"""
            SET MARKUP CSV ON DELIMITER ',' QUOTE OFF
            SET HEADING ON
            SET FEEDBACK OFF
            SET TERMOUT OFF
            SET PAGESIZE 0


            SELECT {columnList}
            FROM {sourceTable};

            EXIT
            """


            data = runSqlPlus(connString, ESql)
            
            if data:
                with open(outputPath, "a") as f:
                    f.write(data + "\n")
                

            logging.logger("INFO", "ETL Pipeline Tool", 200, f"Extracting of the data has been completed")
            logging.logger("INFO", "ETL Pipeline Tool", 200, f"The Total No of rows extracted is {totalRows}")
            
            return totalRows

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

# config = cnfloader.load_properties()
# dbHost = "localhost"
# Port = int(1521)
# serviceName = "ORCLPDB"
# userName = "pipeline_test"
# password = "Shankar123"
# sourceTable = "employees"
# selectColumns = config.get("selectColumns")
# outputDirectory = "E:\\pythonscripts\\etlpipeline\\batchprocess\\workingdirectory"
# outputFile = "shankar.csv"
# incrementalCheck = True
# incrementalColumn = "joined_on"
# lastRunFile = "E:\\pythonscripts\\etlpipeline\\batchprocess\\workingdirectory\\lastRun.txt"

# connString = f"{userName}/{password}@{dbHost}:{Port}/{serviceName}"

# # x = extractSourceToFile(dbHost, Port, serviceName, userName, password, sourceTable, selectColumns, outputDirectory, outputFile, incrementalCheck, lastRunFile)
# # print(x)

# initiatextractSourceToFile(sourceTable,selectColumns, incrementalCheck, outputDirectory, outputFile, connString, lastRunFile)


# print(pd.read_csv(os.path.join(outputDirectory, outputFile)))