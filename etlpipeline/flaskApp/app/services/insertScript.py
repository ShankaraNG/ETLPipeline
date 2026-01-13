import os
import sys
import pandas as pd
import services.mailing as mailing
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dao.oracleDao import OracleDAO
import config_loader as cnfloader
import logger as logging


def InsertingIntoTheDB(filePath, insertTable, insertColumns, hostServer, portOfServer, serviceName, userName, passWord, configmapFilePath):
    conn = None
    cursor = None
    try:
        #config = cnfloader.load_properties()

        # file_dir = config.get("fileDriectory")
        # file_name = config.get("fileName")
        # insert_table = config.get("insertTableName", "").strip()
        # insertCols = [c.strip().upper() for c in insertColumns.split(",") if c.strip()]

        if isinstance(insertColumns, str):
            insertCols = [c.strip().upper() for c in insertColumns.split(",") if c.strip()]
        elif isinstance(insertColumns, (list, tuple)):
            insertCols = [c.strip().upper() for c in insertColumns if c.strip()]
        else:
            raise TypeError("insertColumns must be a string or list/tuple")

        if not os.path.exists(filePath):
            logging.logger("ERROR", "ETL Pipeline Tool", 404, f"File not found: {filePath}")
            sys.exit(1)

        if not insertTable or not insertCols:
            logging.logger("ERROR", "ETL Pipeline Tool", 400, "insertTableName or insertColoumns not configured")
            sys.exit(1)

        # Read Excel (header row is automatically excluded from data)
        df = pd.read_csv(filePath)

        if df.empty:
            logging.logger("INFO", "ETL Pipeline Tool", 204, "File contains no data rows")
            return

        # Ensure correct column order
        try:
            df = df[insertCols]
        except KeyError as e:
            logging.logger("ERROR", "ETL Pipeline Tool", 400, f"Missing columns in file: {str(e)}")
            sys.exit(1)

        # Build INSERT SQL
        placeholders = ", ".join([f":{i+1}" for i in range(len(insertCols))])
        insert_sql = f"""
            INSERT INTO {insertTable}
            ({", ".join(insertCols)})
            VALUES ({placeholders})
        """

        dao = OracleDAO(host=hostServer, port=int(portOfServer), service_name=serviceName, username=userName, password=passWord)

        conn = dao.getConnection()
        if not conn:
            sys.exit(1)

        cursor = conn.cursor()
        inserted_count = 0

        try:
            for _, row in df.iterrows():
                cursor.execute(insert_sql, tuple(row))
                inserted_count += 1

            conn.commit()

            logging.logger("INFO", "ETL Pipeline Tool", 200, f"Inserted {inserted_count} rows into {insertTable}")
        
            return "SUCCESSFUL"

        except Exception as e:
            conn.rollback()
            logging.logger("ERROR", "ETL Pipeline Tool", 500, str(e))
            sys.exit(1)
            return "FAILED"

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
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
    finally:
# This part is now safe because cursor and conn are guaranteed to exist
        if cursor:
            try:
                cursor.close()
            except Exception: 
                pass # Silently fail if already closed
        if conn:
            try:
                conn.close()
            except Exception: 
                pass   

# config = cnfloader.load_properties()
# hostServer = "localhost"
# portOfServer = int(1521)
# serviceName = "ORCLPDB"
# userName = "pipeline_test"
# passWord = "Shankar123"
# insertTable = "employees_target"
# insertColumns = config.get("selectColoumns")
# print(insertColumns)
# filePath = "E:\\pythonscripts\\etlpipeline\\batchprocess\\workingdirectory\\shankar.csv"

# InsertingIntoTheDB(filePath, insertTable, insertColumns, hostServer, portOfServer, serviceName, userName, passWord)  