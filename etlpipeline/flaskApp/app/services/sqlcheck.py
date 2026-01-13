import sys
import os
import services.mailing as mailing
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from dao.oracleDao import OracleDAO
import logger as logging
import config_loader as cnfloader


#logging.logger('ERROR', 'Certificate Viewer Tool', code, message)

def parseTableName(tableName):
    if "." in tableName:
        owner, table = tableName.split(".", 1)
        return owner.upper(), table.upper()
    return None, tableName.upper()


def tableExists(dbHost, Port, serviceName, userName, password, tableName, configmapFilePath):
    conn = None
    cursor = None
    try:
        dao = OracleDAO(
            host=dbHost,
            port=int(Port),
            service_name=serviceName,
            username=userName,
            password=password
        )

        conn = dao.getConnection()
        if not conn:
            return False

        cursor = conn.cursor()
        owner, table = parseTableName(tableName)

        if owner:
            sql = """
                SELECT COUNT(*)
                FROM ALL_TABLES
                WHERE OWNER = :owner
                  AND TABLE_NAME = :tbl
            """
            cursor.execute(sql, owner=owner, tbl=table)
        else:
            sql = """
                SELECT COUNT(*)
                FROM ALL_TABLES
                WHERE TABLE_NAME = :tbl
            """
            cursor.execute(sql, tbl=table)

        return cursor.fetchone()[0] > 0
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
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()



def getTableColumns(dbHost, Port, serviceName, userName, password, tableName, configmapFilePath):
    conn = None
    cursor = None
    try:
        dao = OracleDAO(
            host=dbHost,
            port=int(Port),
            service_name=serviceName,
            username=userName,
            password=password
        )

        conn = dao.getConnection()
        if not conn:
            return set()

        cursor = conn.cursor()
        owner, table = parseTableName(tableName)

        if owner:
            sql = """
                SELECT COLUMN_NAME
                FROM ALL_TAB_COLUMNS
                WHERE OWNER = :owner
                  AND TABLE_NAME = :tbl
            """
            cursor.execute(sql, owner=owner, tbl=table)
        else:
            sql = """
                SELECT COLUMN_NAME
                FROM ALL_TAB_COLUMNS
                WHERE TABLE_NAME = :tbl
            """
            cursor.execute(sql, tbl=table)

        return {row[0] for row in cursor.fetchall()}
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
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def validateColumns(dbHost, Port, serviceName, userName, password, tableName, selectColumns, configmapFilePath):
    try:
        tableColumns = getTableColumns(dbHost, Port, serviceName, userName, password, tableName, configmapFilePath)
        if tableColumns is None:
            raise Exception((400, "Missing table Columns"))
        missing = None

        tabCol = {c.upper() for c in tableColumns}

        if isinstance(selectColumns, str):
            selectCol = [c.strip().upper() for c in selectColumns.split(",") if c.strip()]
        elif isinstance(tableColumns, (list, tuple)):
            selectCol = [c.strip().upper() for c in selectColumns if c.strip()]
        else:
            raise ValueError((400,"selectColumns must be string or list"))
        
        missing = [col.upper() for col in selectCol if col.upper() not in tabCol]
        
        return missing
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
        return "ERROR"



# dbHost = "localhost"
# Port = int(1521)
# serviceName = "ORCLPDB"
# userName = "pipeline_test"
# password = "Shankar123"
# tableName = "employees_target"
# selectColumns = "emp_id", "emp_name", "joined_on"

# hello = validateColumns(dbHost, Port, serviceName, userName, password, tableName, selectColumns)
# if hello:
#     print(hello)
# else:
#     print('true')

# hello = tableExists(dbHost, Port, serviceName, userName, password, tableName)

# if not hello:
#     print(hello)