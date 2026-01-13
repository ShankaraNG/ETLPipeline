import oracledb
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import logger as logging


class OracleDAO:

    def __init__(self, host, port, service_name, username, password):
        self.host = host
        self.port = port
        self.service_name = service_name
        self.username = username
        self.password = password

    def getConnection(self):

        try:
            dsn = oracledb.makedsn(
                host=self.host,
                port=self.port,
                service_name=self.service_name
            )

            conn = oracledb.connect(
                user=self.username,
                password=self.password,
                dsn=dsn
            )

            return conn
        except Exception as e:
            if isinstance(e.args[0], tuple) and len(e.args[0]) == 2:
                code, message = e.args[0]
            else:
                code, message = 500, str(e)
            logging.logger('ERROR', 'ETL Pipeline Tool', code, message)
            if code in [ 400, 404]:
                sys.exit(1)
            return None
