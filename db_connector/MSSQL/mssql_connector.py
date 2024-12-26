import pyodbc
import logging
import sys
import traceback


class MssqlConnector:

    def __init__(self, credentials: dict = {}, **kwargs) -> None:
        self.conn = None
        self.engine = None
        self.credentials = credentials
        self.MSSQL_DRIVER = "ODBC Driver 18 for SQL Server;TrustServerCertificate=yes"
        self.default_credentials = {
            "MSSQLHOST": "localhost",
            "MSSQLUSER": "sa",
            "MSSQLPORT": "1433",
            "MSSQLPASSWORD": "sa",
            "MSSQLDATABASE": "database",
        }
        self.create_conn()

    def parse_credentials_to_pyobdc(self):

        driver = self.MSSQL_DRIVER
        host = f"{self.credentials['MSSQLHOST']},{self.credentials['MSSQLPORT']}"
        uid = f"{self.credentials['MSSQLUSER']}"
        pwd = f"{self.credentials['MSSQLPASSWORD']}"
        database = f"{self.credentials['MSSQLPASSWORD']}"

        return {
            "driver": driver,
            "host": host,
            "uid": uid,
            "pwd": pwd,
            "database": database,
        }

    def create_conn(self):
        try:
            self.conn = pyodbc.connect(**self.parse_credentials_to_pyobdc())
            self.conn.autocommit = True
            logging.info("connected to sql server !")
        except pyodbc.Error as error:
            logging.error(error.args[1])
            logging.error(error)
            sys.exit(1)

    def create_curs(self):
        return self.conn.cursor()

    def execute_query(
        self, query="", **kwargs
    ) -> None | tuple[list[tuple], list[tuple]]:
        try:
            cursor = self.create_curs()
            data = cursor.execute(query).fetchall()
            cursor.close()
            logging.info(f"Values retrieved !")
            return data
        except (Exception, pyodbc.DatabaseError) as error:
            logging.error("Error at execute_query method")
            logging.error(error)
            logging.error(traceback.format_exc())
            return None

    def execute_query_command(self, query: str = "", **kwargs) -> bool:
        try:
            cursor = self.create_curs()
            cursor.execute(query)
            self.conn.commit()
            cursor.close()
            logging.info(f"Query executed !")
            return True
        except (Exception, pyodbc.DatabaseError) as error:
            logging.error("Error at execute_query_command method")
            logging.error(error)
            logging.error(traceback.format_exc())
            return False
