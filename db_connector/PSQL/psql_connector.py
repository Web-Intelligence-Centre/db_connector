import psycopg2
import logging
import pandas.io.sql as sqlio
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
import sys
import numpy as np
import pandas as pd
import sqlalchemy as sa
import traceback


class PsqlConnector:

    def addapt_numpy_float64(numpy_float64):
        return AsIs(numpy_float64)

    def addapt_numpy_int64(numpy_int64):
        return AsIs(numpy_int64)

    def addapt_numpy_float32(numpy_float32):
        return AsIs(numpy_float32)

    def addapt_numpy_int32(numpy_int32):
        return AsIs(numpy_int32)

    register_adapter(np.float64, addapt_numpy_float64)
    register_adapter(np.int64, addapt_numpy_int64)
    register_adapter(np.float32, addapt_numpy_float32)
    register_adapter(np.int32, addapt_numpy_int32)

    def __init__(self, credentials: dict = {}, **kwargs) -> None:
        self.conn = None
        self.engine = None
        self.credentials = credentials
        self.default_credentials = {
            "PGHOST": "localhost",
            "PGUSER": "postgres",
            "PGPORT": "5432",
            "PGDATABASE": "postgres",
            "PGPASSWORD": "postgres",
        }
        self.parse_credentials()
        self.create_conn()

    def help(self):
        print(
            """To create a conection first init the object with the following params .... """
        )

    def parse_credentials_to_psycopg(self):
        return f"""
                
                user={self.credentials['PGUSER']}  
                host={self.credentials['PGHOST']}
                port={self.credentials['PGPORT']} 
                dbname={self.credentials['PGDATABASE']}
                password={self.credentials['PGPASSWORD']}
                
                """

    def parse_credentials_to_sqlalchemy(self):
        return f"postgresql+psycopg2://{self.credentials['PGUSER']}:{self.credentials['PGPASSWORD']}@{self.credentials['PGHOST']}:{self.credentials['PGPORT']}/{self.credentials['PGDATABASE']}"

    def parse_credentials(self):
        if len(self.credentials.keys()) != 0:
            intersection_condition = set(self.default_credentials.keys()).intersection(
                set(self.credentials.keys())
            )
            if len(intersection_condition) > 0:
                """
                overrrides default credentials with new ones,
                if the user fortget to put some of them default
                values will be used
                """
                self.default_credentials = {
                    **self.default_credentials,
                    **self.credentials,
                }
                if len(intersection_condition) < len(self.default_credentials.keys()):
                    logging.warning(
                        f"""
                        
                        Some values were not provided, Therefore default values are going to be used for connection, 
                        just the following values were provided : 
                        {intersection_condition}
                        """
                    )
        else:
            """Credentials are going to be filled with default values"""
            self.credentials = {**self.credentials, **self.default_credentials}

    def create_conn(self):
        if self.credentials is not None:
            try:
                self.conn = psycopg2.connect(self.parse_credentials_to_psycopg())
                self.conn.autocommit = True
                logging.info("connected to postgresql !")
            except (Exception, psycopg2.DatabaseError) as error:
                logging.error(error)
                logging.error(traceback.format_exc())
                sys.exit(1)

        else:
            logging.warning("Credentials were not provider")

    def create_sqlalchemy_engine(self, **kwargs):
        if self.credentials is not None:
            try:
                self.engine = sa.create_engine(self.parse_credentials_to_sqlalchemy())
                self.engine.begin()
            except Exception as error:
                logging.error("Credentials params do not meet conditions")
                logging.error(error)
                logging.error(traceback.format_exc())
        else:
            logging.warning("Credentials was not provider")

    def create_curs(self):
        return self.conn.cursor()

    def close_conn(self):
        logging.info("Connection created !")
        self.conn.close()

    def execute_sqlio_query(self, query="", **kwargs):
        if self.engine is not None:
            try:
                df_ = sqlio.read_sql_query(query, self.engine)
                logging.info(f"Values retrieved !")
                return df_
            except Exception as error:
                logging.error("Error at execute_sqlio_query method")
                logging.error(error)
                logging.error(traceback.format_exc())
                sys.exit()
        else:
            logging.warning("SQL engine was not initialized")

    def execute_query(
        self, query="", **kwargs
    ) -> None | tuple[list[tuple], list[tuple]]:
        try:
            cursor = self.create_curs()
            cursor.execute(query)
            data = cursor.fetchall()
            cols = cursor.description
            cursor.close()
            logging.info(f"Values retrieved !")
            if len(data) == 1:
                return data[0], cols
            else:
                return data, cols
        except (Exception, psycopg2.DatabaseError) as error:
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
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error("Error at execute_query_command method")
            logging.error(error)
            logging.error(traceback.format_exc())
            return False

    def formater(self, data: list[tuple]) -> list[tuple]:

        data_formated = []
        for e in data:
            o = []
            for se in e:
                if isinstance(se, int):
                    o.append(se)
                elif isinstance(se, str):
                    se_encode = se.encode()
                    se_decode = se_encode.decode("utf-8", errors="replace").replace(
                        "\x00", ""
                    )
                    o.append(se_decode)
                else:
                    o.append(se)
            data_formated.append(tuple(o))
        return data_formated

    def insert_loop(self, data: list[tuple], insert_query: str, **kwargs) -> None:
        pgsql_cursor = self.create_curs()
        data = self.formater(data)

        tpls = []
        try:
            execute_values(pgsql_cursor, insert_query, data)
            logging.info("Batch data inserted !")
        except:
            logging.warning(
                "Batch insertion could not possible, Batch will be splited onto mini-batches"
            )
            for it, tpl in enumerate(data):
                tpls.append(tpl)
                if ((it + 1) % 10) == 0:
                    try:
                        execute_values(pgsql_cursor, insert_query, tpls)
                        logging.info("mini-batch inserted !")
                        tpls = []
                    except:
                        for e in tpls:
                            try:
                                execute_values(pgsql_cursor, insert_query, e)
                                logging.info("inserted tuple ! (one register)")
                            except (Exception, psycopg2.DatabaseError) as error:
                                logging.error("Element error")
                                logging.error(e)
                                logging.error(error)
                                logging.error(traceback.format_exc())
                                pass
            try:
                execute_values(pgsql_cursor, insert_query, tpls)
                logging.info("inserted values (rest values)")
            except:
                for e in tpls:
                    try:
                        execute_values(pgsql_cursor, insert_query, e)
                        logging.info("inserted values succes ! (at except) (one value)")
                    except (Exception, psycopg2.DatabaseError) as error:
                        logging.error("Element error")
                        logging.error(e)
                        logging.error(error)
                        logging.error(traceback.format_exc())
                        pass

        """ close cursor and commit values """
        pgsql_cursor.close()
        self.conn.commit()
