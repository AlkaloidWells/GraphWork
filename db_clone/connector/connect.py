import os
from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()

# Your connection classes
class DBConnectionOnline:
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    database = os.getenv('DB_NAME')

    def create_db_connection(self):
        conn = create_engine('mysql+pymysql://' + self.user + ':' + self.password + '@' + self.host + ':' + str(self.port) + '/' + self.database,
                             echo=False)

        return conn



class DBConnectionLocal:
    user = os.getenv('LDB_USER')
    password = os.getenv('LDB_PASSWORD')
    host = os.getenv('LDB_HOST')
    port = os.getenv('LDB_PORT')
    database = os.getenv('LDB_NAME')

    def create_db_connection(self):
        conn = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}', echo=False)
        return conn
