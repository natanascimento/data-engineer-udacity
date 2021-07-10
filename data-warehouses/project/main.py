from config import Config
from pipe import ETL, DatabaseManager

if __name__ == '__main__':
  #Startup database tables
  database_manager = DatabaseManager()
  database_manager.process()
  etl = ETL()
  etl.process()