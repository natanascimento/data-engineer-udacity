from create_tables import main as create_tables
from etl import main as etl_pipeline

if __name__ == "__main__":
    create_tables()
    etl_pipeline()
    print("All tasks on data pipeline was completed!!!\n")