from core.builder import SparkSessionBuilder
from core.aws import AWSConfigurator
from os.path import join
from pyspark.sql.functions import *
from pyspark.sql.types import *
from airflow.models import Variable


class GoldFactStockMarket:

    def __init__(self) -> None:
        self.__S3_BUCKET_SILVER = Variable.get("S3_BUCKET_SILVER")
        self.__S3_BUCKET_GOLD = Variable.get("S3_BUCKET_GOLD")
        self.__AWS_ACCESS_KEY_ID = Variable.get("USER_ACCESS_KEY_ID")
        self.__AWS_SECRET_ACCESS_KEY = Variable.get("USER_SECRET_ACCESS_KEY")

    @property
    def __source_path(self):
        return f"s3a://{self.__S3_BUCKET_SILVER}/stock_method=time_series_daily_adjusted/*/*.parquet"
    
    @property
    def __destination_path(self):    
        return f"s3a://{self.__S3_BUCKET_GOLD}/fact_stock_market.parquet"

    def execute(self):
        try:
            spark = SparkSessionBuilder.build()
            AWSConfigurator.configure(spark=spark, 
                                    access_key=self.__AWS_ACCESS_KEY_ID,
                                    secret_key=self.__AWS_SECRET_ACCESS_KEY)

            df_fact_stock_market = spark.read.format('parquet').load(self.__source_path)

            df_fact_stock_market.write.parquet(self.__destination_path, mode="overwrite")
        
            return "Table fact_stock_market was created into gold layer"
        except Exception as exception:
            return exception

if __name__ == "__main__":
    GoldFactStockMarket().execute()