from pyspark.sql import SparkSession


class SparkSessionBuilder:

    @staticmethod
    def build():
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,") \
            .master("local[*]") \
            .getOrCreate()
        
        return spark