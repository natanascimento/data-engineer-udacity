from pyspark.sql import SparkSession


class AWSConfigurator:

    @staticmethod
    def configure(spark: SparkSession,
                access_key: str,
                secret_key: str):

        sc = spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        hadoop_conf.set("fs.s3a.fast.upload", "true")
        hadoop_conf.set("fs.s3.buffer.dir", "/root/spark/work,/tmp")
        hadoop_conf.set("fs.s3a.awsAccessKeyId", access_key)
        hadoop_conf.set("fs.s3a.awsSecretAccessKey", secret_key)