from os import EX_CANTCREAT
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
import boto3


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, bucket:str, *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.__BUCKET = bucket
        self.__AWS_ACCESS_KEY_ID = Variable.get("USER_ACCESS_KEY_ID")
        self.__AWS_SECRET_ACCESS_KEY = Variable.get("USER_SECRET_ACCESS_KEY")

    def client_connection(self):
        client = boto3.client(
            "s3",
            aws_access_key_id=self.__AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.__AWS_SECRET_ACCESS_KEY,
        )
        return client

    def execute(self, context):
        try:
            bucket = self.client_connection().Bucket(self.__BUCKET)
            records = []
            for obj in bucket.objects.all():
                records.append(obj)
            if len(records) < 1:
                raise ValueError(f"Data quality check failed. {self.__BUCKET} returned no results")
            self.log.info(f"Data quality on bucket {self.__BUCKET} check passed with {len(records)} records")
        except Exception as exception:
            self.log.info(exception)