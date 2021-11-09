from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
import json
import boto3
import requests


class DataLakeOperator(BaseOperator):

    ui_color = '#358140'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DataLakeOperator, self).__init__(*args, **kwargs)
        self.__S3_BUCKET_BRONZE = Variable.get("S3_BUCKET_BRONZE")
        self.__S3_BUCKET_SILVER = Variable.get("S3_BUCKET_SILVER")
        self.__S3_BUCKET_GOLD = Variable.get("S3_BUCKET_GOLD")
        self.__S3_BUCKET_SPARK_CODE = Variable.get("S3_BUCKET_SPARK_CODE")
        self.__layers = [self.__S3_BUCKET_BRONZE, self.__S3_BUCKET_SILVER, 
                        self.__S3_BUCKET_GOLD, self.__S3_BUCKET_SPARK_CODE]
        self.__AWS_ACCESS_KEY_ID = Variable.get("USER_ACCESS_KEY_ID")
        self.__AWS_SECRET_ACCESS_KEY = Variable.get("USER_SECRET_ACCESS_KEY")
        self.__COMPANIES_URL = Variable.get("COMPANIES_URL")

    def s3_client(self):
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.__AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.__AWS_SECRET_ACCESS_KEY,
        )
        return s3_client

    def create_companies_list(self):
        response = requests.get(self.__COMPANIES_URL)
        data = response.json()
        data = json.dumps(data).encode('UTF-8')
        self.s3_client().put_object(Body=data, 
                                Bucket=self.__S3_BUCKET_BRONZE, 
                                Key='companies/s&p-500-companies.json')
        self.log.info('Companies list was injected!')       
        
    def create_data_lake_layers(self):
        for layer in self.__layers:
            self.s3_client().create_bucket(Bucket=f'{layer}',
                                        CreateBucketConfiguration={'LocationConstraint': 'us-west-1'})
            self.log.info(f'{layer} was created')

    def execute(self, context):
        try:
            self.create_data_lake_layers()
            self.create_companies_list()
        except Exception as exception:
            self.log.info(exception)