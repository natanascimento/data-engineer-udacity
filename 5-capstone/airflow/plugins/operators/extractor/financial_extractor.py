from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
import json
import boto3
import time
import requests


class FinancialExtractorOperator(BaseOperator):
        
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, stock_method:str, *args, **kwargs):
        super(FinancialExtractorOperator, self).__init__(*args, **kwargs)
        self.__AWS_S3_BUCKET = Variable.get("S3_BUCKET_BRONZE")
        self.__AWS_ACCESS_KEY_ID = Variable.get("USER_ACCESS_KEY_ID")
        self.__AWS_SECRET_ACCESS_KEY = Variable.get("USER_SECRET_ACCESS_KEY")
        self.__STOCK_API_KEY = Variable.get("STOCK_API_KEY")
        self.__STOCK_METHOD = stock_method
        
    @staticmethod
    def __generate_uri(stock_method,
                        company_symbol,
                        stock_api_key) -> str:
        base_api = 'https://www.alphavantage.co/'

        if stock_method == 'TIME_SERIES_DAILY_ADJUSTED':
            uri = '{}query?function={}&symbol={}&outputsize={}&apikey={}'.format(base_api,
                                                                            stock_method,
                                                                            company_symbol,
                                                                            'full',
                                                                            stock_api_key)
            return uri

        uri = '{}query?function={}&symbol={}&apikey={}'.format(base_api,
                                                            stock_method,
                                                            company_symbol,
                                                            stock_api_key)
        return uri        
    
    def execute(self, context):
        try:
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=self.__AWS_ACCESS_KEY_ID,
                aws_secret_access_key=self.__AWS_SECRET_ACCESS_KEY,
            )
            
            response = s3_client.get_object(Bucket=self.__AWS_S3_BUCKET,
                                            Key="companies/s&p-500-companies.json")
                                            
            companies = json.loads(response.get("Body").read().decode('utf-8'))
            
            for company in companies: 
                uri = self.__generate_uri(self.__STOCK_METHOD,
                                    company['Symbol'],
                                    self.__STOCK_API_KEY)
                time.sleep(15)
                response = requests.get(uri)
                data = response.json()
                data = bytes(json.dumps(data).encode('UTF-8'))
                s3_client.put_object(Body=data, 
                                Bucket=self.__AWS_S3_BUCKET, 
                                Key='stock_method={}/company={}/{}.json'.format(self.__STOCK_METHOD.lower(),
                                                                            company['Symbol'],
                                                                            self.__STOCK_METHOD.lower()))
                self.log.info('{} was ingested'.format(company['Symbol']))                                                                        
            self.log.info('{} was been collected and injested to the S3'.format(self.__STOCK_METHOD))
        except Exception as exception:
            self.log.info(exception)
