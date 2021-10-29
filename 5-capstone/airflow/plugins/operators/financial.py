from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import os
import json
import boto3


class FinancialOperator(BaseOperator):
        
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, s3_bucket:str, 
                access_id_key:str, 
                secret_access_key:str,
                stock_api_key:str,
                *args, **kwargs):
        super(FinancialOperator, self).__init__(*args, **kwargs)
        self.__AWS_S3_BUCKET = s3_bucket
        self.__AWS_ACCESS_KEY_ID = access_id_key
        self.__AWS_SECRET_ACCESS_KEY = secret_access_key
        self.__STOCK_API_KEY = stock_api_key
        
    @staticmethod
    def generate_uri(stock_method,
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

        self.log.info(self.__AWS_S3_BUCKET)
        self.log.info(self.__AWS_ACCESS_KEY_ID)
        self.log.info(self.__AWS_SECRET_ACCESS_KEY)