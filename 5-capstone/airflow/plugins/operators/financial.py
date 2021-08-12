from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os
import json
import dotenv
import boto3
import yfinance as yf
import pandas as pd

class FinancialOperator(BaseOperator):
        
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, method:str, *args, **kwargs):
        dotenv.load_dotenv(dotenv.find_dotenv())

        self.__AWS_S3_BUCKET = os.environ["S3_BUCKET"]
        self.__AWS_ACCESS_KEY_ID = os.environ["ACCESS_KEY_ID"]
        self.__AWS_SECRET_ACCESS_KEY = os.environ["SECRET_ACCESS_KEY"]
        self.__AWS_SESSION_TOKEN = os.environ["SESSION_TOKEN"]
        self.__USER_ACCESS_KEY_ID = os.environ["USER_ACCESS_KEY_ID"]
        self.__USER_SECRET_ACCESS_KEY = os.environ["USER_SECRET_ACCESS_KEY"]
        
        super(FinancialOperator, self).__init__(*args, **kwargs)
        self.__method = method

    
        
    def execute(self, context):
        self.log.info('Creating connection!')

        self.log.info('Executing creating tables!')

        
        self.log.info("Tables was created!")