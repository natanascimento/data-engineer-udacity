from pandas.core.frame import DataFrame
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
import boto3
import json
import pandas as pd 


class FinancialProcessorOperator(BaseOperator):

    ui_color = '#358140'

    @apply_defaults
    def __init__(self, stock_method:str, *args, **kwargs):
        super(FinancialProcessorOperator, self).__init__(*args, **kwargs)
        self.__AWS_S3_BUCKET_BRONZE = Variable.get("S3_BUCKET_BRONZE")
        self.__AWS_S3_BUCKET_SILVER = Variable.get("S3_BUCKET_SILVER")
        self.__AWS_ACCESS_KEY_ID = Variable.get("USER_ACCESS_KEY_ID")
        self.__AWS_SECRET_ACCESS_KEY = Variable.get("USER_SECRET_ACCESS_KEY")
        self.__STOCK_METHOD = stock_method.lower()

    @staticmethod
    def generate_file_path(stock_method: str, company: str, file_name: str, file_format: str):
        return f"stock_method={stock_method}/company={company}/{file_name}.{file_format}"

    def client_connection(self):
        client = boto3.client(
            "s3",
            aws_access_key_id=self.__AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.__AWS_SECRET_ACCESS_KEY,
        )
        return client

    def get_object(self, bucket: str, key: str):
        client = self.client_connection()
        response = client.get_object(Bucket=bucket,
                                        Key=key)
        if self.__STOCK_METHOD == "time_series_daily_adjusted":
            data = response.get("Body")
        else:
            data = json.loads(response.get("Body").read().decode('utf-8'))
        return data

    def read_from_bronze(self, company):
        data = self.get_object(bucket=self.__AWS_S3_BUCKET_BRONZE,
                        key=self.generate_file_path(stock_method=self.__STOCK_METHOD,
                                        company=company['Symbol'],
                                        file_name=self.__STOCK_METHOD,
                                        file_format="json"))
        return data 

    def save_to_silver(self, company:str, df: DataFrame):
        df.to_parquet(
            "s3://{}/{}".format(self.__AWS_S3_BUCKET_SILVER,
                                self.generate_file_path(stock_method=self.__STOCK_METHOD,
                                                        company=company['Symbol'],
                                                        file_name=self.__STOCK_METHOD,
                                                        file_format="parquet")),
            storage_options={
                "key": self.__AWS_ACCESS_KEY_ID,
                "secret": self.__AWS_SECRET_ACCESS_KEY
            },                                                                 
            index=False,
            engine="fastparquet"
        )

        return '{} was processed'.format(company['Symbol'])

    def process_earnings(self):
        companies = self.get_object(bucket=self.__AWS_S3_BUCKET_BRONZE,
                                    key="companies/s&p-500-companies.json")
        for company in companies:
            s3_data = self.read_from_bronze(company=company)
            try:
                symbol = s3_data['symbol']
                quarterlyEarnings = s3_data['quarterlyEarnings']
                df = pd.DataFrame(quarterlyEarnings)
                df['company'] = symbol
                self.log.info(self.save_to_silver(company, df))
            except Exception as exception:
                self.log.info(exception)

    def process_overview(self):
        companies = self.get_object(bucket=self.__AWS_S3_BUCKET_BRONZE,
                                    key="companies/s&p-500-companies.json")
        for company in companies:
            s3_data = self.read_from_bronze(company=company)
            try:
                df = pd.DataFrame(s3_data, index=['Symbol'])
                if "Name" in df.columns:
                    self.log.info(self.save_to_silver(company, df))
            except Exception as exception:
                self.log.info(exception)

    def process_time_series(self):
        companies = self.get_object(bucket=self.__AWS_S3_BUCKET_BRONZE,
                                    key="companies/s&p-500-companies.json")
        companies = json.loads(companies.read().decode('utf-8'))
        for company in companies:
            s3_data = self.read_from_bronze(company=company)
            try:
                df = pd.read_json(s3_data, orient='records')
                if 'Time Series (Daily)' in df.columns:
                    symbol = df['Meta Data']['2. Symbol']
                    df = df['Time Series (Daily)']
                    df.drop(df.index[[0,1,2,3,4,5]], inplace=True)

                    dataset = {"company": [],
                                "date": [], 
                                "1_open": [], 
                                "2_high": [], 
                                "3_low": [], 
                                "4_close": [], 
                                "5_adjusted_close": [], 
                                "6_volume": [], 
                                "7_dividend_amount": [], 
                                "8_split_coefficient": []}

                    for index, data in zip(df.index, df):
                        dataset["company"].append(symbol)
                        dataset["date"].append(index)
                        dataset["1_open"].append(data['1. open'])
                        dataset["2_high"].append(data['2. high'])
                        dataset["3_low"].append(data['3. low'])
                        dataset["4_close"].append(data['4. close'])
                        dataset["5_adjusted_close"].append(data['5. adjusted close'])
                        dataset["6_volume"].append(data['6. volume'])
                        dataset["7_dividend_amount"].append(data['7. dividend amount'])
                        dataset["8_split_coefficient"].append(data['8. split coefficient'])
                    
                    daily_df = pd.DataFrame(dataset)

                    self.log.info(self.save_to_silver(company, daily_df))
            except Exception as exception:
                self.log.info(exception)
         
    def execute(self, context):
        if self.__STOCK_METHOD == "earnings":
            self.process_earnings()
        if self.__STOCK_METHOD == "overview":
            self.process_overview()
        if self.__STOCK_METHOD == "time_series_daily_adjusted":
            self.process_time_series()