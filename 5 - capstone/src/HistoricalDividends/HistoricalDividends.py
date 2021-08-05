import os
import json
import dotenv
import boto3
import yfinance as yf
import pandas as pd


def lambda_handler(event, context):
    
    dotenv.load_dotenv(dotenv.find_dotenv())

    AWS_S3_BUCKET = os.environ["S3_BUCKET"]
    AWS_ACCESS_KEY_ID = os.environ["ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = os.environ["SECRET_ACCESS_KEY"]
    AWS_SESSION_TOKEN = os.environ["SESSION_TOKEN"]
    USER_ACCESS_KEY_ID = os.environ["USER_ACCESS_KEY_ID"]
    USER_SECRET_ACCESS_KEY = os.environ["USER_SECRET_ACCESS_KEY"]

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
    )

    response = s3_client.get_object(Bucket=AWS_S3_BUCKET,
                                    Key="companies/s&p-500-companies.json")

    data = json.loads(response.get("Body").read().decode('utf-8') )

    storage_options = {'key': USER_ACCESS_KEY_ID,
                    'secret': USER_SECRET_ACCESS_KEY}

    for company in data:
        ticket = str(company['Symbol'])
        stocks = yf.Ticker(ticket)
        dividends = stocks.dividends
        df = pd.DataFrame(data=dividends)
        df.to_json('s3://{}/historical-dividends/Ticket={}/dividends.json'.format(AWS_S3_BUCKET, ticket, ticket),
                    storage_options={storage_options})

    return {
        'statusCode': 200,
        'body': json.dumps('Historical Dividends was collected!')
    }