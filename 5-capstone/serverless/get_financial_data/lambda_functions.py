import json
import requests
import boto3
import os

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

def lambda_handler(event, context):
    AWS_S3_BUCKET = event["S3_BUCKET"]
    AWS_ACCESS_KEY_ID = event["ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = event["SECRET_ACCESS_KEY"]
    STOCK_API_KEY = event['STOCK_API_KEY']

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    stock_method = event['stock_method']
    
    response = s3_client.get_object(Bucket=AWS_S3_BUCKET,
                                    Key="companies/s&p-500-companies.json")
                                    
    companies = json.loads(response.get("Body").read().decode('utf-8'))
    
    try:
        for company in companies:
            uri = generate_uri(stock_method,
                            company['Symbol'],
                            STOCK_API_KEY)
            response = requests.get(uri)
            data = response.json()
            data = bytes(json.dumps(data).encode('UTF-8'))
            s3_client.put_object(Body=data, 
                            Bucket=AWS_S3_BUCKET, 
                            Key='stock_method={}/company={}/{}.json'.format(stock_method.lower(),
                                                                        company['Symbol'],
                                                                        stock_method.lower()))
            
    except Exception as e:
        return {
            'statusCode': 404,
            'body': e
        }

    return {
        'statusCode': 200,
        'body': '{} was been collected and injested to the S3!'.format(stock_method)
    }
