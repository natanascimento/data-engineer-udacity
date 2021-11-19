from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import FinancialProcessorOperator


default_args = {
    'owner': 'Natan Nascimento',
    'start_date': datetime(2021, 11, 18),
    'depends_on_past': False,
    'email': ['natanascimentom@outlook.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'catchup' : False,
}

dag = DAG('PIPE_FINANCE_DATA_PROCESSING',
          default_args=default_args,
          description='Extract, Transform and Load data into Data Lake from Bronze to the Silver Layer'
        )

start_operator = DummyOperator(task_id='StartJob',  dag=dag)

earnings_processing = FinancialProcessorOperator(task_id='CompanyEarningsProcessing',
                                              stock_method='EARNINGS',
                                              dag=dag)

overview_processing = FinancialProcessorOperator(task_id='CompanyOverviewProcessing',
                                              stock_method='OVERVIEW',
                                              dag=dag)

daily_process = FinancialProcessorOperator(task_id='TimeSeriesDailyProcessing',
                                              stock_method='TIME_SERIES_DAILY_ADJUSTED',
                                              dag=dag)

finish_operator = DummyOperator(task_id='FinishJob',  dag=dag)

start_operator >> [daily_process, earnings_processing, overview_processing] >> finish_operator
