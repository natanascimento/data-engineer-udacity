from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import FinancialExtractorOperator


default_args = {
    'owner': 'Natan Nascimento',
    'start_date': datetime(2021, 10, 30),
    'depends_on_past': False,
    'email': ['natanascimentom@outlook.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'catchup' : False,
}

dag = DAG('PIPE_FINANCE_DATA',
          default_args=default_args,
          description='Extract and Load data into Data Lake from Finance API with Airflow'
        )

start_operator = DummyOperator(task_id='StartJob',  dag=dag)

daily_extractor = FinancialExtractorOperator(task_id='TimeSeriesDailyDataExtractor',
                                            stock_method='TIME_SERIES_DAILY_ADJUSTED',
                                            dag=dag)

overview_extractor = FinancialExtractorOperator(task_id='CompanyOverviewDataExtractor',
                                            stock_method='OVERVIEW',
                                            dag=dag)

earnings_extractor = FinancialExtractorOperator(task_id='CompanyEarningsExtractor',
                                            stock_method='EARNINGS',
                                            dag=dag)

finish_operator = DummyOperator(task_id='FinishJob',  dag=dag)

start_operator >> [daily_extractor,overview_extractor,earnings_extractor] >> finish_operator
