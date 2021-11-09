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
          description='Load and transform data in S3 with Airflow'
        )

start_operator = DummyOperator(task_id='StartJob',  dag=dag)

data_extractor = FinancialExtractorOperator(task_id='FinancialDataExtractor',
                                            dag=dag)

finish_operator = DummyOperator(task_id='FinishJob',  dag=dag)

start_operator >> data_extractor >> finish_operator