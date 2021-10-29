from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import FinancialOperator

dag_name = 'PIPE_EVENT_LOGS'

default_args = {
    'owner': 'Natan Nascimento',
    'start_date': datetime(2021, 8, 5),
    'depends_on_past': False,
    'email': ['natanascimentom@outlook.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False,
}

dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval="@hourly",
        )

start_operator = DummyOperator(task_id='StartJob',  dag=dag)
financialoperator = FinancialOperator(task_id='FinancialOperator', 
                                      s3_bucket='teste',
                                      access_id_key="teste",
                                      secret_access_key="teste",
                                      dag=dag)
finish_operator = DummyOperator(task_id='FinishJob',  dag=dag)

start_operator >> financialoperator >> finish_operator