from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import DataLakeOperator


default_args = {
    'owner': 'Natan Nascimento',
    'start_date': datetime(2021, 11, 8),
    'depends_on_past': False,
    'email': ['natanascimentom@outlook.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'catchup' : False,
}

dag = DAG('PIPE_DATALAKE_SETUP',
          default_args=default_args,
          description='Create all layers into data lake'
        )

start_operator = DummyOperator(task_id='StartJob',  dag=dag)

layers_setup = DataLakeOperator(task_id='DataLakeLayersSetup',
                                dag=dag)

finish_operator = DummyOperator(task_id='FinishJob',  dag=dag)

start_operator >> layers_setup >> finish_operator