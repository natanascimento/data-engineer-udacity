from datetime import datetime
from airflow import DAG
from os.path import join, dirname, abspath
from airflow.operators.dummy_operator import DummyOperator
from operators import DataQualityOperator


default_args = {
    'owner': 'Natan Nascimento',
    'start_date': datetime(2021, 11, 21),
    'depends_on_past': False,
    'email': ['natanascimentom@outlook.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'catchup' : False,
}

dag = DAG('PIPE_FINANCE_DATA_QUALITY_ASSURANCE',
          default_args=default_args,
          description='Assurance Data Quality into the data pipelines'
        )

start_operator = DummyOperator(task_id='StartJob',  dag=dag)

silver_quality_assurance = DataQualityOperator(task_id='DataQualitySilver', 
                                              bucket='capstone-data-engineering-silver', 
                                              dag=dag)

gold_quality_assurance = DataQualityOperator(task_id='DataQualityGold', 
                                            bucket='capstone-data-engineering-gold', 
                                            dag=dag)

finish_operator = DummyOperator(task_id='FinishJob',  dag=dag)

start_operator >> [silver_quality_assurance, gold_quality_assurance] >> finish_operator