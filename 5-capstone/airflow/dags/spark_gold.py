from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from operators import DataQualityOperator
from os.path import join, dirname, abspath

default_args = {
    'owner': 'Natan Nascimento',
    'start_date': datetime(2021, 11, 20),
    'depends_on_past': False,
    'email': ['natanascimentom@outlook.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'catchup' : False,
}

dag = DAG('PIPE_FINANCE_GOLD_PROCESSING',
          default_args=default_args,
          description='Extract, Transform and Load data into Data Lake from Bronze to the Silver Layer'
        )

start_operator = DummyOperator(task_id='StartJob',  dag=dag)

gold_dim_company_address = SparkSubmitOperator(task_id='CreateGoldDimCompanyAddress',
                                              conn_id='spark_default',
                                              application="spark-code/gold_dim_company_address.py",
                                              total_executor_cores=1,
                                              packages="org.apache.hadoop:hadoop-aws:2.7.0",
                                              executor_cores=1,
                                              executor_memory='8g',
                                              driver_memory='8g',
                                              name='gold_dim_company_address',
                                              dag=dag
                                              )

gold_dim_company = SparkSubmitOperator(task_id='CreateGoldDimCompany',
                                              conn_id='spark_default',
                                              application="spark-code/gold_dim_company.py",
                                              total_executor_cores=1,
                                              packages="org.apache.hadoop:hadoop-aws:2.7.0",
                                              executor_cores=1,
                                              executor_memory='8g',
                                              driver_memory='8g',
                                              name='gold_dim_company',
                                              dag=dag
                                              )

fact_operator = DummyOperator(task_id='InitializeFact',  dag=dag)

gold_fact_company_metrics = SparkSubmitOperator(task_id='CreateGoldFactCompanyMetrics',
                                              conn_id='spark_default',
                                              application="spark-code/gold_fact_company_metrics.py",
                                              total_executor_cores=1,
                                              packages="org.apache.hadoop:hadoop-aws:2.7.0",
                                              executor_cores=1,
                                              executor_memory='8g',
                                              driver_memory='8g',
                                              name='gold_fact_company_metrics',
                                              dag=dag
                                              )

gold_fact_stock_market = SparkSubmitOperator(task_id='CreateGoldFactStockMarket',
                                              conn_id='spark_default',
                                              application="spark-code/gold_fact_stock_market.py",
                                              total_executor_cores=1,
                                              packages="org.apache.hadoop:hadoop-aws:2.7.0",
                                              executor_cores=1,
                                              executor_memory='8g',
                                              driver_memory='8g',
                                              name='gold_fact_stock_market',
                                              dag=dag
                                              )

gold_fact_earnings = SparkSubmitOperator(task_id='CreateGoldFactCompanyEarnings',
                                              conn_id='spark_default',
                                              application="spark-code/gold_fact_earnings.py",
                                              total_executor_cores=1,
                                              packages="org.apache.hadoop:hadoop-aws:2.7.0",
                                              executor_cores=1,
                                              executor_memory='8g',
                                              driver_memory='8g',
                                              name='gold_fact_earnings',
                                              dag=dag
                                              )

finish_operator = DummyOperator(task_id='FinishJob',  dag=dag)

start_operator >> [gold_dim_company_address, gold_dim_company] >> fact_operator >> [gold_fact_company_metrics, gold_fact_stock_market, gold_fact_earnings] >> finish_operator

