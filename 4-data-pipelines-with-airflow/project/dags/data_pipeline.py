from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import CreateTableOperator, StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from dimension_subdag import load
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

s3_bucket = os.environ.get('S3_BUCKET')
song_s3_key = os.environ.get('SONG_S3_KEY')
log_s3_key = os.environ.get('LOG_S3_KEY')
log_json_file = os.environ.get('LOG_JSON_FILE')

dag_name = 'PIPE_EVENT_LOGS'

default_args = {
    'owner': 'Natan Nascimento',
    'start_date': datetime(2021, 7, 25),
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

create_tables_in_redshift = CreateTableOperator(
    task_id = 'CreateTables',
    redshift_conn_id = 'redshift',
    dag = dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='StageEvents',
    table_name="staging_events",
    s3_bucket = s3_bucket,
    s3_key = log_s3_key,
    file_format="JSON",
    log_json_file = log_json_file,
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag,
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='StageSongs',
    table_name="staging_songs",
    s3_bucket = s3_bucket,
    s3_key = song_s3_key,
    file_format="JSON",
    redshift_conn_id = "redshift",
    aws_credential_id="aws_credentials",
    dag=dag,
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='LoadSongplaysFactTable',
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.songplay_table_insert, 
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    subdag=load(
        parent_dag_name=dag_name,
        task_id="LoadUserDimTable",
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        sql_statement=SqlQueries.user_table_insert,
        delete_load = True,
        table_name = "users",
    ),
    task_id="LoadUserDimTable",
    dag=dag,
)

load_song_dimension_table = LoadDimensionOperator(
    subdag=load(
        parent_dag_name=dag_name,
        task_id="LoadSongDimTable",
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        sql_statement=SqlQueries.song_table_insert,
        delete_load = True,
        table_name = "songs",
    ),
    task_id="LoadSongDimTable",
    dag=dag,
)

load_artist_dimension_table = LoadDimensionOperator(
    subdag=load(
        parent_dag_name=dag_name,
        task_id="LoadArtistDimTable",
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        sql_statement=SqlQueries.artist_table_insert,
        delete_load = True,
        table_name = "artists",
    ),
    task_id="LoadArtistDimTable",
    dag=dag,
)

load_time_dimension_table = LoadDimensionOperator(
    subdag=load(
        parent_dag_name=dag_name,
        task_id="LoadTimeDimTable",
        redshift_conn_id="redshift",
        start_date=default_args['start_date'],
        sql_statement=SqlQueries.time_table_insert,
        delete_load = True,
        table_name = "time",
    ),
    task_id="LoadTimeDimTable",
    dag=dag,
)

run_quality_checks = DataQualityOperator(
    task_id='DataQualityChecks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables = ["artists", "songplays", "songs", "time", "users"]
)

end_operator = DummyOperator(task_id='FinishJob',  dag=dag)

start_operator >> create_tables_in_redshift
create_tables_in_redshift >> [stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator