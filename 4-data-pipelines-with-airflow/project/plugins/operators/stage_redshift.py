from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    __COPY_QUERY = " COPY {} \
        FROM '{}' \
        ACCESS_KEY_ID '{}' \
        SECRET_ACCESS_KEY '{}' \
        FORMAT AS json '{}'; \
    "
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credential_id="",
                 table_name = "",
                 s3_bucket="",
                 s3_key = "",
                 file_format = "",
                 log_json_file = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.__redshift_conn_id = redshift_conn_id
        self.__aws_credential_id = aws_credential_id
        self.__table_name = table_name
        self.__s3_bucket = s3_bucket
        self.__s3_key = s3_key
        self.__file_format = file_format
        self.__log_json_file = log_json_file

    def execute(self, context):
        aws_hook = AwsHook(self.__aws_credential_id)
        credentials = aws_hook.get_credentials()
        
        
        s3_path = "s3://{}/{}".format(self.__s3_bucket, self.__s3_key)
        self.log.info(f"Staging file for table {self.__table_name} from location : {s3_path}")
        
        if self.__log_json_file != "":
            self.__log_json_file = "s3://{}/{}".format(self.__s3_bucket, self.__log_json_file)
            copy_query = self.__COPY_QUERY.format(self.__table_name, s3_path, credentials.access_key, credentials.secret_key, self.__log_json_file)
        else:
            copy_query = self.__COPY_QUERY.format(self.__table_name, s3_path, credentials.access_key, credentials.secret_key, 'auto')
        
        
        self.log.info(f"Running copy query : {copy_query}")
        redshift_hook = PostgresHook(self.__redshift_conn_id)
        
        redshift_hook.run(copy_query)
        self.log.info(f"Table {self.__table_name} staged!!")
