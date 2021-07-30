from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os


class CreateTableOperator(BaseOperator):
        
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, redshift_conn_id = "", *args, **kwargs):
        
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.__redshift_conn_id = redshift_conn_id
        self.ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.SQL_DIR = os.path.join(self.ROOT_DIR, 'sql')
        self.SQL_FILE = os.path.join(self.SQL_DIR, 'create_tables.sql')
        
    def execute(self, context):
        self.log.info('Creating connection!')
        redshift = PostgresHook(postgres_conn_id = self.__redshift_conn_id)

        self.log.info('Executing creating tables!')
        queries =  open(self.SQL_FILE, 'r').read()
        redshift.run(queries)
        
        self.log.info("Tables was created!")