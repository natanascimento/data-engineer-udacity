from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os


class CreateTableOperator:
    
    
    ui_color = '#358140'
    __ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    __SQL_DIR = os.path.join(__ROOT_DIR, 'sql')
    __SQL_FILE = os.path.join(__SQL_DIR, 'create_tables.sql')
    
    
    @apply_defaults
    def __init__(self, redshift_conn_id = "", *args, **kwargs):
        
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        
    def execute(self, context):
        self.log.info('Creating connection!')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info('Executing creating tables!')
        queries =  open(__SQL_FILE, 'r').read()
        redshift.run(queries)
        
        self.log.info("Tables was created!")