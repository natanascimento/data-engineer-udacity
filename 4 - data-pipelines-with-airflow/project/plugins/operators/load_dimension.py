from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query = "",
                 delete_load = False,
                 table_name = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.__redshift_conn_id = redshift_conn_id
        self.__sql_query = sql_query
        self.__delete_load = delete_load
        self.__table_name = table_name

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.__redshift_conn_id)
        if self.__delete_load:
            self.log.info(f"Running delete statement on table {self.__table_name}")
            redshift_hook.run(f"DELETE FROM {self.__table_name}")
            
        self.log.info(f"Running query to load data into Dimension Table {self.__table_name}")
        redshift_hook.run(self.__sql_query)
        self.log.info(f"Dimension Table {self.__table_name} loaded.")
