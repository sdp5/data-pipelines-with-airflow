from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RunListSQLOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 list_sql=None,
                 *args, **kwargs):
        super(RunListSQLOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.list_sql = list_sql if list_sql else []

    def execute(self, context):
        self.log.info('Running SQL statements using RunListSQLOperator.')
        redshift_hook = PostgresHook(self.conn_id)
        for item in self.list_sql:
            self.log.info(f'Executing SQL statement: {item}')
            redshift_hook.run(item)
