from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 append=True,
                 table="",
                 *args,
                 **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.append = append
        self.table = table

    def execute(self, context):
        self.log.info('LoadDimensionOperator', self.sql)
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.table != "" and not self.append:
            self.log.info("Clearing data from destination Redshift table")
            redshift_hook.run("DELETE FROM {}".format(self.table))

        redshift_hook.run(self.sql)
