from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend


class StageToRedshiftOperator(BaseOperator):
    # Set UI color for the operator
    ui_color = '#358140'

    # SQL COPY command template to load data into Redshift
    sql_template = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS
        BLANKSASNULL
        EMPTYASNULL
        JSON '{}'
    """

    # Fields that support Jinja2 templating
    template_fields = ('s3_key',)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_format="auto",
                 *args,
                 **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format

    def execute(self, context):
        self.log.info('Starting StageToRedshiftOperator')

        # Fetch AWS credentials from secrets manager or connection
        metastore_backend = MetastoreBackend()
        aws_connection = metastore_backend.get_connection(self.aws_credentials_id)

        # Initialize the Redshift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear destination table in Redshift
        self.log.info(f"Clearing data from Redshift table: {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        # Render S3 path (Airflow resolves templated `s3_key` automatically)
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        # Format the SQL COPY command
        formatted_sql = StageToRedshiftOperator.sql_template.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.json_format
        )

        # Log and execute the formatted SQL
        self.log.info(f"Formatted SQL COPY command:\n{formatted_sql}")
        redshift.run(formatted_sql)
