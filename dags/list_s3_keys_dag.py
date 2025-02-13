import pendulum
import logging

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


# Define a DAG with start date now
# Not setting a `end_date` implies a non-end running
# Not setting a `schedule` implies a `@daily` schedule by default.
@dag(start_date=pendulum.now())
def list_keys():
    @task
    def list_keys():

        # A hook to access S3
        # the connection between Airflow and S3 comes from my
        # custom airflow connection called 'aws_credentials'
        # which contains my IAM access key and secret
        hook = S3Hook(aws_conn_id='aws_credentials')

        # my custom variables on Airflow
        bucket = Variable.get('s3_bucket')
        log_data_prefix = Variable.get('s3_prefix_log_data')
        song_data_prefix = Variable.get('s3_prefix_song_data')

        logging.info(f"Listing Keys from {bucket}/{log_data_prefix}")
        keys = hook.list_keys(bucket, prefix=log_data_prefix)
        for key in keys:
            logging.info(f"- s3://{bucket}/{key}")
        logging.info('\n\n')

        logging.info(f"Listing Keys from {bucket}/{song_data_prefix}")
        keys = hook.list_keys(bucket, prefix=song_data_prefix)
        for key in keys:
            logging.info(f"- s3://{bucket}/{key}")
        logging.info('\n')

    list_keys()


list_keys_dag = list_keys()
