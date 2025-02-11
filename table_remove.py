from datetime import timedelta
import datetime
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator

from final_project_operators.query_run import RunListSQLOperator
from udacity.common.final_project_sql_statements import SqlQueries


default_args = {
    'owner': 'parrishm',
    'start_date': pendulum.now(),
    'email_on_retry': False
}


@dag(
    default_args=default_args,
    # depends_on_past=False,
    max_active_runs=3,
    # max_retry_delay=timedelta(minutes=5),
    catchup=False,
    schedule_interval='0 * * * *',
    description='Load and transform data in Redshift with Airflow'
)
def drop_table():

    sql_obj = SqlQueries()

    start_operator = DummyOperator(task_id='Begin_execution')
    end_operator = DummyOperator(task_id='End_execution')

    drop_table_task = RunListSQLOperator(
        task_id = "drop_table",
        conn_id="redshift",
        list_sql=sql_obj.drop_table_list
    )

    start_operator >> drop_table_task >> end_operator

drop_tables_dag = drop_table()
