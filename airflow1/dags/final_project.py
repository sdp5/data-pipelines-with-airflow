from datetime import timedelta
import datetime
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.final_project_operators.empty import EmptyOperator

from final_project_operators.stage_redshift import StageJson2RedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from final_project_operators.query_run import RunListSQLOperator
from udacity.common.final_project_sql_statements import SqlQueries

# from airflow.final_project_operators.postgres_operator import PostgresOperator

sdate = "02/10/2025"
date_run = datetime.datetime.strptime(sdate, "%d/%m/%Y")
# date_run  = datetime.datetime.now()

default_args = {
    'owner': 'parrishm',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'max_active_runs': 3
}


@dag(
    default_args=default_args,
    catchup=False,
    schedule_interval='0 * * * *',
    description='Load and transform data in Redshift with Airflow'
)
def final_project():
    sql_obj = SqlQueries()

    start_operator = DummyOperator(task_id='Begin_execution')
    end_operator = DummyOperator(task_id='End_execution')

    create_table_task = RunListSQLOperator(
        task_id="Create_table",
        conn_id="redshift",
        list_sql=sql_obj.create_table_list
    )

    stage_events_to_redshift = StageJson2RedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="mpd608",
        s3_key="log-data/{{ execution_date.year }}/{{ execution_date.month }}/",
        json_format="s3://mpd608/events_jpaths.json",
        # execution_date=date_run
    )

    stage_songs_to_redshift = StageJson2RedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="mpd608",
        s3_key="song-data",
        json_format="auto"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        sql=sql_obj.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        append=True,
        table='users',
        redshift_conn_id="redshift",
        sql=sql_obj.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        append=True,
        table='songs',
        redshift_conn_id="redshift",
        sql=sql_obj.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        append=True,
        table='artists',
        redshift_conn_id="redshift",
        sql=sql_obj.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        append=True,
        table='times',
        redshift_conn_id="redshift",
        sql=sql_obj.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        table="songplays"
    )

    start_operator >> create_table_task
    create_table_task >> stage_events_to_redshift >> load_songplays_table
    create_table_task >> stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator


final_project_dag = final_project()
