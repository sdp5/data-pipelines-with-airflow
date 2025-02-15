# Airflow with AWS Services

## Using the S3Hook with your AWS Connection

```python
from airflow.hooks.S3_hook import S3Hook
. . .
hook = S3Hook(aws_conn_id='aws_credentials')
        bucket = Variable.get('s3_bucket')
        prefix = Variable.get('s3_prefix')
        logging.info(f"Listing Keys from {bucket}/{prefix}")
        keys = hook.list_keys(bucket, prefix=prefix)
        for key in keys:
            logging.info(f"- s3://{bucket}/{key}")
    list_keys()

list_keys_dag = list_keys()
```

## Saving Connections

```bash
# TO-DO: run the following command and observe the JSON output: 
# airflow connections get aws_credentials -o json 
# 
[{"id": "1", 
# "conn_id": "aws_credentials",
# "conn_type": "aws", 
# "description": "", 
# "host": "", 
# "schema": "", 
# "login": "AKIA4QE4NTH3R7EBEANN", 
# "password": "s73eJIJRbnqRtll0/YKxyVYgrDWXfoRpJCDkcG2m", 
# "port": null, 
# "is_encrypted": "False", 
# "is_extra_encrypted": "False", 
# "extra_dejson": {}, 
# "get_uri": "aws://AKIA4QE4NTH3R7EBEANN:s73eJIJRbnqRtll0%2FYKxyVYgrDWXfoRpJCDkcG2m@"
}]

# Copy the value after "get_uri":

# For example: aws://AKIA4QE4NTH3R7EBEANN:s73eJIJRbnqRtll0%2FYKxyVYgrDWXfoRpJCDkcG2m@

# TO-DO: Update the following command with the URI and un-comment it:

# airflow connections add aws_credentials --conn-uri 'aws://AKIA4QE4NTH3R7EBEANN:s73eJIJRbnqRtll0%2FYKxyVYgrDWXfoRpJCDkcG2m@'

# TO-DO: run the foll0wing command and observe the JSON output: 
# airflow connections get redshift -o json
# 
# [{"id": "3", 
# "conn_id": "redshift", 
# "conn_type": "redshift", 
# "description": "", 
# "host": "default.859321506295.us-east-1.redshift-serverless.amazonaws.com", 
# "schema": "dev", 
# "login": "awsuser", 
# "password": "R3dsh1ft", 
# "port": "5439", 
# "is_encrypted": "False", 
# "is_extra_encrypted": "False", 
# "extra_dejson": {}, 
# "get_uri": "redshift://awsuser:R3dsh1ft@default.859321506295.us-east-1.redshift-serverless.amazonaws.com:5439/dev"}]

# Copy the value after "get_uri":

# For example: redshift://awsuser:R3dsh1ft@default.859321506295.us-east-1.redshift-serverless.amazonaws.com:5439/dev
```

## Copy S3 Data

```bash
aws s3 cp s3://udacity-dend/data-pipelines/ ~/data-pipelines/ --recursive

aws s3 cp ~/data-pipelines/ s3://sean-murdock/data-pipelines/ --recursive

aws s3 ls s3://sean-murdock/data-pipelines/
```

### Example

```python
CREATE_TRIPS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trips (
trip_id INTEGER NOT NULL,
start_time TIMESTAMP NOT NULL,
end_time TIMESTAMP NOT NULL,
bikeid INTEGER NOT NULL,
tripduration DECIMAL(16,2) NOT NULL,
from_station_id INTEGER NOT NULL,
from_station_name VARCHAR(100) NOT NULL,
to_station_id INTEGER NOT NULL,
to_station_name VARCHAR(100) NOT NULL,
usertype VARCHAR(20),
gender VARCHAR(6),
birthyear INTEGER,
PRIMARY KEY(trip_id))
DISTSTYLE ALL;
"""

CREATE_STATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stations (
id INTEGER NOT NULL,
name VARCHAR(250) NOT NULL,
city VARCHAR(100) NOT NULL,
latitude DECIMAL(9, 6) NOT NULL,
longitude DECIMAL(9, 6) NOT NULL,
dpcapacity INTEGER NOT NULL,
online_date TIMESTAMP NOT NULL,
PRIMARY KEY(id))
DISTSTYLE ALL;
"""

COPY_SQL = """
COPY {}
FROM '{}'
ACCESS_KEY_ID '{{}}'
SECRET_ACCESS_KEY '{{}}'
IGNOREHEADER 1
DELIMITER ','
"""

COPY_MONTHLY_TRIPS_SQL = COPY_SQL.format(
    "trips",
    "s3://sean-murdock/data-pipelines/divvy/partitioned/{year}/{month}/divvy_trips.csv"
)

COPY_ALL_TRIPS_SQL = COPY_SQL.format(
    "trips",
    "s3://sean-murdock/data-pipelines/divvy/unpartitioned/divvy_trips_2018.csv"
)

COPY_STATIONS_SQL = COPY_SQL.format(
    "stations",
    "s3://sean-murdock/data-pipelines/divvy/unpartitioned/divvy_stations_2017.csv"
)

LOCATION_TRAFFIC_SQL = """
BEGIN;
DROP TABLE IF EXISTS station_traffic;
CREATE TABLE station_traffic AS
SELECT
    DISTINCT(t.from_station_id) AS station_id,
    t.from_station_name AS station_name,
    num_departures,
    num_arrivals
FROM trips t
JOIN (
    SELECT
        from_station_id,
        COUNT(from_station_id) AS num_departures
    FROM trips
    GROUP BY from_station_id
) AS fs ON t.from_station_id = fs.from_station_id
JOIN (
    SELECT
        to_station_id,
        COUNT(to_station_id) AS num_arrivals
    FROM trips
    GROUP BY to_station_id
) AS ts ON t.from_station_id = ts.to_station_id
"""
```

## Connections and Hooks

```python
import pendulum
import logging

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook


@dag(start_date=pendulum.now())
def list_keys():

    @task
    def list_keys():
        hook = S3Hook(aws_conn_id='aws_credentials')
        bucket = Variable.get('s3_bucket')
        prefix = Variable.get('s3_prefix')
        logging.info(f"Listing Keys from {bucket}/{prefix}")
        keys = hook.list_keys(bucket, prefix=prefix)
        for key in keys:
            logging.info(f"- s3://{bucket}/{key}")
    list_keys()

list_keys_dag = list_keys()
```

## PostgresHook and PostgresOperator

### MetastoreBackend Usage

```python
from airflow.decorators import dag
from airflow.secrets.metastore import MetastoreBackend


@dag(
    start_date=pendulum.now()
)
def load_data_to_redshift_dag():

    @task
    def copy_task():    
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        logging.info(vars(aws_connection))
```

### Using PostgresHook

```
from airflow.providers.postgres.operators.postgres import PostgresOperator
. . .
        redshift_hook = PostgresHook("redshift")

redhisft_hook.run("SELECT * FROM trips")
```

### Using PostgresOperator

```
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag

@dag(
    start_date=pendulum.now()
)
def load_data_to_redshift_dag():


...
    create_table_task=PostgresOperator(
        task_id="create_table",
        postgres_conn_id="redshift",
        sql=sql_statements.CREATE_TRIPS_TABLE_SQL
    )
...
    create_table_task >> copy_data
```
