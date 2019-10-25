import os
import logging

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from datetime import datetime, timedelta

# custom utils
from das42.utils.job_config import JobConfig
from das42.utils.sql_utils import SqlUtils

JOB_ARGS = JobConfig.get_config()
DEFAULTS = JOB_ARGS["default_args"]
ENV = JOB_ARGS["env_name"]
TEAM_NAME = JOB_ARGS["team_name"]
DATEHOUR_PATH = '{{ execution_date.strftime("%Y%m%d/%H") }}'
AWS_CONN_ID = JOB_ARGS["aws_conn_id"]
AWS_BUCKET_NAME = JOB_ARGS["aws_rl_bucket_name"]
SF_CONN_ID = JOB_ARGS["snowflake_conn_id"]
SF_ROLE = JOB_ARGS["snowflake"]["role"]
SF_WAREHOUSE = JOB_ARGS["snowflake"]["warehouse"]
SF_DATABASE = JOB_ARGS["snowflake"]["database"]

# create DAG
DAG = DAG(
    "stage_adlogs",
    default_args=DEFAULTS,
    start_date=datetime(2018, 1, 1),
    schedule_interval=JOB_ARGS["schedule_interval"],
    catchup=False
)

stage_finish = DummyOperator(task_id="adlogs_snowflake_staging_finish")

# staging ad logs hourly
for table in JOB_ARGS["tables"]:

    manifest_key = os.path.join(
        JOB_ARGS["manifest_path_base"],
        JOB_ARGS["{}_manifest_name".format(table)],
        DATEHOUR_PATH,
        JOB_ARGS["manifest_filename"]
    )

    check_manifest_job = S3KeySensor(
        task_id="{}_logs_all_present".format(table),
        depends_on_past = True,
        aws_conn_id=AWS_CONN_ID,
        bucket_name=AWS_BUCKET_NAME,
        bucket_key=manifest_key,
        wildcard_match=True,
        retries=0,
        dag=DAG
    )

    stage_sql_path = os.path.join(
        JOB_ARGS["stage_sql_path"],
        table
        )

    query_log = SqlUtils.load_query(stage_sql_path).split("---")

    stage_adlogs_hourly_job = SnowflakeOperator(
        task_id="stage_logs_{}_hourly".format(table),
        snowflake_conn_id=SF_CONN_ID,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        sql=query_log,
        params={
            "env": ENV,
            "team_name": TEAM_NAME
        },
        autocommit=True,
        dag=DAG
    )

    check_manifest_job >> stage_adlogs_hourly_job >> stage_finish
