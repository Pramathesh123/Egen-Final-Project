import pandas as pd 
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.dummy_operator import DummyOperator 
from google.cloud import storage
from google.cloud import bigquery
import os
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor

DAG_NAME = "pockemon_data_dag_load"
SQL_PASSWORD = Variable.get("db_pw")

default_args = {
    "depends_on_past" : False,
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "start_date": days_ago(1)
}

dag = DAG(
    dag_id="pockemon_data_dag_load",
    default_args=default_args,
    schedule_interval = "@daily",
    catchup=False,
    description="pockemon_data_Dag_load",
    max_active_runs=5,)


def csv_upload_to_bq(tablename,filename):
    client = bigquery.Client(project='pramathesh-shukla')
    table_id = f'pramathesh-shukla.pokemon_data.{tablename}'
    print(table_id)
    destination_table = client.get_table(table_id)

    rows_before_insert = destination_table.num_rows
    print(f"rows before insert:{rows_before_insert}")

    if rows_before_insert > 0:
        disposition = bigquery.WriteDisposition.WRITE_APPEND
        print(f"rows before insert: {rows_before_insert} i.e >0 so disposition is {disposition}")
    elif rows_before_insert == 0:
        disposition = bigquery.WriteDisposition.WRITE_EMPTY
        print(f"rows before insert:{rows_before_insert} i.e =0 so disposition is {disposition}")

    job_config = bigquery.LoadJobConfig(
        write_disposition = disposition,
        source_format = bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )

    uri = f'gs://us-east4-de-capstone-2-755cba8b-bucket/data/{filename}.csv'
    print(uri)

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )

    load_job.result()

    destination_table = client.get_table(table_id)
    rows_after_insert = destination_table.num_rows
    print(f"rows after insert: {rows_after_insert}")
    print(f"inserted {abs(rows_before_insert-rows_after_insert)} in the table. Total {rows_after_insert} rows in table now.")


    
def upload_to_bigquery():
    csv_upload_to_bq("Types","types_transformed")
    csv_upload_to_bq("Moves","moves_transformed")
    csv_upload_to_bq("Pokemon","pokemon_merged_fix")
    csv_upload_to_bq("TYPE_NAMES","type_names_transformed")


load_data = PythonOperator(
    task_id="upload_to_bigquery",
    python_callable=upload_to_bigquery,
    dag=dag
)

child_task1 = ExternalTaskSensor(
    task_id="upload_to_bigquery",
    external_dag_id= "pockemon_data_Dag_transform",
    external_task_id="clean_and_process_records",
    timeout=600,
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    mode="reschedule",
)