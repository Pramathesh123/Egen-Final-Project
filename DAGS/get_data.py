import pandas as pd 
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.dummy_operator import DummyOperator 
from google.cloud import storage
from airflow.utils.dates import days_ago

DAG_NAME = "pockemon_data_dag"

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
    dag_id="pockemon_data_dag",
    default_args=default_args,
    schedule_interval = "@daily",
    catchup=False,
    description="pockemon_data_Dag",
    max_active_runs=5,
)

def get_pockemon_data():
    storage_client = storage.Client()
    bucket = storage_client.get_bucket("pockemon_data")
    blobs = bucket.list_blobs()
    print(blobs)

    try:
        for blob in blobs:
            filename = blob.name.replace('/','_')
            print(f"Downloading file{filename}")

            blob.download_to_filename(f'/home/airflow/gcs/data/{filename}')
            print(f"Concatenating {filename} together into a single dataframe")
    except:
        print("no files found!!!!")
        

get_combined_records_task = PythonOperator(
    task_id="get_pockemon_data",
    python_callable=get_pockemon_data,
    dag=dag
)
