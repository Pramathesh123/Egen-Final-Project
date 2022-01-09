import pandas as pd 
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.dummy_operator import DummyOperator 
from google.cloud import storage
from google.cloud import bigquery
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.utils.dates import days_ago



DAG_NAME = "pockemon_data_Dag_transform"

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
    dag_id="pockemon_data_dag_tranform",
    default_args=default_args,
    schedule_interval = "@daily",
    catchup=False,
    description="pockemon_data_Dag_transform",
    max_active_runs=5,
)

def clean_and_process_records():
    pokemon_file = "/home/airflow/gcs/data/pokemon.csv"
    pokemon_df = pd.read_csv(pokemon_file)
    types_file = "/home/airflow/gcs/data/types.csv"
    types_df = pd.read_csv(types_file)
    moves_file = "/home/airflow/gcs/data/moves.csv"
    moves_df = pd.read_csv(moves_file)
    pokemon_species = "/home/airflow/gcs/data/pokemon_species.csv"
    pockemon_species_df = pd.read_csv(pokemon_species)
    type_names = "/home/airflow/gcs/data/type_names.csv"
    type_names_df = pd.read_csv(type_names)


    # Create a filtered dataframe from specific columns for Pokemon
    types_cols = ["id","identifier"]
    types_transformed = types_df[types_cols].copy()

    # Rename the Column Headers
    types_transformed = types_transformed.rename(columns={"id": "TID","identifier": "Type"})

    # Create new Index Column
    types_transformed.insert(0,"id",1)
    types_transformed['id'] = types_transformed.index + 1

    # Set Index
    types_transformed.set_index("id",inplace=True)

    # Capitalize 
    types_transformed['Type']=types_transformed['Type'].str.capitalize()
    types_transformed.to_csv(f'/home/airflow/gcs/data/types_transformed.csv',index=False)

    ##tranform move data
    moves_cols = ["id","identifier","type_id"]
    moves_transformed = moves_df[moves_cols].copy()

    # Rename the Column Headers
    moves_transformed = moves_transformed.rename(columns={"id": "MID","identifier": "Moves","type_id": "TID"})

    # Set index
    moves_transformed.set_index("MID", inplace=True)

    # Capitalize
    moves_transformed['Moves']=moves_transformed['Moves'].str.title()
    moves_transformed.to_csv(f'/home/airflow/gcs/data/moves_transformed.csv',index=False)

    ##transform type_names_df
    type_names_df.replace("？？？","0")
    type_names_df.replace("???","0")
    
    type_names_transformed = type_names_df.rename(columns={"type_id": "TID","local_language_id":"LANID"})
    type_names_transformed.set_index("TID", inplace=True)
    type_names_transformed.to_csv(f'/home/airflow/gcs/data/type_names_transformed.csv',index=False)

    #tranform pokemon dataframe
    # Create a filtered dataframe from specific columns for Pokemon
    pokemon_cols = ["Name","Type 1","Attack","Defense","Speed"]
    pokemon_transformed = pokemon_df[pokemon_cols].copy()

    # Rename the Column Headers
    pokemon_transformed = pokemon_transformed.rename(columns={"Name": "Pokemon_Name","Type 1": "Type"})

    # Create new Index Column
    pokemon_transformed.insert(0,"id",1)
    pokemon_transformed['id'] = pokemon_transformed.index + 1

    # Set index
    pokemon_transformed.set_index("id", inplace=True)
    pokemon_transformed.to_csv(f'/home/airflow/gcs/data/pokemon_transformed.csv',index=False)

    #merge pokemon and type tranformation dataframes
    # Merge on type to include typeid
    pokemon_merged=pokemon_transformed.merge(types_transformed, on='Type', how='outer')

    pokemon_merged_fix=pokemon_merged[['Pokemon_Name','TID','Type','Attack','Defense','Speed']].dropna()
    pokemon_merged_fix.set_index("Pokemon_Name", inplace=True)
    pokemon_merged_fix.to_csv(f'/home/airflow/gcs/data/pokemon_merged_fix.csv',index=False)


clean_and_process_records = PythonOperator(
    task_id="clean_and_process_records",
    python_callable=clean_and_process_records,
    dag=dag
)

child_task1 = ExternalTaskSensor(
    task_id="clean_and_process_records",
    external_dag_id= "pockemon_data_dag",
    external_task_id="get_pockemon_data",
    timeout=600,
    allowed_states=['success'],
    failed_states=['failed', 'skipped'],
    mode="reschedule",
)