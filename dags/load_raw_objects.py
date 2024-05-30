from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
from snowflake.connector.pandas_tools import write_pandas
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import snowflake.connector
import json


default_args = {
    'start_date': datetime(2024, 5, 28),
    'xcom_push': True
}

snowflake_conn_id = SnowflakeHook.get_connection("snowflake_conn")
json_file_path = "/opt/airflow/dags/sources/matches.json" 
csv_file_path = "/opt/airflow/dags/sources/Players_info.csv" 


def extract_and_transform_deliveries(**kwargs):
    
    # Connect to sqlite database
    conn = sqlite3.connect('/opt/airflow/dags/sources/IPL_Deliveries.sqlite')
    print('Connected to IPL_Deliveries!')

    # Execute Query
    df = pd.read_sql_query("select *,current_timestamp as elt_update_ts from deliveries;", conn)

    # Get Snowflake connection from Airflow connection store
    extra = json.loads(snowflake_conn_id.extra)
    snowflake_table = "DELIVERIES_STAGE"
    cnx = snowflake.connector.connect(
        user=snowflake_conn_id.login,
        password=snowflake_conn_id.password,
        account=extra.get('account'),
        database='DEV',
        schema='RAW'
    )
    write_pandas(cnx , df, snowflake_table, database= 'DEV', schema='RAW', quote_identifiers= False)
    cnx.close()


def load_json_to_snowflake(**kwargs):
    
    # Read JSON data from file
    with open(json_file_path, "r") as f:
        json_data = json.load(f)

    # Convert JSON data to DataFrame
    df = pd.DataFrame(json_data)  # For single JSON object

    # Get Snowflake connection from Airflow connection store
    extra = json.loads(snowflake_conn_id.extra)
    snowflake_table = "match_info_stage"
    cnx = snowflake.connector.connect(
        user=snowflake_conn_id.login,
        password=snowflake_conn_id.password,
        account=extra.get('account'),
        database='DEV',
        schema='RAW'
    )
    write_pandas(cnx , df, snowflake_table, database= 'DEV', schema='RAW', quote_identifiers= False)
    cnx.close()

schema_type = {'BOWLING_Tests_Balls': 'str', 'BOWLING_Tests_Econ':'str', 'BOWLING_Tests_SR':'str', 'BOWLING_First-class_Ball':'str', 'BOWLING_First-class_Balls':'str',
               'BOWLING_First-class_Runs':'str', 'BOWLING_First-class_Ave':'str', 'BOWLING_First-class_Econ':'str', 'BOWLING_First-class_SR':'str', 
               'BOWLING_List A_Balls':'str', 'BOWLING_List A_Runs':'str', 'BOWLING_List A_Ave':'str', 'BOWLING_List A_Econ':'str', 'BOWLING_List A_SR':'str'}

def load_csv_to_snowflake(**kwargs):
    
    # Read CSV data into DataFrame
    df = pd.read_csv(csv_file_path, dtype=schema_type)
    df = df.rename(columns={'Unnamed: 0': "ROW_ID"})
    
    old_names = df.columns.tolist()

    def transform_column_name(old_name):
        """
        This function transforms a column name by:
          - Removing spaces and hyphens, replacing them with underscores.
          - Maintaining case sensitivity.
        """
        return old_name.replace(" ", "_").replace("-", "_")
    
    new_names = [transform_column_name(name) for name in old_names]

    # Ensure old and new names have the same length
    if len(old_names) != len(new_names):
        raise ValueError("Number of old and new names must match")

    for i in range(len(old_names)):
        df = df.rename(columns={old_names[i]: new_names[i]})

    # Get Snowflake connection from Airflow connection store
    extra = json.loads(snowflake_conn_id.extra)
    snowflake_table = "players_info_stage"
    cnx = snowflake.connector.connect(
        user=snowflake_conn_id.login,
        password=snowflake_conn_id.password,
        account=extra.get('account'),
        database='DEV',
        schema='RAW'
    )
    write_pandas(cnx , df, snowflake_table, database= 'DEV', schema='RAW', quote_identifiers= False)
    cnx.close()

with DAG(
    dag_id='load_raw_objects',
    schedule_interval=None,
    start_date=datetime.today(),  # Use today's date to avoid unnecessary backfills
    catchup=False
) as dag:

    with TaskGroup("DELIVERIES_DATALOAD", tooltip= "Load Deliveries Sqlite data to Snowflake Deliveries Table"):   
        truncate_deliveries_stage = SnowflakeOperator(
            snowflake_conn_id = 'snowflake_conn',
            task_id = 'truncate_deliveries_stage',
            sql = "truncate table dev.raw.deliveries_stage"
        )

        extract_transform_load_deliveries_stage = PythonOperator(
            task_id='extract_transform_load_deliveries_stage',
            python_callable=extract_and_transform_deliveries,
            provide_context=True
        )

        move_data_to_deliveries = SnowflakeOperator(
            snowflake_conn_id = 'snowflake_conn',
            task_id = 'move_data_to_deliveries',
            sql = "insert overwrite into dev.raw.deliveries select * from dev.raw.deliveries_stage"
        )

    with TaskGroup("MATCH_INFO_DATALOAD", tooltip= "Load MATCH Info Json data to Snowflake MATCH_INFO Table"):
        truncate_match_info_stage = SnowflakeOperator(
            snowflake_conn_id = 'snowflake_conn',
            task_id = 'truncate_match_info_stage',
            sql = "truncate table dev.raw.match_info_stage"
        )

        extract_transform_load_match_info_stage = PythonOperator(
            task_id='extract_transform_load_match_info_stage',
            python_callable=load_json_to_snowflake,
            provide_context=True
        )

        move_data_to_match_info = SnowflakeOperator(
            snowflake_conn_id = 'snowflake_conn',
            task_id = 'move_data_to_match_info',
            sql = "insert overwrite into dev.raw.match_info select * from dev.raw.match_info_stage"
        )

    with TaskGroup("PLAYERS_INFO_DATALOAD", tooltip= "Load Players Info CSV data to Snowflake PLAYERS_INFO Table"):
        truncate_players_info_stage = SnowflakeOperator(
            snowflake_conn_id = 'snowflake_conn',
            task_id = 'truncate_players_info_stage',
            sql = "truncate table dev.raw.players_info_stage"
        )


        extract_transform_load_players_info_stage = PythonOperator(
            task_id='extract_transform_load_players_info_stage',
            python_callable=load_csv_to_snowflake,
            provide_context=True
        )

        move_data_to_players_info = SnowflakeOperator(
            snowflake_conn_id = 'snowflake_conn',
            task_id = 'move_data_to_players_info',
            sql = "insert overwrite into dev.raw.players_info select * from dev.raw.players_info_stage"
        )

    trigger_dbt_models = TriggerDagRunOperator(
        task_id = 'trigger_dbt_models',
        trigger_dag_id  = "load_dbt_models",
        wait_for_completion = True,
        poke_interval = 30,
        deferrable = True
    )


truncate_deliveries_stage >> extract_transform_load_deliveries_stage >> move_data_to_deliveries

truncate_match_info_stage >> extract_transform_load_match_info_stage >> move_data_to_match_info

truncate_players_info_stage >> extract_transform_load_players_info_stage >> move_data_to_players_info

[move_data_to_deliveries, move_data_to_match_info, move_data_to_players_info] >> trigger_dbt_models