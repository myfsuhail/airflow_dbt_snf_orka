from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sqlite3
import os

default_args = {
    'start_date': datetime(2024,5,28),
    'xcom_push': True
}

def extract_deliveries(**kwargs):
    #connect sqlite database
    print(os.getcwd())
    os.chdir('/opt/airflow/dags/sources')
    print(os.getcwd())
    conn = sqlite3.connect('IPL_Deliveries.sqlite')
    cursor = conn.cursor()

    print('Connected to IPL_Deliveries!')

    #Execute Query
    cursor.execute("select *,current_timestamp as elt_update_ts from deliveries;")
    data = cursor.fetchall()

    cursor.close()
    conn.close()

    #Push data to XCom
    kwargs['ti'].xcom_push(key='extracted_data', value=data)

# transforms the data
def transform_deliveries(**kwargs):

    # gets the task instance object from the kwargs dictionary
    ti = kwargs['ti']

    # gets the table data from the XCOM store.
    table_data = ti.xcom_pull(task_ids='extract_data_from_sqlite', key='extracted_data')

    # creates a query to insert the data into the specified table
    query = f'insert into dev.raw.deliveries values'

    # iterates over the table data and adds each row to the query.
    for row in table_data:
        # remove the square bracket
        query += f" ({str(row)[1:-1]}),"
    
    # adds a semicolon to the end of the query.
    query = query[:-1] + ';'
    
    return query

with DAG (
    dag_id = 'load_sqlite_deliveries',
    schedule_interval=None,
    start_date=datetime(2023, 3, 22),
    catchup=False
) as dag:

    extract_deliveries_from_sqlite = PythonOperator(
        task_id = 'extract_deliveries_from_sqlite',
        provide_context=True,
        python_callable = extract_deliveries
    )

    transform_deliveries = PythonOperator(
            task_id='transform_deliveries',
            python_callable=transform_deliveries,
            provide_context=True
        )

    load_deliveries_to_snowflake = SnowflakeOperator(
        task_id = 'load_deliveries_to_snowflake',
        snowflake_conn_id = 'snowflake_conn',
        autocommit = True,
        sql = transform_deliveries.output

    )

    extract_deliveries_from_sqlite >> transform_deliveries >> load_deliveries_to_snowflake