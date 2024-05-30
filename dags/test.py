from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Define the DAG
dag = DAG(
    'hello_world',
    description='A simple tutorial DAG',
    schedule_interval=None,
    start_date=datetime(2023, 3, 22),
    catchup=False
)

# Define the BashOperator task
first_model = BashOperator(
    task_id='first_model',
    bash_command='cd /opt/airflow/dbt_snf_orka && dbt run -s my_first_dbt_model',
    dag=dag
)

second_model = BashOperator(
    task_id='second_model',
    bash_command='cd /opt/airflow/dbt_snf_orka && dbt run -s my_second_dbt_model',
    dag=dag
)

# Define the task dependencies
first_model >> second_model