from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Define the DAG
dag = DAG(
    'load_dbt_models',
    schedule_interval=None,
    start_date=datetime(2023, 3, 22),
    catchup=False
)

# Define the BashOperator task
install_dependencies = BashOperator(
    task_id='install_dependencies',
    bash_command='cd /opt/airflow/dbt_snf_orka && dbt deps',
    dag=dag
)

load_raw_models = BashOperator(
    task_id='load_raw_models',
    bash_command='cd /opt/airflow/dbt_snf_orka && dbt run -s raw.*',
    dag=dag
)

load_match_scorecard = BashOperator(
    task_id='load_match_scorecard',
    bash_command='cd /opt/airflow/dbt_snf_orka && dbt run -s modelled.*',
    dag=dag
)

# Define the task dependencies
install_dependencies >> load_raw_models >> load_match_scorecard