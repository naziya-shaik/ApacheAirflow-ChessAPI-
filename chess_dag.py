from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from chess_api import chess_etl_program
from snowflake_connect import snowflake_connection

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 28),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'chess_dag', #This name will show in the airflow UI
    default_args=default_args,
    description='ETL job for extracting data from chess.com API'
)

run_etl = PythonOperator(
    task_id='complete_chess_etl', # This will show in task Ui inside the dag
    python_callable=chess_etl_program,
    dag=dag,
)

snowflake_task = PythonOperator(
    task_id = 'snowflake_task', # This will show in task Ui inside the dag
    python_callable = snowflake_connection,
    dag=dag,
)

run_etl >> snowflake_task