from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from ps_extract import extract
from ps_load import run_snowflake_load_sql
from ps_sv import data_schema_validation
from ps_transform_dq import transform_data

# Now you can import openpyxl and use it in your script

dag = DAG(
    'ps_etl',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 11, 5),
        'retries': 0,
    },
    description='A DAG to extract, transform, and load data from a file to Snowflake.',
    schedule_interval=None,
)

ps_extract = PythonOperator(
    task_id='ps_extract',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

ps_sv = PythonOperator(
    task_id='ps_schema_validation',
    python_callable=data_schema_validation,
    provide_context=True,
    dag=dag,
)

ps_transform_dq = PythonOperator(
    task_id='ps_transform',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

ps_load = PythonOperator(
    task_id='load_to_snowflake_table',
    python_callable=run_snowflake_load_sql,
    dag=dag,
)

ps_extract >> ps_sv >> ps_transform_dq >> ps_load

# Local Testing
if __name__ == "__main__":
    ps_extract.execute(context={})
