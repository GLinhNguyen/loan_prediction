# airflow_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'model_retraining',
    default_args=default_args,
    description='Retrain ML model using MongoDB data on a schedule',
    schedule_interval='@weekly',
)

extract_mongo = BashOperator(
    task_id='extract_mongo',
    bash_command='python /path/to/extract_mongo.py',
    dag=dag,
)

train_model = BashOperator(
    task_id='train_model',
    bash_command='python /path/to/model_training.py',
    dag=dag,
)

deploy_model = BashOperator(
    task_id='deploy_model',
    bash_command='python /path/to/deploy_model.py',
    dag=dag,
)

extract_mongo >> train_model >> deploy_model