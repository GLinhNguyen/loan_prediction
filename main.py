from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import joblib
from etl import train_preprocessor
from model_training import train_model
import os
# Set base directory (Use Airflow's /opt/airflow/data)
BASE_DIR = "/opt/airflow/data"
RAW_DATA_PATH = os.path.join(BASE_DIR, "Loan_default.csv")
EXTRACTED_DATA_PATH = os.path.join(BASE_DIR, "temp_extracted.csv")
TRANSFORMED_DATA_PATH = os.path.join(BASE_DIR, "temp_transformed.csv")

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 1),
    "retries": 1,
}

# Define DAG
dag = DAG(
    "loan_etl_dag",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # Run daily at 3 AM
    catchup=False,  # Avoid backfilling old dates
)

# Extract Task
def extract():
    """Extract batch data from CSV"""
    df = pd.read_csv(RAW_DATA_PATH)
    df.to_csv(EXTRACTED_DATA_PATH, index=False)

# Transform Task
def transform():
    """Apply ETL transformations"""
    df = pd.read_csv(EXTRACTED_DATA_PATH)
    
    # Apply transformation
    transformed_df = train_preprocessor(df)  
    
    # Save transformed data
    transformed_df.to_csv(TRANSFORMED_DATA_PATH, index=False)

# Train Task
def train():
    """Train ML model on transformed batch data"""
    train_model(TRANSFORMED_DATA_PATH)

# Airflow Task Definitions
extract_task = PythonOperator(task_id="extract", python_callable=extract, dag=dag)
transform_task = PythonOperator(task_id="transform", python_callable=transform, dag=dag)
train_task = PythonOperator(task_id="train", python_callable=train, dag=dag)

# Task Dependencies
extract_task >> transform_task >> train_task