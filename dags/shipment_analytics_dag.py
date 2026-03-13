"""
Shipment Analytics Pipeline
Extracts shipment data from API and customer tiers from CSV,
then generates analytics on shipping spend by customer tier
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Add scripts directory to path
sys.path.insert(0, '/opt/airflow/scripts')

# Import pipeline functions
from extract_shipments import extract_shipments_from_api
from extract_customer_tiers import extract_customer_tiers_from_csv
from transform_data import transform_shipment_data
from load_analytics import load_analytics_data

# Default args
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

# Create DAG
dag = DAG(
    'shipment_analytics_pipeline',
    default_args=default_args,
    description='Process shipment data and generate analytics',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['analytics', 'shipments'],
)

# Task 1: Extract shipment data from API
extract_shipments_task = PythonOperator(
    task_id='extract_shipments',
    python_callable=extract_shipments_from_api,
    dag=dag,
)

# Task 2: Extract customer tier data from CSV
extract_tiers_task = PythonOperator(
    task_id='extract_customer_tiers',
    python_callable=extract_customer_tiers_from_csv,
    dag=dag,
)

# Task 3: Transform and join data
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_shipment_data,
    dag=dag,
)

# Task 4: Load analytics data
load_analytics_task = PythonOperator(
    task_id='load_analytics',
    python_callable=load_analytics_data,
    dag=dag,
)

# Define task dependencies
# Extract tasks run in parallel, then transform, then load
[extract_shipments_task, extract_tiers_task] >> transform_task >> load_analytics_task
