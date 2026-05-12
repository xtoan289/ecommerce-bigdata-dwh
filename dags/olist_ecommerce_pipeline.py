from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Cấu hình mặc định cho các Task
default_args = {
    'owner': 'toan',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 12),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Khai báo DAG
with DAG(
    'olist_ecommerce_v1',
    default_args=default_args,
    description='Pipeline tu dong xu ly du lieu Olist tu Raw den Silver',
    schedule_interval=None, # Chay thu cong khi ban nhan nut Play
    catchup=False,
    tags=['pyspark', 'ecommerce']
) as dag:

    # Bước 1: Ingestion (Doc tu Raw folder va luu vao Bronze)
    task_raw_to_bronze = BashOperator(
        task_id='raw_to_bronze',
        # Duong dan trong Docker bat dau tu /opt/airflow/src
        bash_command='python /opt/airflow/src/ingestion/raw_to_bronze.py',
    )

    # Bước 2: Transformation (Loc du lieu va luu vao Silver)
    task_bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command='python /opt/airflow/src/transformation/bronze_to_silver.py',
    )

    # Thiet lap thu tu: Task 1 chay xong moi den Task 2
    task_raw_to_bronze >> task_bronze_to_silver