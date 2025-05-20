from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Đường dẫn đến script Python gốc
SCRIPT_PATH = "/opt/airflow/scripts/EXTRACT_XML_NOKIA_MT_10.27.5.12.py"

# Hàm gọi script
def run_my_script():
    import subprocess
    subprocess.run(["python3", SCRIPT_PATH], check=True)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    dag_id='EXTRACT_XML_NOKIA_MT_10.27.5.12_job',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='15 * * * *',  
    catchup=False,
) as dag:

    task_run_script = PythonOperator(
        task_id='run_script',
        python_callable=run_my_script
    )
