from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define a Python function to be executed by the PythonOperator
def print_hello():
    print("Hello from Airflow!")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
with DAG(
    'hello_airflow',
    default_args=default_args,
    description='A simple DAG to print "Hello from Airflow!"',
    schedule_interval=None,  # Run manually
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Define a single task using the PythonOperator
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello,
    )
