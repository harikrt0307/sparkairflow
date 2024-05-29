from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime
from airflow.utils.dates import days_ago
import requests

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 29),
    'retries': 1,
}

# Function to fetch the PySpark job script from GitHub
def fetch_script():
    url = "https://raw.githubusercontent.com/harikrt0307/apache_airflow/main/dags/gpscript.py"
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Failed to fetch script from GitHub. Status code: {response.status_code}")

# Define the DAG
dag = DAG(
    'run_pyspark_job_dag',
    default_args=default_args,
    description='A DAG to run a PySpark job on Kubernetes',
    schedule_interval=None,
)

# Fetch the PySpark job script
script_content = fetch_script()

# Define the task to run the PySpark job using KubernetesPodOperator
run_pyspark_job_task = KubernetesPodOperator(
    task_id='pyspark_job',
    namespace='default',
    image='docker.io/kabileshe/originalspark:3.5.1',
    cmds=['spark-submit'],
    arguments=['--master', 'k8s://https://172.19.33.11:6443',
               '--conf', 'spark.kubernetes.container.image=docker.io/kabileshe/originalspark:3.5.1',
               '--conf', 'spark.executor.instances=2',
               '--conf', 'spark.driver.memory=4g',
               '--conf', 'spark.driver.cores=2',
               '--conf', 'spark.executor.memory=8g',
               '--conf', 'spark.executor.cores=2',
               '--conf', 'spark.dynamicAllocation.enabled=true',
               '--conf', 'spark.dynamicAllocation.initialExecutors=2',
               '--conf', 'spark.dynamicAllocation.minExecutors=2',
               '--conf', 'spark.dynamicAllocation.maxExecutors=20',
               '--conf', 'spark.dynamicAllocation.executorIdleTimeout=120s',
               '--conf', 'spark.dynamicAllocation.schedulerBacklogTimeout=5s',
               '--conf', 'spark.executor.memoryOverhead=1g',
               '--conf', 'spark.driver.memoryOverhead=1g',
               '--conf', 'spark.memory.fraction=0.6',
               '--py-files', '/tmp/gpscript.py',  # Pass the script as a file
               'main.py'],  # The main Python file to run
    name='run_pyspark_job_pod',
    is_delete_operator_pod=True,
    dag=dag,
)

# Set task dependencies
run_pyspark_job_task
