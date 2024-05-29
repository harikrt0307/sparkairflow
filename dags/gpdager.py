from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import Variable
from datetime import datetime
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
    token = Variable.get("GITHUB_TOKEN")
    headers = {"Authorization": f"token {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open("/tmp/gpscript.py", "w") as f:
            f.write(response.text)
    else:
        raise Exception(f"Failed to fetch script from GitHub. Status code: {response.status_code}")

# Fetch the script before creating the DAG
fetch_script()

# Define the DAG
dag = DAG(
    'edge_dag',
    default_args=default_args,
    description='A DAG to run a PySpark job on Kubernetes',
    schedule_interval=None,
)

# Define the task to run the PySpark job using KubernetesPodOperator
run_pyspark_job_task = KubernetesPodOperator(
    task_id='run_pyspark_job_task',
    namespace='default',
    image='docker.io/kabileshe/originalspark:3.5.1',
    cmds=['spark-submit'],
    arguments=[
        '--master', 'k8s://https://172.19.33.11:6443',
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
        '/tmp/gpscript.py'  # The main Python file to run
    ],
    name='run_pyspark_job_pod',
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
)

# Set task dependencies
run_pyspark_job_task
