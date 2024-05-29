from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 29),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'simple_pyspark_dag',
    default_args=default_args,
    description='A simple DAG to run a PySpark job on Kubernetes',
    schedule_interval=None,
)

# Define the PySpark job script
pyspark_script = """
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Simple DataFrame Printing Job") \
    .getOrCreate()

# Create a simple DataFrame
data = [("Alice", 34), ("Bob", 45), ("Charlie", 26)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Print the DataFrame
df.show()

# Stop the SparkSession
spark.stop()
"""

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
        '--py-files', '/tmp/gpscript.py',  # Not used, just for illustration
        '--files', '/tmp/script.py',  # Pass the script as a file
        '/tmp/script.py'  # The main Python file to run
    ],
    name='run_pyspark_job_pod',
    is_delete_operator_pod=True,
    dag=dag,
)

# Set task dependencies
run_pyspark_job_task
