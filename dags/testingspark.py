from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'spark_job_from_github',
    default_args=default_args,
    schedule_interval=None,
    description='Run Spark job using script from GitHub with KubernetesPodOperator',
)

# Define the KubernetesPodOperator task
submit_spark_job = KubernetesPodOperator(
    task_id='submit_spark_job',
    namespace='default',
    name='spark-job',
    image='docker.io/kabileshe/redtagspark:3.5.1',
    cmds=["/bin/bash", "-c"],
    arguments=["spark-submit --master k8s://https://172.19.33.11:6443 --deploy-mode cluster --name spark-job --conf spark.executor.instances=2 --conf spark.kubernetes.container.image=docker.io/kabileshe/redtagspark:3.5.1 https://raw.githubusercontent.com/harikrt0307/sparkairflow/main/dags/gpscript.py"],
    env_vars={
        'PYSPARK_PYTHON': '/usr/bin/python3',
    },
    get_logs=True,
    dag=dag,
)

submit_spark_job
