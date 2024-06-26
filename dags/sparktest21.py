from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 21),
}

with DAG('run_pyspark_scripts', schedule_interval=None, default_args=default_args, catchup=False) as dag:

    script1 = KubernetesPodOperator(
        task_id='run_script1',
        name='spark-script1',
        namespace='spark-operator',
        image='apache/spark:3.5.1',
        cmds=["/opt/spark/bin/spark-submit"],
        arguments=["--master", "k8s://https://172.19.33.11:6443", "--deploy-mode", "cluster", "https://raw.githubusercontent.com/harikrt0307/sparkairflow/main/dags/testscript2.py"],
    )

    script1
