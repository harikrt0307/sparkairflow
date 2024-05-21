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
        env_vars={
            'SPARK_MASTER': 'k8s://https://172.19.33.11:6443',
        },
        cmds=["python", "https://raw.githubusercontent.com/harikrt0307/sparkairflow/main/dags/testscript2.py"],
    )

    script2 = KubernetesPodOperator(
        task_id='run_script2',
        name='spark-script2',
        namespace='spark-operator',
        image='apache/spark:3.5.1',
        env_vars={
            'SPARK_MASTER': 'k8s://https://172.19.33.11:6443',
        },
        cmds=["python", "https://raw.githubusercontent.com/harikrt0307/sparkairflow/main/dags/testscript.py"],
    )

    script1 >> script2
