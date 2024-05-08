from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('kubernetes_pod_example', default_args=default_args, schedule_interval=None) as dag:

    passing = KubernetesPodOperator(
        task_id="passing-task",
        name="passing-test",
        namespace='default',
        image="python:3.6",
        cmds=["python", "-c"],
        arguments=["print('hello world. This is CSK Army')"],
        labels={"foo": "bar"},
        get_logs=True,
    )

passing 
