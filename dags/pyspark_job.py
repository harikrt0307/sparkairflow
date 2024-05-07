from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
}

with DAG('pyspark_job', default_args=default_args, schedule_interval=None) as dag:

    pyspark_task = KubernetesPodOperator(
        task_id='pyspark_task',
        name='pyspark_job',
        namespace='airflow',
        image='kabileshe/newspark:3.5.1',
        cmds=[
            '/spark/bin/spark-submit',
            '--master=k8s://https://172.19.33.11:6443',
            '--deploy-mode', 'cluster',
            '--name', 'script',
            '--conf', 'spark.kubernetes.authenticate.driver.serviceAccountName=newspark',
            'local:///opt/spark/examples/jars/script1.py'
        ],
        is_delete_operator_pod=True,
        get_logs=True
    )
