from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG('insert_into_green', default_args=default_args, schedule_interval=None) as dag:
    spark_submit_command = """
        /opt/spark/bin/spark-submit \
        --master=k8s://https://172.19.33.11:6443 \
        --deploy-mode cluster \
        --name insert-into-greenplum \
        --conf spark.kubernetes.container.image=kabileshe/gptest:1.2 \
        local:///opt/spark/gpscript.py
    """

    run_spark_job = KubernetesPodOperator(
        task_id='run_spark_job',
        name='insert-into-greenplum',
        namespace='airflow',
        image='kabileshe/gptest:1.2',
        cmds=["bash", "-c"],
        arguments=[spark_submit_command],
        labels={"app": "spark"},
        get_logs=True,
    )

run_spark_job
