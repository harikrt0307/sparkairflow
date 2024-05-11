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

with DAG('script3', default_args=default_args, schedule_interval=None) as dag:

    spark_submit_command = """
    /opt/spark/bin/spark-submit \
    --master=k8s://https://172.19.33.11:6443 \
    --deploy-mode cluster \
    --name script3 \
    --conf spark.kubernetes.container.image=kabileshe/newspark:3.5.1 \
    local:///opt/spark/examples/jars/script3.py
    """

    run_spark_job = KubernetesPodOperator(
        task_id='run_spark_job',
        namespace='default',
        image='kabileshe/newspark:3.5.1',  # Use the same image as in the spark-submit command
        cmds=["bash", "-c"],
        arguments=[spark_submit_command],
        labels={"app": "spark"},
        get_logs=True,
    )

run_spark_job
