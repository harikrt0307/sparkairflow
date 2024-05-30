from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the volume and volume mount
volume = Volume(
    name='airflow-rwm-pvc',
    configs={
        'persistentVolumeClaim': {
            'claimName': 'airflow-rwm-pvc'
        }
    }
)

volume_mount = VolumeMount(
    'airflow-rwm-pvc',
    mount_path='/mnt/scripts',
    sub_path=None,
    read_only=True
)

with DAG('inserttestert', default_args=default_args, schedule_interval=None) as dag:
    spark_submit_command = """
        /opt/spark/bin/spark-submit \
        --master=k8s://https://172.19.33.11:6443 \
        --deploy-mode cluster \
        --name insert-into-greenplum \
        --conf spark.kubernetes.container.image=kabileshe/gptest:1.2 \
        local:///mnt/scripts/script1.py
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
        volumes=[volume],
        volume_mounts=[volume_mount],
    )

run_spark_job
