from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('wordscript', default_args=default_args, schedule_interval=None) as dag:
    spark_submit_command = """
        /opt/spark/bin/spark-submit \
        --master=k8s://https://172.19.33.11:6443 \
        --deploy-mode cluster \
        --name script3 \
        --conf spark.kubernetes.container.image=kabileshe/sparktest1505:1.0 \
        local:///opt/spark/script3.py
    """

    # Create a Kubernetes Volume Object for the NFS mount
    nfs_volume = k8s.V1Volume(
        name="nfs-volume",
        nfs=k8s.V1NFSVolumeSource(
            server="192.168.200.227",
            path="/BMAFTP/RTCC"
        )
    )

    # Create a Kubernetes Volume Mount Object to mount the NFS volume
    nfs_volume_mount = k8s.V1VolumeMount(
        name="nfs-volume",
        mount_path="/mnt/nfs",
        read_only=False
    )

    run_spark_job = KubernetesPodOperator(
        task_id='run_spark_job',
        namespace='default',
        image='kabileshe/sparktest1505:1.0',
        cmds=["bash", "-c"],
        arguments=[spark_submit_command],
        labels={"app": "spark"},
        get_logs=True,
        volumes=[nfs_volume],  # Add the NFS volume
        volume_mounts=[nfs_volume_mount]  # Add the NFS volume mount
    )
