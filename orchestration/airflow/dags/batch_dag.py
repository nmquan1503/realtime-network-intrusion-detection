from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from kubernetes.client import models as k8s


NAMESPACE = "bigdata"
CONFIG_ENV_FROM = [
    k8s.V1EnvFromSource(
        config_map_ref=k8s.V1ConfigMapEnvSource(name="config")
    )
]
PROCESS_DATE_ENV = k8s.V1EnvVar(name="PROCESS_DATE", value="{{ ds }}")
POD_IP_ENV = k8s.V1EnvVar(
    name="POD_IP",
    value_from=k8s.V1EnvVarSource(
        field_ref=k8s.V1ObjectFieldSelector(field_path="status.podIP")
    ),
)

BATCH_IMAGE = "mquan1503/bigdata-batch:latest"


def build_resources(request_cpu, request_memory, limit_cpu, limit_memory):
    return k8s.V1ResourceRequirements(
        requests={"cpu": request_cpu, "memory": request_memory},
        limits={"cpu": limit_cpu, "memory": limit_memory},
    )


def build_batch_task(
    dag,
    task_id,
    image,
    command,
    env_vars=None,
    resources=None,
):
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace=NAMESPACE,
        image=image,
        cmds=command,
        env_vars=env_vars or [],
        env_from=CONFIG_ENV_FROM,
        container_resources=resources,
        labels={"pipeline": "batch-training", "step": task_id},
        get_logs=True,
        is_delete_operator_pod=True,
        image_pull_policy="IfNotPresent",
        in_cluster=True,
        log_events_on_failure=True,
        reattach_on_restart=False,
        startup_timeout_seconds=300,
        do_xcom_push=False,
        dag=dag,
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="batch_training_pipeline_k8s",
    description="Daily batch training pipeline for realtime intrusion detection",
    default_args=default_args,
    start_date=datetime(2018, 2, 14),
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["batch", "training", "kubernetes"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    consume_kafka_to_bronze = build_batch_task(
        dag=dag,
        task_id="consume_kafka_to_bronze",
        image=BATCH_IMAGE,
        command=["python3", "-u", "/app/consumer.py"],
        env_vars=[PROCESS_DATE_ENV],
        resources=build_resources("250m", "512Mi", "500m", "1Gi"),
    )

    preprocess_bronze_to_silver = build_batch_task(
        dag=dag,
        task_id="preprocess_bronze_to_silver",
        image=BATCH_IMAGE,
        command=["python3", "-u", "/app/preprocessing.py"],
        env_vars=[PROCESS_DATE_ENV, POD_IP_ENV],
        resources=build_resources("1", "3Gi", "2", "4Gi"),
    )

    feature_engineering = build_batch_task(
        dag=dag,
        task_id="feature_engineering",
        image=BATCH_IMAGE,
        command=["python3", "-u", "/app/feature_engineering.py"],
        env_vars=[PROCESS_DATE_ENV, POD_IP_ENV],
        resources=build_resources("1", "3Gi", "2", "4Gi"),
    )

    train_model = build_batch_task(
        dag=dag,
        task_id="train_model",
        image=BATCH_IMAGE,
        command=["python3", "-u", "/app/trainer.py"],
        env_vars=[PROCESS_DATE_ENV, POD_IP_ENV],
        resources=build_resources("2", "4Gi", "3", "6Gi"),
    )

    wait_streaming_model_reload = TimeDeltaSensor(
        task_id="wait_streaming_model_reload",
        delta=timedelta(seconds=75),
        mode="reschedule",
    )

    start >> consume_kafka_to_bronze
    consume_kafka_to_bronze >> preprocess_bronze_to_silver
    preprocess_bronze_to_silver >> feature_engineering >> train_model
    train_model >> wait_streaming_model_reload >> end
