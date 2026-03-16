from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

start_date = datetime(2026, 1, 1)

with DAG("batch_dag", start_date=start_date, schedule_interval="*/2 * * * *", catchup=False) as dag:

    ingestion = BashOperator(
        task_id="ingestion",
        bash_command="docker exec ingestion python /app/batch_producer.py"
    )

    requesting = BashOperator(
        task_id="requesting",
        bash_command="docker exec batch-job python /app/batch_consumer.py"
    )

    processing = BashOperator(
        task_id="processing",
        bash_command="docker exec batch-job python /app/process.py"
    )

    ingestion >> requesting >> processing