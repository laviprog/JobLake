from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
}

SPARK_SUBMIT_BASE = "docker exec joblake-spark-master spark-submit --master spark://spark-master:7077 "


with DAG(
    dag_id="daily_joblake_pipeline",
    description="Daily JobLake ingestion and lakehouse processing pipeline.",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["joblake", "lakehouse", "jobs"],
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
) as dag:

    spark_kafka_to_bronze = BashOperator(
        task_id="spark_kafka_to_bronze",
        bash_command=f"{SPARK_SUBMIT_BASE}/opt/spark-apps/kafka_to_bronze.py",
    )

    spark_bronze_to_silver = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command=f"{SPARK_SUBMIT_BASE}/opt/spark-apps/bronze_to_silver.py",
    )

    spark_silver_to_gold = BashOperator(
        task_id="spark_silver_to_gold",
        bash_command=f"{SPARK_SUBMIT_BASE}/opt/spark-apps/silver_to_gold.py",
    )

    (
        spark_kafka_to_bronze >> spark_bronze_to_silver >> spark_silver_to_gold
    )
