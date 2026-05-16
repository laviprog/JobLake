from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="daily_joblake_pipeline",
    description="Daily JobLake ingestion and lakehouse processing pipeline.",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["joblake", "lakehouse", "jobs"],
) as dag:
    validate_sources_config = BashOperator(
        task_id="validate_sources_config",
        bash_command="python - <<'PY'\nfrom pathlib import Path\ncfg = Path('/opt/airflow/configs/sources.yaml')\ntext = cfg.read_text()\nassert 'sources:' in text, 'sources key is required'\nprint(f'Validated {cfg}')\nPY",
    )

    run_collector = BashOperator(
        task_id="run_collector",
        bash_command="echo 'TODO: run joblake-collector service or container task'",
    )

    spark_kafka_to_bronze = BashOperator(
        task_id="spark_kafka_to_bronze",
        bash_command="echo 'TODO: spark-submit /opt/spark-apps/jobs/kafka_to_bronze.py'",
    )

    spark_bronze_to_silver = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command="echo 'TODO: spark-submit /opt/spark-apps/jobs/bronze_to_silver.py'",
    )

    spark_silver_to_gold = BashOperator(
        task_id="spark_silver_to_gold",
        bash_command="echo 'TODO: spark-submit /opt/spark-apps/jobs/silver_to_gold.py'",
    )

    data_quality_checks = EmptyOperator(task_id="data_quality_checks")

    (
        validate_sources_config
        >> run_collector
        >> spark_kafka_to_bronze
        >> spark_bronze_to_silver
        >> spark_silver_to_gold
        >> data_quality_checks
    )
