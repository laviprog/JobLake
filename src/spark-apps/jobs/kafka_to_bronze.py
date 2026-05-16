from __future__ import annotations

import os

from common.spark_session import create_spark


def main() -> None:
    spark = create_spark("joblake-kafka-to-bronze")
    topic = os.getenv("KAFKA_TOPIC_RAW_VACANCIES", "joblake.raw.vacancies")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

    print(f"TODO: read Kafka topic {topic} from {bootstrap_servers} and write bronze.vacancies_raw")
    spark.stop()


if __name__ == "__main__":
    main()
