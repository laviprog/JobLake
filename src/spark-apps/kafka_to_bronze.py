import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.spark_session import create_spark


CATALOG = "joblake"
NAMESPACE = "bronze"
TABLE = "vacancies_raw"


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def qualified_table() -> str:
    return f"{CATALOG}.{NAMESPACE}.{TABLE}"


def ensure_bronze_table(spark) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table()} (
            kafka_topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP,
            kafka_timestamp_type INT,
            event_key STRING,
            payload STRING,
            headers ARRAY<STRUCT<key: STRING, value: STRING>>,
            ingested_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(ingested_at))
        """
    )


def read_kafka_stream(spark, bootstrap_servers: str, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", env("KAFKA_STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", env("KAFKA_FAIL_ON_DATA_LOSS", "false"))
        .option("includeHeaders", "true")
        .load()
    )


def to_bronze_events(kafka_df: DataFrame) -> DataFrame:
    return kafka_df.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("timestampType").alias("kafka_timestamp_type"),
        F.col("key").cast("string").alias("event_key"),
        F.col("value").cast("string").alias("payload"),
        F.expr(
            """
            transform(
                headers,
                header -> named_struct(
                    'key', header.key,
                    'value', cast(header.value as string)
                )
            )
            """
        ).alias("headers"),
        F.current_timestamp().alias("ingested_at"),
    )



spark = create_spark("joblake-kafka-to-bronze")
topic = env("KAFKA_TOPIC_RAW", "joblake.raw.vacancies")
bootstrap_servers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
checkpoint_location = env(
    "BRONZE_CHECKPOINT_LOCATION",
    "/opt/spark-data/checkpoints/kafka_to_bronze/vacancies_raw",
)

try:
    ensure_bronze_table(spark)

    bronze_df = to_bronze_events(read_kafka_stream(spark, bootstrap_servers, topic))
    query = (
        bronze_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .trigger(availableNow=True)
        .toTable(qualified_table())
    )
    query.awaitTermination()
finally:
    spark.stop()

