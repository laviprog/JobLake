from __future__ import annotations

import os

from pyspark.sql import SparkSession


def create_spark(app_name: str) -> SparkSession:
    warehouse = os.getenv("JOBLAKE_WAREHOUSE", "s3://joblake/warehouse")
    catalog_uri = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "adminadmin")

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.catalog.joblake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.joblake.type", "rest")
        .config("spark.sql.catalog.joblake.uri", catalog_uri)
        .config("spark.sql.catalog.joblake.warehouse", warehouse)
        .config("spark.sql.catalog.joblake.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.joblake.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.joblake.s3.path-style-access", "true")
        .config("spark.sql.catalog.joblake.s3.access-key-id", access_key)
        .config("spark.sql.catalog.joblake.s3.secret-access-key", secret_key)
        .getOrCreate()
    )
