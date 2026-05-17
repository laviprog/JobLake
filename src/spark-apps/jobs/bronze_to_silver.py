import os

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.spark_session import create_spark


BRONZE_NAMESPACE = "bronze"
BRONZE_TABLE = "vacancies_raw"
SILVER_NAMESPACE = "silver"

SILVER_TABLES = {
    "vacancies": "vacancies",
    "companies": "companies",
    "skills": "skills",
    "vacancy_skills": "vacancy_skills",
    "specializations": "specializations",
    "vacancy_specializations": "vacancy_specializations",
    "locations": "locations",
    "vacancy_locations": "vacancy_locations",
    "rejected_vacancies": "rejected_vacancies",
}

RAW_VACANCY_SCHEMA = T.StructType(
    [
        T.StructField("source", T.StringType()),
        T.StructField("id", T.StringType()),
        T.StructField("url", T.StringType()),
        T.StructField("title", T.StringType()),
        T.StructField(
            "company",
            T.StructType(
                [
                    T.StructField("id", T.LongType()),
                    T.StructField("name", T.StringType()),
                    T.StructField("url", T.StringType()),
                    T.StructField("site", T.StringType()),
                ]
            ),
        ),
        T.StructField("date_posted", T.StringType()),
        T.StructField("published_at", T.StringType()),
        T.StructField("published_title", T.StringType()),
        T.StructField("valid_through", T.StringType()),
        T.StructField("employment_type_schema", T.StringType()),
        T.StructField("employment", T.StringType()),
        T.StructField("employment_type_text", T.StringType()),
        T.StructField("remote", T.BooleanType()),
        T.StructField("job_location_type", T.StringType()),
        T.StructField("locations", T.ArrayType(T.StringType())),
        T.StructField("human_city_names", T.StringType()),
        T.StructField("short_geo", T.StringType()),
        T.StructField("qualification", T.StringType()),
        T.StructField("salary_qualification", T.StringType()),
        T.StructField("specializations", T.ArrayType(T.StringType())),
        T.StructField("skills", T.ArrayType(T.StringType())),
        T.StructField(
            "salary",
            T.StructType(
                [
                    T.StructField("salary_from", T.DoubleType()),
                    T.StructField("salary_to", T.DoubleType()),
                    T.StructField("currency", T.StringType()),
                    T.StructField("formatted", T.StringType()),
                    T.StructField("period", T.StringType()),
                ]
            ),
        ),
        T.StructField("description_html", T.StringType()),
        T.StructField("description_text", T.StringType()),
        T.StructField("banner_description", T.StringType()),
        T.StructField("_corrupt_record", T.StringType()),
    ]
)


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def catalog() -> str:
    return env("JOBLAKE_CATALOG", "joblake")


def qualified_table(namespace: str, table: str) -> str:
    return f"{catalog()}.{namespace}.{table}"


def bronze_table() -> str:
    return qualified_table(
        BRONZE_NAMESPACE, env("BRONZE_VACANCIES_TABLE", BRONZE_TABLE)
    )


def silver_table(name: str) -> str:
    return qualified_table(SILVER_NAMESPACE, SILVER_TABLES[name])


def clean_string(col: Column) -> Column:
    value = F.trim(F.regexp_replace(col.cast("string"), r"\s+", " "))
    return F.when(value == "", None).otherwise(value)


def normalized_string(col: Column) -> Column:
    return F.lower(clean_string(col))


def hash_key(*cols: Column) -> Column:
    parts = [F.coalesce(col.cast("string"), F.lit("")) for col in cols]
    return F.sha2(F.concat_ws("||", *parts), 256)


def parse_bronze_vacancies(bronze_df: DataFrame) -> DataFrame:
    parsed = F.from_json(
        F.col("payload"),
        RAW_VACANCY_SCHEMA,
        {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
    )

    return bronze_df.select(
        F.col("event_key").alias("bronze_event_key"),
        F.col("kafka_topic").alias("bronze_kafka_topic"),
        F.col("kafka_partition").alias("bronze_kafka_partition"),
        F.col("kafka_offset").alias("bronze_kafka_offset"),
        F.col("kafka_timestamp").alias("bronze_kafka_timestamp"),
        F.col("ingested_at").alias("bronze_ingested_at"),
        F.col("payload").alias("bronze_payload"),
        parsed.alias("vacancy"),
    )


def flatten_vacancies(parsed_df: DataFrame) -> DataFrame:
    source = normalized_string(F.col("vacancy.source"))
    vacancy_id = clean_string(F.col("vacancy.id"))
    company_name = clean_string(F.col("vacancy.company.name"))
    company_url = clean_string(F.col("vacancy.company.url"))
    company_site = clean_string(F.col("vacancy.company.site"))

    return (
        parsed_df.select(
            "bronze_event_key",
            "bronze_kafka_topic",
            "bronze_kafka_partition",
            "bronze_kafka_offset",
            "bronze_kafka_timestamp",
            "bronze_ingested_at",
            "bronze_payload",
            F.col("vacancy._corrupt_record").alias("corrupt_record"),
            source.alias("source"),
            vacancy_id.alias("vacancy_id"),
            clean_string(F.col("vacancy.url")).alias("url"),
            clean_string(F.col("vacancy.title")).alias("title"),
            F.col("vacancy.company.id").cast("bigint").alias("source_company_id"),
            company_name.alias("company_name"),
            company_url.alias("company_url"),
            company_site.alias("company_site"),
            F.to_date(clean_string(F.col("vacancy.date_posted"))).alias("date_posted"),
            F.to_timestamp(clean_string(F.col("vacancy.published_at"))).alias(
                "published_at"
            ),
            clean_string(F.col("vacancy.published_title")).alias("published_title"),
            F.to_date(clean_string(F.col("vacancy.valid_through"))).alias(
                "valid_through"
            ),
            clean_string(F.col("vacancy.employment_type_schema")).alias(
                "employment_type_schema"
            ),
            clean_string(F.col("vacancy.employment")).alias("employment"),
            clean_string(F.col("vacancy.employment_type_text")).alias(
                "employment_type_text"
            ),
            F.col("vacancy.remote").alias("remote"),
            clean_string(F.col("vacancy.job_location_type")).alias("job_location_type"),
            F.col("vacancy.locations").alias("locations"),
            clean_string(F.col("vacancy.human_city_names")).alias("human_city_names"),
            clean_string(F.col("vacancy.short_geo")).alias("short_geo"),
            clean_string(F.col("vacancy.qualification")).alias("qualification"),
            clean_string(F.col("vacancy.salary_qualification")).alias(
                "salary_qualification"
            ),
            F.col("vacancy.specializations").alias("specializations"),
            F.col("vacancy.skills").alias("skills"),
            F.col("vacancy.salary.salary_from").cast("double").alias("salary_from"),
            F.col("vacancy.salary.salary_to").cast("double").alias("salary_to"),
            clean_string(F.col("vacancy.salary.currency")).alias("salary_currency"),
            clean_string(F.col("vacancy.salary.formatted")).alias("salary_formatted"),
            clean_string(F.col("vacancy.salary.period")).alias("salary_period"),
            clean_string(F.col("vacancy.description_html")).alias("description_html"),
            clean_string(F.col("vacancy.description_text")).alias("description_text"),
            clean_string(F.col("vacancy.banner_description")).alias(
                "banner_description"
            ),
        )
        .withColumn(
            "company_identity",
            F.coalesce(
                F.col("source_company_id").cast("string"),
                normalized_string(F.col("company_name")),
                normalized_string(F.col("company_url")),
                normalized_string(F.col("company_site")),
            ),
        )
        .withColumn(
            "company_key",
            F.when(
                F.col("company_identity").isNotNull(),
                hash_key(F.col("source"), F.col("company_identity")),
            ),
        )
        .withColumn(
            "vacancy_uid",
            F.when(
                F.col("source").isNotNull() & F.col("vacancy_id").isNotNull(),
                hash_key(F.col("source"), F.col("vacancy_id")),
            ),
        )
    )


def select_latest_vacancies(flat_df: DataFrame) -> DataFrame:
    window = Window.partitionBy("source", "vacancy_id").orderBy(
        F.col("bronze_kafka_timestamp").desc_nulls_last(),
        F.col("bronze_ingested_at").desc_nulls_last(),
        F.col("bronze_kafka_offset").desc_nulls_last(),
    )

    return (
        flat_df.filter(
            F.col("corrupt_record").isNull()
            & F.col("source").isNotNull()
            & F.col("vacancy_id").isNotNull()
        )
        .withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def build_rejected_vacancies(flat_df: DataFrame) -> DataFrame:
    return (
        flat_df.filter(
            F.col("corrupt_record").isNotNull()
            | F.col("source").isNull()
            | F.col("vacancy_id").isNull()
        )
        .withColumn(
            "reject_reason",
            F.when(F.col("corrupt_record").isNotNull(), F.lit("invalid_json"))
            .when(F.col("source").isNull(), F.lit("missing_source"))
            .when(F.col("vacancy_id").isNull(), F.lit("missing_vacancy_id"))
            .otherwise(F.lit("unknown")),
        )
        .select(
            "bronze_event_key",
            "bronze_kafka_topic",
            "bronze_kafka_partition",
            "bronze_kafka_offset",
            "bronze_kafka_timestamp",
            "bronze_ingested_at",
            "reject_reason",
            "corrupt_record",
            "bronze_payload",
            F.current_timestamp().alias("silver_processed_at"),
        )
    )


def build_vacancies(latest_df: DataFrame) -> DataFrame:
    return latest_df.select(
        "vacancy_uid",
        "source",
        "vacancy_id",
        "url",
        "title",
        "company_key",
        "date_posted",
        "published_at",
        "published_title",
        "valid_through",
        "employment_type_schema",
        "employment",
        "employment_type_text",
        "remote",
        "job_location_type",
        "human_city_names",
        "short_geo",
        "qualification",
        "salary_qualification",
        "salary_from",
        "salary_to",
        "salary_currency",
        "salary_formatted",
        "salary_period",
        "description_html",
        "description_text",
        "banner_description",
        "bronze_event_key",
        "bronze_kafka_topic",
        "bronze_kafka_partition",
        "bronze_kafka_offset",
        "bronze_kafka_timestamp",
        "bronze_ingested_at",
        F.current_timestamp().alias("silver_processed_at"),
    )


def build_companies(latest_df: DataFrame) -> DataFrame:
    window = Window.partitionBy("company_key").orderBy(
        F.col("bronze_kafka_timestamp").desc_nulls_last(),
        F.col("bronze_ingested_at").desc_nulls_last(),
        F.col("bronze_kafka_offset").desc_nulls_last(),
    )

    return (
        latest_df.filter(F.col("company_key").isNotNull())
        .withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .select(
            "company_key",
            "source",
            "source_company_id",
            F.col("company_name").alias("name"),
            F.col("company_url").alias("url"),
            F.col("company_site").alias("site"),
            F.col("bronze_ingested_at").alias("last_seen_at"),
            F.current_timestamp().alias("silver_processed_at"),
        )
    )


def explode_items(
    latest_df: DataFrame,
    array_col: str,
    item_col: str,
    key_col: str,
    position_col: str,
) -> DataFrame:
    window = Window.partitionBy("vacancy_uid", key_col).orderBy(
        F.col(position_col).asc()
    )

    return (
        latest_df.select(
            "vacancy_uid",
            "source",
            "vacancy_id",
            F.posexplode_outer(F.col(array_col)).alias(position_col, item_col),
        )
        .withColumn(item_col, clean_string(F.col(item_col)))
        .filter(F.col(item_col).isNotNull())
        .withColumn("normalized_name", normalized_string(F.col(item_col)))
        .withColumn(key_col, hash_key(F.col("normalized_name")))
        .withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def build_dimension(items_df: DataFrame, item_col: str, key_col: str) -> DataFrame:
    return (
        items_df.groupBy(key_col, "normalized_name")
        .agg(F.min(F.col(item_col)).alias("name"))
        .withColumn("silver_processed_at", F.current_timestamp())
    )


def build_vacancy_item_bridge(
    items_df: DataFrame,
    key_col: str,
    position_col: str,
) -> DataFrame:
    return items_df.select(
        "vacancy_uid",
        "source",
        "vacancy_id",
        key_col,
        F.col(position_col).cast("int").alias(position_col),
        F.current_timestamp().alias("silver_processed_at"),
    )


def build_silver_frames(bronze_df: DataFrame) -> dict[str, DataFrame]:
    flat_df = flatten_vacancies(parse_bronze_vacancies(bronze_df))
    latest_df = select_latest_vacancies(flat_df)

    skills_df = explode_items(
        latest_df, "skills", "skill_name", "skill_key", "skill_position"
    )
    specializations_df = explode_items(
        latest_df,
        "specializations",
        "specialization_name",
        "specialization_key",
        "specialization_position",
    )
    locations_df = explode_items(
        latest_df,
        "locations",
        "location_name",
        "location_key",
        "location_position",
    )

    return {
        "vacancies": build_vacancies(latest_df),
        "companies": build_companies(latest_df),
        "skills": build_dimension(skills_df, "skill_name", "skill_key"),
        "vacancy_skills": build_vacancy_item_bridge(
            skills_df,
            "skill_key",
            "skill_position",
        ),
        "specializations": build_dimension(
            specializations_df,
            "specialization_name",
            "specialization_key",
        ),
        "vacancy_specializations": build_vacancy_item_bridge(
            specializations_df,
            "specialization_key",
            "specialization_position",
        ),
        "locations": build_dimension(locations_df, "location_name", "location_key"),
        "vacancy_locations": build_vacancy_item_bridge(
            locations_df,
            "location_key",
            "location_position",
        ),
        "rejected_vacancies": build_rejected_vacancies(flat_df),
    }


def ensure_silver_tables(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog()}.{SILVER_NAMESPACE}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {silver_table("vacancies")} (
            vacancy_uid STRING,
            source STRING,
            vacancy_id STRING,
            url STRING,
            title STRING,
            company_key STRING,
            date_posted DATE,
            published_at TIMESTAMP,
            published_title STRING,
            valid_through DATE,
            employment_type_schema STRING,
            employment STRING,
            employment_type_text STRING,
            remote BOOLEAN,
            job_location_type STRING,
            human_city_names STRING,
            short_geo STRING,
            qualification STRING,
            salary_qualification STRING,
            salary_from DOUBLE,
            salary_to DOUBLE,
            salary_currency STRING,
            salary_formatted STRING,
            salary_period STRING,
            description_html STRING,
            description_text STRING,
            banner_description STRING,
            bronze_event_key STRING,
            bronze_kafka_topic STRING,
            bronze_kafka_partition INT,
            bronze_kafka_offset BIGINT,
            bronze_kafka_timestamp TIMESTAMP,
            bronze_ingested_at TIMESTAMP,
            silver_processed_at TIMESTAMP
        )
        USING iceberg
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {silver_table("companies")} (
            company_key STRING,
            source STRING,
            source_company_id BIGINT,
            name STRING,
            url STRING,
            site STRING,
            last_seen_at TIMESTAMP,
            silver_processed_at TIMESTAMP
        )
        USING iceberg
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {silver_table("skills")} (
            skill_key STRING,
            normalized_name STRING,
            name STRING,
            silver_processed_at TIMESTAMP
        )
        USING iceberg
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {silver_table("vacancy_skills")} (
            vacancy_uid STRING,
            source STRING,
            vacancy_id STRING,
            skill_key STRING,
            skill_position INT,
            silver_processed_at TIMESTAMP
        )
        USING iceberg
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {silver_table("specializations")} (
            specialization_key STRING,
            normalized_name STRING,
            name STRING,
            silver_processed_at TIMESTAMP
        )
        USING iceberg
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {silver_table("vacancy_specializations")} (
            vacancy_uid STRING,
            source STRING,
            vacancy_id STRING,
            specialization_key STRING,
            specialization_position INT,
            silver_processed_at TIMESTAMP
        )
        USING iceberg
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {silver_table("locations")} (
            location_key STRING,
            normalized_name STRING,
            name STRING,
            silver_processed_at TIMESTAMP
        )
        USING iceberg
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {silver_table("vacancy_locations")} (
            vacancy_uid STRING,
            source STRING,
            vacancy_id STRING,
            location_key STRING,
            location_position INT,
            silver_processed_at TIMESTAMP
        )
        USING iceberg
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {silver_table("rejected_vacancies")} (
            bronze_event_key STRING,
            bronze_kafka_topic STRING,
            bronze_kafka_partition INT,
            bronze_kafka_offset BIGINT,
            bronze_kafka_timestamp TIMESTAMP,
            bronze_ingested_at TIMESTAMP,
            reject_reason STRING,
            corrupt_record STRING,
            bronze_payload STRING,
            silver_processed_at TIMESTAMP
        )
        USING iceberg
        """
    )


def overwrite_silver_table(df: DataFrame, table: str) -> None:
    df.writeTo(table).overwrite(F.lit(True))

spark = create_spark("joblake-bronze-to-silver")

try:
    ensure_silver_tables(spark)
    frames = build_silver_frames(spark.table(bronze_table()))

    for name in SILVER_TABLES:
        overwrite_silver_table(frames[name], silver_table(name))
        print(f"Overwritten {silver_table(name)}")
finally:
    spark.stop()
