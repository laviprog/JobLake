import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from common.spark_session import create_spark


SILVER_NAMESPACE = "silver"
GOLD_NAMESPACE = "gold"

SILVER_TABLES = {
    "vacancies": "vacancies",
    "companies": "companies",
    "skills": "skills",
    "vacancy_skills": "vacancy_skills",
    "specializations": "specializations",
    "vacancy_specializations": "vacancy_specializations",
    "locations": "locations",
    "vacancy_locations": "vacancy_locations",
}

GOLD_TABLES = {
    "vacancies_enriched": "vacancies_enriched",
    "vacancy_overview_daily": "vacancy_overview_daily",
    "skill_demand": "skill_demand",
    "specialization_demand": "specialization_demand",
    "location_demand": "location_demand",
    "company_demand": "company_demand",
    "salary_distribution": "salary_distribution",
}


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def catalog() -> str:
    return env("JOBLAKE_CATALOG", "joblake")


def qualified_table(namespace: str, table: str) -> str:
    return f"{catalog()}.{namespace}.{table}"


def silver_table(name: str) -> str:
    return qualified_table(SILVER_NAMESPACE, SILVER_TABLES[name])


def gold_table(name: str) -> str:
    return qualified_table(GOLD_NAMESPACE, GOLD_TABLES[name])


def vacancy_date_col() -> Column:
    return F.coalesce(
        F.col("date_posted"),
        F.to_date(F.col("published_at")),
        F.to_date(F.col("bronze_ingested_at")),
    )


def salary_midpoint_col() -> Column:
    return (
        F.when(
            F.col("salary_from").isNotNull() & F.col("salary_to").isNotNull(),
            (F.col("salary_from") + F.col("salary_to")) / F.lit(2.0),
        )
        .when(F.col("salary_from").isNotNull(), F.col("salary_from"))
        .when(F.col("salary_to").isNotNull(), F.col("salary_to"))
    )


def salary_bucket_col(salary_col: Column) -> Column:
    return (
        F.when(salary_col < 50_000, F.lit("<50k"))
        .when(salary_col < 100_000, F.lit("50k-100k"))
        .when(salary_col < 150_000, F.lit("100k-150k"))
        .when(salary_col < 200_000, F.lit("150k-200k"))
        .when(salary_col < 250_000, F.lit("200k-250k"))
        .when(salary_col < 300_000, F.lit("250k-300k"))
        .when(salary_col < 400_000, F.lit("300k-400k"))
        .otherwise(F.lit("400k+"))
    )


def safe_ratio(numerator: Column, denominator: Column) -> Column:
    return F.when(
        denominator.cast("double") > F.lit(0.0),
        numerator.cast("double") / denominator.cast("double"),
    ).otherwise(F.lit(0.0))


def empty_string_array() -> Column:
    return F.array().cast("array<string>")


def with_gold_vacancy_fields(vacancies_df: DataFrame) -> DataFrame:
    return (
        vacancies_df.withColumn("vacancy_date", vacancy_date_col())
        .withColumn("salary_midpoint", salary_midpoint_col())
        .withColumn(
            "has_salary",
            F.col("salary_from").isNotNull() | F.col("salary_to").isNotNull(),
        )
    )


def build_item_array(
    bridge_df: DataFrame,
    dimension_df: DataFrame,
    key_col: str,
    position_col: str,
    output_col: str,
) -> DataFrame:
    item_name_col = f"{output_col}_item_name"

    return (
        bridge_df.join(
            dimension_df.select(key_col, F.col("name").alias(item_name_col)),
            key_col,
            "left",
        )
        .filter(F.col(item_name_col).isNotNull())
        .groupBy("vacancy_uid")
        .agg(
            F.expr(
                f"""
                transform(
                    array_sort(
                        collect_list(
                            named_struct(
                                'position', {position_col},
                                'name', {item_name_col}
                            )
                        )
                    ),
                    item -> item.name
                )
                """
            ).alias(output_col)
        )
    )


def add_processing_timestamp(df: DataFrame) -> DataFrame:
    return df.withColumn("gold_processed_at", F.current_timestamp())


def aggregate_vacancy_metrics(grouped_df) -> DataFrame:
    return grouped_df.agg(
        F.countDistinct("vacancy_uid").alias("vacancies_count"),
        F.countDistinct("company_key").alias("companies_count"),
        F.sum(F.when(F.col("remote") == F.lit(True), F.lit(1)).otherwise(F.lit(0)))
        .cast("bigint")
        .alias("remote_count"),
        F.count("salary_midpoint").cast("bigint").alias("salary_count"),
        F.avg("salary_midpoint").alias("salary_avg"),
        F.expr("percentile_approx(salary_midpoint, 0.5)").alias("salary_median"),
        F.min("salary_midpoint").alias("salary_min"),
        F.max("salary_midpoint").alias("salary_max"),
        F.min("vacancy_date").alias("first_seen_date"),
        F.max("vacancy_date").alias("last_seen_date"),
    )


def build_vacancies_enriched(
    vacancies_df: DataFrame,
    companies_df: DataFrame,
    skills_df: DataFrame,
    vacancy_skills_df: DataFrame,
    specializations_df: DataFrame,
    vacancy_specializations_df: DataFrame,
    locations_df: DataFrame,
    vacancy_locations_df: DataFrame,
) -> DataFrame:
    vacancies = with_gold_vacancy_fields(vacancies_df).filter(
        F.col("company_key").isNotNull()
    )
    companies = companies_df.select(
        "company_key",
        F.col("name").alias("company_name"),
        F.col("url").alias("company_url"),
        F.col("site").alias("company_site"),
    )

    skill_groups = build_item_array(
        vacancy_skills_df,
        skills_df,
        "skill_key",
        "skill_position",
        "skills",
    )
    specialization_groups = build_item_array(
        vacancy_specializations_df,
        specializations_df,
        "specialization_key",
        "specialization_position",
        "specializations",
    )
    location_groups = build_item_array(
        vacancy_locations_df,
        locations_df,
        "location_key",
        "location_position",
        "locations",
    )

    enriched = (
        vacancies.join(companies, "company_key", "left")
        .join(skill_groups, "vacancy_uid", "left")
        .join(specialization_groups, "vacancy_uid", "left")
        .join(location_groups, "vacancy_uid", "left")
        .withColumn("skills", F.coalesce(F.col("skills"), empty_string_array()))
        .withColumn(
            "specializations",
            F.coalesce(F.col("specializations"), empty_string_array()),
        )
        .withColumn("locations", F.coalesce(F.col("locations"), empty_string_array()))
        .withColumn(
            "search_text",
            F.lower(
                F.concat_ws(
                    " ",
                    F.col("title"),
                    F.col("company_name"),
                    F.col("qualification"),
                    F.col("employment"),
                    F.col("employment_type_text"),
                    F.col("human_city_names"),
                    F.col("short_geo"),
                    F.col("skills"),
                    F.col("specializations"),
                    F.col("locations"),
                    F.col("description_text"),
                    F.col("banner_description"),
                )
            ),
        )
    )

    return add_processing_timestamp(
        enriched.select(
            "vacancy_uid",
            "source",
            "vacancy_id",
            "url",
            "title",
            "company_key",
            "company_name",
            "company_url",
            "company_site",
            "vacancy_date",
            "published_at",
            "valid_through",
            "employment",
            "employment_type_text",
            "remote",
            "job_location_type",
            "qualification",
            "salary_qualification",
            "salary_from",
            "salary_to",
            "salary_midpoint",
            "salary_currency",
            "salary_period",
            "skills",
            "specializations",
            "locations",
            "human_city_names",
            "short_geo",
            "description_text",
            "search_text",
        )
    )


def build_vacancy_overview_daily(vacancies_df: DataFrame) -> DataFrame:
    vacancies = with_gold_vacancy_fields(vacancies_df)

    return add_processing_timestamp(
        aggregate_vacancy_metrics(vacancies.groupBy("vacancy_date", "source"))
        .withColumn("remote_share", safe_ratio(F.col("remote_count"), F.col("vacancies_count")))
        .select(
            "vacancy_date",
            "source",
            "vacancies_count",
            "companies_count",
            "remote_count",
            "remote_share",
            "salary_count",
            "salary_avg",
            "salary_median",
            "salary_min",
            "salary_max",
        )
    )


def build_item_demand(
    vacancies_df: DataFrame,
    bridge_df: DataFrame,
    dimension_df: DataFrame,
    key_col: str,
    name_col: str,
) -> DataFrame:
    vacancies = with_gold_vacancy_fields(vacancies_df)

    source_totals = vacancies.groupBy("source").agg(
        F.countDistinct("vacancy_uid").alias("source_vacancies_count")
    )

    item_facts = vacancies.join(bridge_df, ["vacancy_uid", "source", "vacancy_id"], "inner").join(
        dimension_df.select(
            key_col,
            F.col("name").alias(name_col),
            "normalized_name",
        ),
        key_col,
        "left",
    )

    aggregated = aggregate_vacancy_metrics(
        item_facts.groupBy("source", key_col, name_col, "normalized_name")
    )

    return add_processing_timestamp(
        aggregated.join(source_totals, "source", "left")
        .withColumn(
            "demand_share",
            safe_ratio(F.col("vacancies_count"), F.col("source_vacancies_count")),
        )
        .withColumn("remote_share", safe_ratio(F.col("remote_count"), F.col("vacancies_count")))
        .select(
            "source",
            key_col,
            name_col,
            "normalized_name",
            "vacancies_count",
            "source_vacancies_count",
            "demand_share",
            "companies_count",
            "remote_count",
            "remote_share",
            "salary_count",
            "salary_avg",
            "salary_median",
            "first_seen_date",
            "last_seen_date",
        )
    )


def build_company_demand(
    vacancies_df: DataFrame,
    companies_df: DataFrame,
    vacancy_skills_df: DataFrame,
    vacancy_specializations_df: DataFrame,
    vacancy_locations_df: DataFrame,
) -> DataFrame:
    vacancies = with_gold_vacancy_fields(vacancies_df)
    companies = companies_df.select(
        "company_key",
        F.col("name").alias("company_name"),
        F.col("url").alias("company_url"),
        F.col("site").alias("company_site"),
    )

    base = vacancies.join(companies, "company_key", "left")
    metrics = aggregate_vacancy_metrics(
        base.groupBy(
            "source",
            "company_key",
            "company_name",
            "company_url",
            "company_site",
        )
    )

    skills_count = (
        vacancies.join(vacancy_skills_df, ["vacancy_uid", "source", "vacancy_id"], "left")
        .groupBy("source", "company_key")
        .agg(F.countDistinct("skill_key").cast("bigint").alias("skills_count"))
    )
    specializations_count = (
        vacancies.join(
            vacancy_specializations_df,
            ["vacancy_uid", "source", "vacancy_id"],
            "left",
        )
        .groupBy("source", "company_key")
        .agg(
            F.countDistinct("specialization_key")
            .cast("bigint")
            .alias("specializations_count")
        )
    )
    locations_count = (
        vacancies.join(vacancy_locations_df, ["vacancy_uid", "source", "vacancy_id"], "left")
        .groupBy("source", "company_key")
        .agg(F.countDistinct("location_key").cast("bigint").alias("locations_count"))
    )

    return add_processing_timestamp(
        metrics.join(skills_count, ["source", "company_key"], "left")
        .join(specializations_count, ["source", "company_key"], "left")
        .join(locations_count, ["source", "company_key"], "left")
        .withColumn("remote_share", safe_ratio(F.col("remote_count"), F.col("vacancies_count")))
        .withColumn(
            "skills_count",
            F.coalesce(F.col("skills_count"), F.lit(0).cast("bigint")),
        )
        .withColumn(
            "specializations_count",
            F.coalesce(F.col("specializations_count"), F.lit(0).cast("bigint")),
        )
        .withColumn(
            "locations_count",
            F.coalesce(F.col("locations_count"), F.lit(0).cast("bigint")),
        )
        .select(
            "source",
            "company_key",
            "company_name",
            "company_url",
            "company_site",
            "vacancies_count",
            "remote_count",
            "remote_share",
            "salary_count",
            "salary_avg",
            "salary_median",
            "first_seen_date",
            "last_seen_date",
            "skills_count",
            "specializations_count",
            "locations_count",
        )
    )


def build_salary_distribution(vacancies_df: DataFrame) -> DataFrame:
    vacancies = with_gold_vacancy_fields(vacancies_df).filter(F.col("has_salary"))

    return add_processing_timestamp(
        vacancies.withColumn("salary_bucket", salary_bucket_col(F.col("salary_midpoint")))
        .groupBy(
            "source",
            "salary_currency",
            "salary_period",
            "qualification",
            "employment",
            "remote",
            "salary_bucket",
        )
        .agg(
            F.countDistinct("vacancy_uid").alias("vacancies_count"),
            F.min("salary_midpoint").alias("salary_min"),
            F.max("salary_midpoint").alias("salary_max"),
            F.avg("salary_midpoint").alias("salary_avg"),
            F.expr("percentile_approx(salary_midpoint, 0.5)").alias("salary_median"),
        )
        .select(
            "source",
            "salary_currency",
            "salary_period",
            "qualification",
            "employment",
            "remote",
            "salary_bucket",
            "vacancies_count",
            "salary_min",
            "salary_max",
            "salary_avg",
            "salary_median",
        )
    )


def read_silver_frames(spark: SparkSession) -> dict[str, DataFrame]:
    return {name: spark.table(silver_table(name)) for name in SILVER_TABLES}


def build_gold_frames(silver: dict[str, DataFrame]) -> dict[str, DataFrame]:
    return {
        "vacancies_enriched": build_vacancies_enriched(
            silver["vacancies"],
            silver["companies"],
            silver["skills"],
            silver["vacancy_skills"],
            silver["specializations"],
            silver["vacancy_specializations"],
            silver["locations"],
            silver["vacancy_locations"],
        ),
        "vacancy_overview_daily": build_vacancy_overview_daily(silver["vacancies"]),
        "skill_demand": build_item_demand(
            silver["vacancies"],
            silver["vacancy_skills"],
            silver["skills"],
            "skill_key",
            "skill_name",
        ),
        "specialization_demand": build_item_demand(
            silver["vacancies"],
            silver["vacancy_specializations"],
            silver["specializations"],
            "specialization_key",
            "specialization_name",
        ),
        "location_demand": build_item_demand(
            silver["vacancies"],
            silver["vacancy_locations"],
            silver["locations"],
            "location_key",
            "location_name",
        ),
        "company_demand": build_company_demand(
            silver["vacancies"],
            silver["companies"],
            silver["vacancy_skills"],
            silver["vacancy_specializations"],
            silver["vacancy_locations"],
        ),
        "salary_distribution": build_salary_distribution(silver["vacancies"]),
    }


def ensure_gold_tables(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog()}.{GOLD_NAMESPACE}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {gold_table("vacancies_enriched")} (
            vacancy_uid STRING,
            source STRING,
            vacancy_id STRING,
            url STRING,
            title STRING,
            company_key STRING,
            company_name STRING,
            company_url STRING,
            company_site STRING,
            vacancy_date DATE,
            published_at TIMESTAMP,
            valid_through DATE,
            employment STRING,
            employment_type_text STRING,
            remote BOOLEAN,
            job_location_type STRING,
            qualification STRING,
            salary_qualification STRING,
            salary_from DOUBLE,
            salary_to DOUBLE,
            salary_midpoint DOUBLE,
            salary_currency STRING,
            salary_period STRING,
            skills ARRAY<STRING>,
            specializations ARRAY<STRING>,
            locations ARRAY<STRING>,
            human_city_names STRING,
            short_geo STRING,
            description_text STRING,
            search_text STRING,
            gold_processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(vacancy_date))
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {gold_table("vacancy_overview_daily")} (
            vacancy_date DATE,
            source STRING,
            vacancies_count BIGINT,
            companies_count BIGINT,
            remote_count BIGINT,
            remote_share DOUBLE,
            salary_count BIGINT,
            salary_avg DOUBLE,
            salary_median DOUBLE,
            salary_min DOUBLE,
            salary_max DOUBLE,
            gold_processed_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(vacancy_date))
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {gold_table("skill_demand")} (
            source STRING,
            skill_key STRING,
            skill_name STRING,
            normalized_name STRING,
            vacancies_count BIGINT,
            source_vacancies_count BIGINT,
            demand_share DOUBLE,
            companies_count BIGINT,
            remote_count BIGINT,
            remote_share DOUBLE,
            salary_count BIGINT,
            salary_avg DOUBLE,
            salary_median DOUBLE,
            first_seen_date DATE,
            last_seen_date DATE,
            gold_processed_at TIMESTAMP
        )
        USING iceberg
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {gold_table("specialization_demand")} (
            source STRING,
            specialization_key STRING,
            specialization_name STRING,
            normalized_name STRING,
            vacancies_count BIGINT,
            source_vacancies_count BIGINT,
            demand_share DOUBLE,
            companies_count BIGINT,
            remote_count BIGINT,
            remote_share DOUBLE,
            salary_count BIGINT,
            salary_avg DOUBLE,
            salary_median DOUBLE,
            first_seen_date DATE,
            last_seen_date DATE,
            gold_processed_at TIMESTAMP
        )
        USING iceberg
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {gold_table("location_demand")} (
            source STRING,
            location_key STRING,
            location_name STRING,
            normalized_name STRING,
            vacancies_count BIGINT,
            source_vacancies_count BIGINT,
            demand_share DOUBLE,
            companies_count BIGINT,
            remote_count BIGINT,
            remote_share DOUBLE,
            salary_count BIGINT,
            salary_avg DOUBLE,
            salary_median DOUBLE,
            first_seen_date DATE,
            last_seen_date DATE,
            gold_processed_at TIMESTAMP
        )
        USING iceberg
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {gold_table("company_demand")} (
            source STRING,
            company_key STRING,
            company_name STRING,
            company_url STRING,
            company_site STRING,
            vacancies_count BIGINT,
            remote_count BIGINT,
            remote_share DOUBLE,
            salary_count BIGINT,
            salary_avg DOUBLE,
            salary_median DOUBLE,
            first_seen_date DATE,
            last_seen_date DATE,
            skills_count BIGINT,
            specializations_count BIGINT,
            locations_count BIGINT,
            gold_processed_at TIMESTAMP
        )
        USING iceberg
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {gold_table("salary_distribution")} (
            source STRING,
            salary_currency STRING,
            salary_period STRING,
            qualification STRING,
            employment STRING,
            remote BOOLEAN,
            salary_bucket STRING,
            vacancies_count BIGINT,
            salary_min DOUBLE,
            salary_max DOUBLE,
            salary_avg DOUBLE,
            salary_median DOUBLE,
            gold_processed_at TIMESTAMP
        )
        USING iceberg
        """
    )


def overwrite_gold_table(df: DataFrame, table: str) -> None:
    df.writeTo(table).overwrite(F.lit(True))


spark = create_spark("joblake-silver-to-gold")

try:
    ensure_gold_tables(spark)
    frames = build_gold_frames(read_silver_frames(spark))

    for name in GOLD_TABLES:
        overwrite_gold_table(frames[name], gold_table(name))
        print(f"Overwritten {gold_table(name)}")
finally:
    spark.stop()
