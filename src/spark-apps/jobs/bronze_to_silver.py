from __future__ import annotations

from common.spark_session import create_spark


def main() -> None:
    spark = create_spark("joblake-bronze-to-silver")
    print("TODO: deduplicate, clean, normalize, and write Silver Iceberg tables")
    spark.stop()


if __name__ == "__main__":
    main()
