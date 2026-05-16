from __future__ import annotations

from common.spark_session import create_spark


def main() -> None:
    spark = create_spark("joblake-silver-to-gold")
    print("TODO: build Gold analytical marts")
    spark.stop()


if __name__ == "__main__":
    main()
