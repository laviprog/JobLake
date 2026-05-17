from common.spark_session import create_spark


spark = create_spark("joblake-silver-to-gold")
print("TODO: build Gold analytical marts")
spark.stop()
