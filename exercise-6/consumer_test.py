import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('exercise-6') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0') \
    .getOrCreate()

# Read a small batch of data from Kafka
sample_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:19092") \
    .option("subscribe", "gps-user-review-tmp") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", """{"gps-user-review-tmp":{"0":10}}""") \
    .load() \
    .select(F.col("value").cast("string"))

clean_df = sample_df.select(F.expr("regexp_replace(value, '\\\\\\\\', '')").alias("clean_value"))
clean_df = clean_df.withColumn(
    "clean_value",
    F.expr("regexp_replace(clean_value, '^\"|\"$', '')")  # Remove leading and trailing quotes
)

# Infer the schema from the JSON sample
inferred_schema = spark.read.json(clean_df.rdd.map(lambda x: x["clean_value"])).schema
print(inferred_schema)

clean_df.show(10,truncate=False)
