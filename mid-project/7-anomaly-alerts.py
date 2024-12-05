from pyspark.sql import SparkSession
#from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, TimestampType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import col, window, count, max

# Define the event schema
json_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("car_id", IntegerType(), False),
    StructField("speed", IntegerType(), False),
    StructField("rpm", IntegerType(), False),
    StructField("gear", IntegerType(), False),
    StructField("driver_id", T.IntegerType(), False),
    StructField("brand_name", StringType(), False),
    StructField("model_name", StringType(), False),
    StructField("color_name", StringType(), False),
    StructField("expected_gear", IntegerType(), False)
])

spark:SparkSession = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('spark-kafka-proj-7') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
    .getOrCreate()

# ReadStream from kafka
stream_df = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", 'course-kafka:19092')\
    .option("subscribe", 'alert-data')\
    .option("startingOffsets", 'earliest')\
    .load()\
    .select(F.col('value').cast(T.StringType()))
    
# change json to dataframe with schema
parsed_df = stream_df.withColumn('parsed_json', F.from_json(F.col('value'), json_schema)) \
                     .select(col('parsed_json.*'))
# Show the inferred schema
parsed_df.printSchema()
#parsed_df.writeStream.format("console").option("truncate", "false").start().awaitTermination()

# Perform aggregation for the last 15 minutes
aggregated_df = parsed_df \
    .withWatermark("event_time", "15 minutes") \
    .groupBy(window(col("event_time"), "15 minutes")) \
    .agg(
        count("*").alias("num_of_rows"),
        F.sum(F.when(F.col("color_name") == "Black", 1).otherwise(0)).alias("num_of_black"),
        F.sum(F.when(F.col("color_name") == "White", 1).otherwise(0)).alias("num_of_white"),
        F.sum(F.when(F.col("color_name") == "Silver", 1).otherwise(0)).alias("num_of_silver"),       
        max(col("speed")).alias("maximum_speed"),
        max(col("gear")).alias("maximum_gear"),
        max(col("rpm")).alias("maximum_rpm")
    )


# Write the aggregated results to the console
query = aggregated_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="1 minute") \
    .start().awaitTermination()

spark.stop()
