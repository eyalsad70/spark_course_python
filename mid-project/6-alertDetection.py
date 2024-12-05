"""
read enriched events from kafka
filter anomalities, matching all the following criterias:
    * Speed is greater than 120
    * Expected gear is not equals to actual gear
    * RPM is greater than 6000
send them to alerts kafka topic
"""

from pyspark.sql import SparkSession
#from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, TimestampType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql import types as T

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
    .appName('spark-kafka-proj-6') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
    .getOrCreate()

# ReadStream from kafka
stream_df = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", 'course-kafka:19092')\
    .option("subscribe", 'samples-enriched')\
    .option("startingOffsets", 'earliest')\
    .load()\
    .select(F.col('value').cast(T.StringType()))
    
# change json to dataframe with schema
parsed_df = stream_df.withColumn('parsed_json', F.from_json(F.col('value'), json_schema)) \
                     .select(F.col('parsed_json.*'))
# Show the inferred schema
parsed_df.printSchema()
#parsed_df.writeStream.format("console").option("truncate", "false").start().awaitTermination()

# Filter rows based on the conditions
filtered_df = parsed_df.filter(
    (F.col("speed") > 120) &  # Speed is greater than 120
    (F.col("expected_gear") != F.col("gear")) &  # Expected gear is not equal to actual gear
    (F.col("rpm") > 6000)  # RPM is greater than 6000
)
#filtered_df.writeStream.format("console").option("truncate", "false").start().awaitTermination()

json_df = filtered_df.select(F.to_json(F.struct("*")).alias("value"))

query = json_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:19092") \
    .option("topic", "alert-data") \
    .option('checkpointLocation', 's3a://spark/checkpoints/midProj/review_calculation-alert') \
    .start().awaitTermination()

spark.stop()
