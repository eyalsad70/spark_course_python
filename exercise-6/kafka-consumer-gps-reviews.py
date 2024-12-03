import os
from pyspark.sql import SparkSession
#from pyspark.sql.functions import *
#from pyspark.sql.types import StructField, StringType, StructType, IntegerType, FloatType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Topics/Brokers
#topics = 'gps-user-review-source'

def show_debug(batch_df, batch_id):
    print(f"--- Showing results for Batch {batch_id} ---")
    batch_df.show(5,truncate=False)

# Received message:  {"application_name":"1800 Contacts - Lens Store","translated_review":"Very easy use.","sentiment_rank":1,"sentiment_polarity":0.56333333,"sentiment_subjectivity":1.0}
json_schema = T.StructType() \
    .add("application_name", T.StringType()) \
    .add("translated_review", T.StringType()) \
    .add("sentiment_rank", T.IntegerType()) \
    .add("sentiment_polarity", T.FloatType()) \
    .add("sentiment_subjectivity", T.FloatType())


spark:SparkSession = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('exercise-6') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
    .getOrCreate()


# ReadStream from kafka
stream_df = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", 'course-kafka:19092')\
    .option("subscribe", 'gps-user-review-source')\
    .option("startingOffsets", 'earliest')\
    .load()\
    .select(F.col('value').cast(T.StringType()))

# clean escape chars '\'
stream_df = stream_df.withColumn('value', F.expr("regexp_replace(value, '\\\\\\\\\', '')"))
# kafka producer got json as string, so need to remove double quotes
stream_df = stream_df.withColumn('value', F.expr("regexp_replace(value, '^\"|\"$', '')"))  # Remove leading and trailing quotes
#stream_df.select(F.col("value").cast(T.StringType())).writeStream.format("console").option("truncate", "false").start().awaitTermination()

# change json to dataframe with schema
parsed_df = stream_df.withColumn('parsed_json', F.from_json(F.col('value'), json_schema)) \
                     .select(F.col('parsed_json.*'))
#parsed_df.writeStream.format("console").option("truncate", "false").start().awaitTermination()

# Show the inferred schema
parsed_df.printSchema()

# fetch static data from s3    
static_data_df = spark.read.parquet('s3a://spark/data/source/google_apps/')
static_data_df.cache()

# Group by application_name and calculate the sentiment counts
aggregated_df = parsed_df.groupBy("application_name") \
    .agg(
        F.sum(F.when(F.col("sentiment_rank") == 1, 1).otherwise(0)).alias("positive_count"),
        F.sum(F.when(F.col("sentiment_rank") == 0, 1).otherwise(0)).alias("neutral_count"),
        F.sum(F.when(F.col("sentiment_rank") == -1, 1).otherwise(0)).alias("negative_count"),
        # Average sentiment polarity
        F.avg(F.col("sentiment_polarity")).alias("avg_sentiment_polarity"),        
        # Average sentiment subjectivity
        F.avg(F.col("sentiment_subjectivity")).alias("avg_sentiment_subjectivity")
    )

joined_df = aggregated_df.join(static_data_df, "application_name")

# Output the results to the console
#query = joined_df.writeStream.format("console").outputMode("update").start().awaitTermination()
    
# Convert the joined dataframe to JSON
json_df = joined_df.select(F.to_json(F.struct("*")).alias("value"))
#json_df.select(F.col("value").cast(T.StringType())).writeStream.format("console").option("truncate", "false").outputMode("update").start().awaitTermination()

# Write the JSON results to Kafka
query = json_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:19092") \
    .option("topic", "gps-with-reviews-tmp") \
    .option('checkpointLocation', 's3a://spark/checkpoints/ex6/review_calculation') \
    .outputMode("update") \
    .start().awaitTermination()

static_data_df.unpersist()
spark.stop()
