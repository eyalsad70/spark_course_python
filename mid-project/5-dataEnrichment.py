"""
Data enrichment using Spark Streaming Structure
consume events from kafka topic 'sensors-sample' (produced by 2-generateSensorSamples.py), 
enrich the data with the static data built on step-1, 
and send it to new kafka topic 'samples-enriched'
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
    StructField("gear", IntegerType(), False)
])

spark:SparkSession = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('spark-kafka-proj-5') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
    .getOrCreate()

# ReadStream from kafka
stream_df = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", 'course-kafka:19092')\
    .option("subscribe", 'sensors-sample')\
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

# fetch enrichment tables
models_df = spark.read.parquet('s3a://spark/data/source/carmodels/')
colors_df = spark.read.parquet('s3a://spark/data/source/carcolors/')
cars_df = spark.read.parquet('s3a://spark/data/dims/cars')

joined_cars_df = parsed_df.join(F.broadcast(cars_df), 'car_id')
#joined_cars_df.cache()

joined_df = joined_cars_df.join(F.broadcast(models_df), "model_id") \
                           .join(F.broadcast(colors_df), "color_id")
        
selected_df = joined_df \
    .select(F.col('event_id'),
            F.col('event_time') ,
            F.col('car_id') ,
            F.col('speed') ,
            F.col('rpm') ,
            F.col('gear'),
            F.col('driver_id') ,
            F.col('car_brand').alias("brand_name"),
            F.col('car_model').alias('model_name') ,
            F.col('color_name') ) 
selected_df = selected_df.withColumn("expected_gear", F.round(F.col('speed')/30).cast("int"))
#selected_df.writeStream.format("console").option("truncate", "false").start().awaitTermination()
selected_df.printSchema()

json_df = selected_df.select(F.to_json(F.struct("*")).alias("value"))

query = json_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:19092") \
    .option("topic", "samples-enriched") \
    .option('checkpointLocation', 's3a://spark/checkpoints/midProj/review_calculation-new') \
    .start().awaitTermination()

spark.stop()
