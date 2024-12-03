from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, StructField, DoubleType
from pyspark.sql import functions as F

# Create a SparkSession
spark:SparkSession = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('exercise-6') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
    .getOrCreate()

# Define the schema for the JSON messages
json_schema = StructType([
    StructField("application_name", StringType(), True),
    StructField("positive_count", IntegerType(), True),
    StructField("neutral_count", IntegerType(), True),
    StructField("negative_count", IntegerType(), True),
    StructField("avg_sentiment_polarity", DoubleType(), True),
    StructField("avg_sentiment_subjectivity", DoubleType(), True),
    StructField("category", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("reviews", DoubleType(), True),
    StructField("size", StringType(), True),
    StructField("num_of_installs", DoubleType(), True),
    StructField("price", DoubleType(), True),
    StructField("age_limit", IntegerType(), True),
    StructField("genres", StringType(), True),
    StructField("version", StringType(), True)
])

# Read the data from the Kafka topic
stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:19092") \
    .option("subscribe", "gps-with-reviews-tmp") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(F.col('value').cast(StringType()))

# Parse the JSON data
parsed_df = stream_df \
    .withColumn("parsed_json", from_json(col("value"), json_schema)) \
    .select("parsed_json.*")
#parsed_df.writeStream.format("console").option("truncate", "false").start().awaitTermination()
# Show the inferred schema
parsed_df.printSchema()

# Example of additional processing (if needed)
# For example, you can add sentiment polarity-based grouping or filtering here:
# processed_df = parsed_df.filter(col("sentiment_rank").isNotNull())
def process_batch(batch_df, batch_id):
    # Print the batch id and the count of records in the batch to the console
    print(f"Processing batch {batch_id} with {batch_df.count()} records")
    
    # Optional: You can show a small sample of the batch (e.g., first 5 rows) if you want
    batch_df.show(2)
    
    # Write the batch data to Parquet (you can change the mode based on your requirements)
    batch_df.write \
        .mode("append") \
        .parquet("s3a://spark/data/target/google_reviews_calc")
        
    
# Write the processed data to S3 in Parquet format
query = parsed_df \
    .writeStream \
    .trigger(processingTime="1 minute") \
    .option("checkpointLocation", "s3a://spark/checkpoints/ex6_1/") \
    .foreachBatch(process_batch) \
    .start()

    
# Wait for the termination of the query
query.awaitTermination()

spark.stop()
