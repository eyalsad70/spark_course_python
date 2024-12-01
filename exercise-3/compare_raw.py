from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session with additional configurations
spark = SparkSession.builder.master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .appName('ex3_clean_flights') \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "10")  # Default is 200

# Read parquet data into Spark DataFrames
flights_df = spark.read.parquet('s3a://spark/data/source/flights/')
flights_raw_df = spark.read.parquet('s3a://spark/data/source/flights_raw/')

# Remove duplicate records from both DataFrames
flights_distinct_df = flights_df.dropDuplicates()
flights_raw_distinct_df = flights_raw_df.dropDuplicates()

# Identify matched records in both DataFrames
matched_df = flights_distinct_df.intersect(flights_raw_df)

# Identify unmatched records and add a new column to annotate the source DataFrame
unmatched_flights = flights_distinct_df.subtract(matched_df).withColumn('source_of_data', F.lit('flights'))
unmatched_flights_raw = flights_raw_distinct_df.subtract(matched_df).withColumn('source_of_data', F.lit('flights_raw'))

# Union the unmatched records from both DataFrames
unmatched_df = unmatched_flights.union(unmatched_flights_raw)

print(unmatched_df.count())
unmatched_df.show(50)

# Write matched and unmatched DataFrames to S3 as Parquet files
matched_df.write.parquet('s3a://spark/data/stg/flight_matched/', mode='overwrite')
unmatched_df.write.parquet('s3a://spark/data/stg/flight_unmatched/', mode='overwrite')

# Stop the Spark session to release resources
spark.stop()
