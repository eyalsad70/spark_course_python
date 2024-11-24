from pyspark.sql import SparkSession
# Alias for functions
import pyspark.sql.functions as F
# Alias for types
import pyspark.sql.types as T


spark = SparkSession.builder.master("local[*]").appName('ex2_flights').getOrCreate()

# Initialize SparkSession with MinIO configurations
# spark = SparkSession.builder.master("local[*]") \
#     .appName("ex2_flights") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9001") \
#     .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .getOrCreate()
    
flights_raw_df = spark.read.csv('s3a://spark/data/raw/flights_raw/', header=True)

# Convert DayofMonth to IntegerType and rename to day_of_month
result_df = flights_raw_df.select( F.col("DayofMonth").cast(T.IntegerType()).alias("day_of_month"),
                                   F.col("DayofWeek").cast(T.IntegerType()).alias("day_of_week"),
                                   F.col("Carrier").alias("carrier"),
                                   F.col("OriginAirportID").cast(T.IntegerType()).alias("origin_airport_id"),
                                   F.col("DestAirportID").cast(T.IntegerType()).alias("dest_airport_id"),
                                   F.col("DepDelay").cast(T.IntegerType()).alias("dep_delay"),
                                   F.col("ArrDelay").cast(T.IntegerType()).alias("arr_delay")
                                )

result_df.printSchema()
result_df.show(20)

result_df.write.parquet('s3a://spark/data/source/flights_raw/',mode='overwrite')

spark.stop()