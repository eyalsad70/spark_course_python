# SparkSession
from pyspark.sql import SparkSession
# Alias for functions
import pyspark.sql.functions as F
# Alias for types
import pyspark.sql.types as T

spark=SparkSession.builder.master("local").appName('ex2_airports').getOrCreate()

airports_raw_df = spark.read.csv('s3a://spark/data/raw/airports/', header=True)

airports_raw_df.printSchema()

# # Define schema
# schema = T.StructType([
#     T.StructField("airport_id", T.StringType(), True),  # Assume airport_id is initially a string
#     T.StructField("city", T.StringType(), True),
#     T.StructField("state", T.StringType(), True),
#     T.StructField("name", T.StringType(), True)
# ])

# Change airport_id to StringType and select city and name fields
result_df = airports_raw_df.withColumn("airport_id", F.col("airport_id").cast(T.IntegerType())) \
              .select("airport_id", "city", "state", "name")

result_df.printSchema()
result_df.show(10)

result_df.write.parquet('s3a://spark/data/source/airports/',mode='overwrite')

spark.stop()


