from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

spark:SparkSession = SparkSession.builder.master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .appName('ex4_anomalies_detection') \
    .getOrCreate()

all_history_window = Window.partitionBy(F.col("carrier")).orderBy(F.col("latest_date")).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

flights_df = spark.read.parquet('s3a://spark/data/transformed/flights/')
#flights_df = flights_df.filter(F.col("latest_date").isNotNull())
flights_df.cache()

# Calculate historical average delay up to the current flight's date
avg_delay_df = flights_df.withColumn("avg_all_time", F.avg("arr_delay").over(all_history_window))

# Calculate the percentage difference
deviation_df = avg_delay_df \
    .withColumn('avg_diff_percent', F.abs(F.col('arr_delay') / F.col('avg_all_time')))

# Filter flights with avg_diff_percent greater than 300%
high_diff_flights = deviation_df.filter(F.col("avg_diff_percent") > 5.0)

high_diff_flights.show(30)
# print(avg_delay_df.count())

flights_df.unpersist()
spark.stop()
