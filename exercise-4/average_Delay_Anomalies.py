from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

spark:SparkSession = SparkSession.builder.master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .appName('ex4_anomalies_detection') \
    .getOrCreate()

all_history_window = Window.partitionBy(F.col("carrier")).orderBy(F.col("latest_date"))

flights_df = spark.read.parquet('s3a://spark/data/transformed/flights/')
#flights_df = flights_df.filter(F.col("latest_date").isNotNull())

#flights_df.show(10)
#print(flights_df.count())
#flights_df.cache()

# Define the window
#all_history_window = Window.partitionBy(F.col("carrier")).orderBy(F.col("latest_date")).rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Calculate historical average delay up to the current flight's date
avg_delay_df = flights_df.withColumn("avg_till_now", F.avg("arr_delay").over(all_history_window))

avg_delay_df.show(100)

# Calculate the percentage difference
deviation_df = avg_delay_df \
    .withColumn('avg_diff_percent', F.abs(F.col('arr_delay') / F.col('avg_till_now')))
# flights_with_diff = flights_with_avg.withColumn(
#     "avg_diff_percent",
#     F.abs(F.col("arr_delay") - F.col("avg_till_now")) / F.col("avg_till_now") * 100
# )

# Filter flights with avg_diff_percent greater than 300%
high_diff_flights = deviation_df.filter(F.col("avg_diff_percent") > 3.0)

# Show the filtered result
high_diff_flights.show(30)
print(high_diff_flights.count())

spark.stop()
