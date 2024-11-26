from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.master("local[*]").appName('ex3_adding_dates').getOrCreate()

def get_dates_df():
    dummy_df = spark.createDataFrame([Row(dummy='x')])
    
    date_sequence = F.sequence(F.lit("2020-01-01").cast(T.DateType()), F.lit("2020-12-31").cast(T.DateType()))
    exploded_dates = F.explode(date_sequence)
    
    in_dates_df = dummy_df.select(exploded_dates.alias("flight_date"))
    return in_dates_df

matched_df = spark.read.parquet('s3a://spark/data/stg/flight_matched/')

dates_df = get_dates_df()
dates_df_new = dates_df.withColumn("day_of_week", F.dayofweek("flight_date")) \
                       .withColumn("day_of_month", F.dayofmonth("flight_date"))

# Group by day_of_week and day_of_month, then find the maximum (latest) flight_date
max_date_df = dates_df_new.groupBy("day_of_week", "day_of_month") \
    .agg(F.max("flight_date").alias("latest_date"))
print(max_date_df.count())

# Join flights_df with max_date_df on day_of_week and day_of_month
flights_with_latest_date_df = matched_df.join(max_date_df, ["day_of_week", "day_of_month"])

# Show the result (including the flight_date column)
flights_with_latest_date_df.show(20)

# Count rows where latest_date is null
null_count = flights_with_latest_date_df.filter(F.col("latest_date").isNull()).count()

# Print the result
print(f"Number of rows with null latest_date: {null_count}")

flights_with_latest_date_df.write.parquet('s3a://spark/data/transformed/flights/', mode='overwrite')

spark.stop()

