from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
import pyspark.sql.types as T

spark = SparkSession.builder.master("local[*]").appName('ex3_aggregations').getOrCreate()

flights_df = spark.read.parquet('s3a://spark/data/transformed/flights/')
airports_df = spark.read.parquet('s3a://spark/data/source/airports/')

flights_df.printSchema()
airports_df.printSchema()

######################  DEPARTERS  ######################

# Group by 'origin_airport_id' and count the number of departures
departures_by_airport = flights_df.groupBy("origin_airport_id").agg(F.count("*").alias("num_departures"))
# Rename 'origin_airport_id' to 'airport_id'
departures_by_airport = departures_by_airport.withColumnRenamed("origin_airport_id", "airport_id")

# Perform the join between aggregated departures and airports DataFrame
joined_departures_df = departures_by_airport.join(
    airports_df,
    on="airport_id",
    how="inner"  # Only include matching records
).orderBy(F.desc("num_departures"))

# joined_departures_df.show(10)


###############  ARRIVALS   ####################

# Group by 'origin_airport_id' and count the number of departures
arrivals_by_airport = flights_df.groupBy("dest_airport_id").agg(F.count("*").alias("num_departures"))
# Rename 'origin_airport_id' to 'airport_id'
arrivals_by_airport = arrivals_by_airport.withColumnRenamed("dest_airport_id", "airport_id")

# Perform the join between aggregated departures and airports DataFrame
joined_arrivals_df = arrivals_by_airport.join(
    airports_df,
    on="airport_id",
    how="inner"  # Only include matching records
).orderBy(F.desc("num_departures"))

joined_arrivals_df.show(10)


###############  ROUTES   ####################

# Group by origin_airport_id and dest_airport_id to represent unique flight routes
flight_routes_df = flights_df.groupBy("origin_airport_id", "dest_airport_id") \
    .agg(F.count("*").alias("num_flights")) \
    .withColumnRenamed("origin_airport_id", "source_airport_id") \
    .withColumnRenamed("dest_airport_id", "dest_airport_id")
    
# Show the result
flight_routes_df.show()

# Join with airports dataset to get the source airport name
flight_routes_with_source = flight_routes_df.join(
    airports_df,
    flight_routes_df["source_airport_id"] == airports_df["airport_id"],
    "left"
).select(
    flight_routes_df["source_airport_id"],
    flight_routes_df["dest_airport_id"],
    F.col("name").alias("source_airport_name"),
    F.col("city").alias("source_airport_city"),
    F.col("num_flights")
)

# Join the result with airports dataset to get the destination airport name
flight_routes_with_names = flight_routes_with_source.join(
    airports_df,
    flight_routes_with_source["dest_airport_id"] == airports_df["airport_id"],
    "left"
).select(
    F.col("source_airport_id"),
    F.col("source_airport_name"),
    F.col("source_airport_city"),
    F.col("dest_airport_id"),
    F.col("name").alias("dest_airport_name"),
    F.col("city").alias("dest_airport_city"),
    F.col("num_flights")
)

# Show the result
flight_routes_with_names.show()


spark.stop()


