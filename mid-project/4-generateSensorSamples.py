""" 
Creating random sensor event for each car (cars data are taken from static file created in previous model)
and send a bulk of 20 events once every second to kafka
"""

import json
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from time import sleep
from datetime import datetime
import json

# Topics/Brokers
topic1 = 'sensors-sample'
brokers = ['course-kafka:19092']

# Create a SparkSession, setting up configurations as needed.
spark:SparkSession = SparkSession.builder.master("local[*]").appName('spark-kafka-proj-4').getOrCreate()

# Define the event schema
event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("car_id", IntegerType(), False),
    StructField("speed", IntegerType(), False),
    StructField("rpm", IntegerType(), False),
    StructField("gear", IntegerType(), False)
])

cars_df = spark.read.parquet('s3a://spark/data/dims/cars')
car_ids_df = cars_df.select("car_id")

#######################################
def create_car_events():
    # Generate event data
    events_df = car_ids_df \
        .withColumn("event_id", F.expr("uuid()")) \
        .withColumn("event_time", F.current_timestamp()) \
        .withColumn("speed", F.floor(F.rand() * 201)) \
        .withColumn("rpm", F.floor(F.rand() * 8001)) \
        .withColumn("gear", F.floor(F.rand() * 7 + 1))

    # Rearrange columns to make car_id the 3rd column (as requested in exercise sheet)
    events_df = events_df.select(
            "event_id",       # 1st column
            "event_time",     # 2nd column
            "car_id",         # 3rd column
            "speed",          # 4th column
            "rpm",            # 5th column
            "gear"            # 6th column
    )
    # Show the generated events
    #events_df.show(truncate=False)
    return events_df
######################################

producer = KafkaProducer(bootstrap_servers=brokers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while 1:
    # send car events once per second
    events_df = create_car_events()
    #events_df.show(2)
    # Convert each row to a JSON string
    json_df = events_df.toJSON()
    print(json_df.take(2))
    
    for record in json_df.collect():
        producer.send(topic1, value=record)
    producer.flush()
    sleep(1)
