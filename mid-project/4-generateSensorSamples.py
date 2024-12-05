""" 
Creating random sensor event for each car (cars data are taken from static file created in previous model)
and send a bulk of 20 events once every second to kafka
"""

import json
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct
from pyspark.sql.functions import lit, col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from time import sleep
from datetime import datetime
import json
import random
import uuid

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
    events = []
    for row in car_ids_df.collect():
        events.append((
        str(uuid.uuid4()),  # Unique event ID
        datetime.now(),  # Current timestamp
        row['car_id'],  # Car ID from the Parquet file
        random.randint(0, 200),  # Random speed
        random.randint(0, 8000),  # Random RPM
        random.randint(1, 7)  # Random gear
        ))
    events_df = spark.createDataFrame(events, schema=event_schema)
    return events_df
######################################

producer = KafkaProducer(bootstrap_servers=brokers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while 1:
    # send car events once per second
    events_df = create_car_events()
    #events_df.show(1)
    # Convert each row to a JSON string
    json_df = events_df.toJSON()
    print(json_df.take(2))
    
    for record in json_df.collect():
        producer.send(topic1, value=record)
    producer.flush()
    sleep(2)
