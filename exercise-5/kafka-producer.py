import json
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct


from time import sleep

# Topics/Brokers
topic1 = 'gps-user-review-source'
brokers = ['course-kafka:9092']

# Create a SparkSession, setting up configurations as needed.
spark:SparkSession = SparkSession.builder.master("local[*]").appName('ParquetToKafka').getOrCreate()

df = spark.read.parquet("s3a://spark/data/source/google_reviews")
print(df.head())
df.show(6)

# Convert each row to a JSON string
json_df = df.toJSON()
print(json_df.take(6))

producer = KafkaProducer(bootstrap_servers=brokers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Iterate through each JSON record and send it to Kafka
index = 0
for record in json_df.collect():
    producer.send(topic1, value=record)
    index += 1
    if not index % 50:
        producer.flush()
        sleep(5)

producer.close()
spark.stop()
