from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql import types as T

# Create a SparkSession, setting up configurations as needed.
spark:SparkSession = SparkSession.builder.master("local[*]").appName('ex5_google_apps').getOrCreate()

#Read the Google Play Store data CSV into a DataFrame .
google_reviews_df = spark.read.csv('s3a://spark/data/raw/google_reviews/', header=True)
google_reviews_df.printSchema()

sentiment_arr = [Row(Sentiment="Positive", sentiment_rank=1),
                Row(Sentiment="Neutral", sentiment_rank=0),
                Row(Sentiment="Negative", sentiment_rank=-1)]

sentiment_df = spark.createDataFrame(sentiment_arr)
#sentiment_df.show()

# Join the age_limit_df with the main DataFrame based on the 'Content Rating' column .
joined_df = google_reviews_df.join(F.broadcast(sentiment_df), ['Sentiment'])
joined_df.show(10)

selected_df = joined_df \
    .select(F.col('App').alias('application_name'),
            F.col('Translated_Review').alias('translated_review') ,
            F.col('sentiment_rank'),
            F.col('Sentiment_Polarity').cast(T.FloatType()).alias('sentiment_polarity') ,
            F.col('Sentiment_Subjectivity').cast(T.FloatType()).alias('sentiment_subjectivity'))

# Display the transformed data and its schema.
selected_df.show(10)
selected_df.printSchema()

# Save the processed data into a Parquet file.
selected_df.write.parquet('s3a://spark/data/source/google_reviews', mode='overwrite')

spark.stop()
