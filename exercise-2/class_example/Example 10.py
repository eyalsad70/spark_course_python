'''
Save the table resulted from the previous question as a parquet file
'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Read a csv file
dessert = spark.read.csv(r"dessert.csv",
                         header=True, inferSchema=True)\
  .drop('id')\
  .withColumnRenamed('day.of.week', 'weekday')\
  .withColumnRenamed('num.of.guests', 'num_of_guests')\
  .withColumnRenamed('dessert', 'purchase')\
  .withColumnRenamed('hour', 'shift')

#################################################

dessert.write.parquet('output.parquet', mode='append')

my_backup = spark.read.parquet('output.parquet')

my_backup.show(5)


