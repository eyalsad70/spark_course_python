'''
How many groups purchased a dessert on Mondays?
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

col = (dessert.weekday == 'Monday') & (dessert.purchase)
print(dessert.where(col).count())

#option2
# print(dessert\
#       .where((dessert.weekday == 'Monday') & (dessert.purchase))\
#       .show(3))
