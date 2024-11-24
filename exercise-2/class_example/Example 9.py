'''
Find the tables with the 5 highest percent of dessert buyers
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import pyspark.sql.functions as fn

spark = SparkSession.builder.getOrCreate()

# Read a csv file
dessert = spark.read.csv(r"dessert.csv",
                         header=True, inferSchema=True)\
  .drop('id')\
  .withColumnRenamed('day.of.week', 'weekday')\
  .withColumnRenamed('num.of.guests', 'num_of_guests')\
  .withColumnRenamed('dessert', 'purchase')\
  .withColumnRenamed('hour', 'shift')

dessert = dessert.withColumn('no_purchase', ~dessert.purchase)

#################################################

get_ratio_udf = udf(lambda row: row[0] / (row[0]+row[1]))


res = dessert\
    .groupby('table')\
    .agg(fn.sum(fn.col('purchase').cast(IntegerType())).alias('buyers'),
         fn.sum(fn.col('no_purchase').cast(IntegerType())).alias('non_buyers'))\
    .withColumn('ratio', get_ratio_udf(fn.struct('buyers', 'non_buyers')))\
    .orderBy('ratio', ascending=False)
res.show(5)