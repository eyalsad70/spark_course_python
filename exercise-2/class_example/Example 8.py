'''
Capitalize the names of the shifts (e.g. noon  →  Noon)
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,StringType,initcap
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

def capitalize(s):
  return s[0].upper() + s[1:]

capitalize_udf = udf(f=capitalize, returnType=StringType())


dessert = dessert\
    .withColumn('capital_shift', capitalize_udf('shift'))\
    .drop('shift')\
    .withColumnRenamed('capital_shift', 'shift')
dessert.show(5)

#
# dessert2 = dessert\
#     .withColumn('shift', initcap('shift'))\
#
# dessert2.show(5)