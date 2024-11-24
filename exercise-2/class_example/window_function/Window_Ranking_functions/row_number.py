"""
1.row_number Window Function

row_number() window function is used to give the sequential row number
starting from 1 to the result of each window partition.
"""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = (("James", "Sales", 3000), ("Michael", "Sales", 4600),\
              ("James", "Sales", 3000),("Scott", "Finance", 3300), \
              ("Jen", "Finance", 3900), ("Jeff", "Marketing", 3000),\
              ("Kumar", "Marketing", 2000), ("Saif", "Sales", 4100))

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)

df.printSchema()
#show the full content of the column >> truncate=False
df.show(truncate=False)



windowSpec  = Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number",row_number().over(windowSpec)) \
    .show(truncate=False)

