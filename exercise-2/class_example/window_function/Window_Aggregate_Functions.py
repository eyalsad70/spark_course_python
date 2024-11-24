
"""PySpark Window Aggregate Functions
how to calculate sum, min, max for each department using PySpark SQL Aggregate window functions
and WindowSpec.
When working with Aggregate functions, we don’t need to use order by clause.
"""

from pyspark.sql.functions import col,avg,sum,min,max,row_number
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = (("James", "Sales", 3000), ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100), ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000), ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), ("Saif", "Sales", 4100)  )

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)
# df.printSchema()
# df.show(truncate=False)

windowSpec = Window.partitionBy("department").orderBy("salary")
windowSpecAgg  = Window.partitionBy("department")

df.withColumn("row",row_number().over(windowSpec)) \
  .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
  .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
  .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
  .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
  .where(col("row")==1).select("department","avg","sum","min","max") \
  .show()
