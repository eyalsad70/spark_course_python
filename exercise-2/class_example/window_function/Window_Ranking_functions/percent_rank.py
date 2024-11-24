
"""rank
rank() window function is used to provide a rank to the result within a window partition.
This function leaves gaps in rank when there are ties.
This is the same as the PERCENT_RANK function in SQL.
"""
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = (("James", "Sales", 3000), ("Michael", "Sales", 4600),\
              ("James", "Sales", 3000),("Scott", "Finance", 3300), \
              ("Jen", "Finance", 3900), ("Jeff", "Marketing", 3000),\
              ("Kumar", "Marketing", 2000), ("Saif", "Sales", 4100))


columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)
df.printSchema()
df.show(truncate=False)

windowSpec = Window.partitionBy("department").orderBy("salary")
df.withColumn("percent_rank",percent_rank().over(windowSpec)) \
    .show()