
"""
dense_rank Window Function

dense_rank() window function is used to get the result with rank of rows
within a window partition without any gaps.
This is similar to rank() function difference being rank function
leaves gaps in rank when there are ties.
"""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = (("James", "Sales", 3000), ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100), ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000), ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), ("Saif", "Sales", 4100)  )

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)
df.printSchema()
df.show(truncate=False)

windowSpec = Window.partitionBy("department").orderBy("salary")
df.withColumn("dense_rank",dense_rank().over(windowSpec)) \
    .show()