
"""ntile
ntile() window function returns the relative rank of result rows within a window partition.
In below example we have used 2 as an argument to ntile hence it returns ranking between 2
values (1 and 2)
This is the same as the NTILE function in SQL.
"""
from pyspark.sql.functions import ntile
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
df.show(truncate=False)

windowSpec = Window.partitionBy("department").orderBy("salary")
df.withColumn("ntile",ntile(2).over(windowSpec)) \
    .show()