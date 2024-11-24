
from pyspark import SparkContext
from pyspark.sql import SparkSession

text = """This is example of spark application in Python
Python is very common development language and it also one of Spark supported
languages
The library of Spark in Python called PySpark
In this example you will implements word count application using PySpark
Good luck!!""".split("\n")

#sc = SparkContext.getOrCreate()
sc = SparkSession.builder.master('local[*]').appName('exercise1_app').getOrCreate()

lines_rdd  = sc.sparkContext.parallelize(text)
print(lines_rdd.collect())

# Split each line into individual words and create key-value pairs
words_rdd = lines_rdd.flatMap(lambda line: line.split()).map(lambda word: (word, 1))

word_counts_rdd = words_rdd.reduceByKey(lambda a, b: a + b)               # Aggregate counts by key (word)
    
# Display the RDD content
print(word_counts_rdd.collect())

sc.stop()

