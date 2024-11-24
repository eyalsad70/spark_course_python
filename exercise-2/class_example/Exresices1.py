# DataFrames fundamentals
'''
Read the file "people" into a Dataframe and answer the following questions:

5. Create a new Dataframe with the data of the males only and call it males.
6. How many males are in the table?
7. What is the mean height and weight of the males?
8. What is the height of the tallest female who is older than 40?
9. Create a new Dataframe with two columns for the age and the average weight of the people in this age.
'''

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
