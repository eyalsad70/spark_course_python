""" 
This function build all static data, as follows:
    1. car models 
    2. car colors 
    3. 20 unique cars 
all data is saved in S3a in Parquet format 
"""

from pyspark.sql import SparkSession
# Alias for functions
import pyspark.sql.functions as F
# Alias for types
import pyspark.sql.types as T
import random


spark:SparkSession = SparkSession.builder.master("local[*]").appName('spark-kafka-proj-1').getOrCreate()

num_rows = 20

##############################################################
def create_cars_static_data():
    car_models = spark.read.csv('s3a://spark/data/raw/cars_data/', header=True)
    car_models = car_models.withColumn("model_id", F.col("model_id").cast("int"))
    car_models.printSchema()
    
    car_colors = spark.read.csv('s3a://spark/data/raw/car_colors/', header=True)
    car_colors = car_colors.withColumn("color_id", F.col("color_id").cast("int"))
    car_colors.printSchema()
    
    car_models.write.parquet('s3a://spark/data/source/carmodels/',mode='overwrite')
    car_colors.write.parquet('s3a://spark/data/source/carcolors/',mode='overwrite')


##############################################################
def CarsGeneratorPython():
    # Generate 20 unique car records using python (not pyspark)
    cars = []
    car_ids = set()

    while len(cars) < num_rows:
        # Generate unique car_id
        car_id = random.randint(1000000, 9999999)
        if car_id in car_ids:
            continue
        car_ids.add(car_id)

        # Generate other fields
        driver_id = random.randint(100000000, 999999999)
        model_id = random.randint(1, 7)
        color_id = random.randint(1, 7)
        
        # Create a car record
        car = (car_id, driver_id, model_id, color_id)
        cars.append(car)

    # Define schema
    schema = T.StructType([
        T.StructField("car_id", T.IntegerType(), False),
        T.StructField("driver_id", T.IntegerType(), False),
        T.StructField("model_id", T.IntegerType(), False),
        T.StructField("color_id", T.IntegerType(), False)
    ])

    # Create DataFrame
    car_df = spark.createDataFrame(cars, schema)
    
    return car_df


#################################################################    
def CarsGeneratorPySpark():
    # Create a DataFrame with random car_id values (but this could result in duplicates)
    car_df = spark.range(num_rows).select(
        F.round(F.rand() * 8999999 + 1000000).cast(T.IntegerType()).alias("car_id")
    )

    # Drop duplicates to ensure uniqueness of car_id
    car_df = car_df.dropDuplicates(["car_id"])

    # Add driver_id column using random values
    car_df = car_df.withColumn(
        "driver_id", F.round(F.rand() * 899999999 + 100000000).cast(T.IntegerType())
    )

    # Add model_id column using random values from 1 to 7
    car_df = car_df.withColumn(
        "model_id", F.round(F.rand() * 6 + 1).cast(T.IntegerType())
    )

    # Add color_id column using random values from 1 to 7
    car_df = car_df.withColumn(
        "color_id", F.round(F.rand() * 6 + 1).cast(T.IntegerType())
    )
    
    return car_df



#create_cars_static_data()

#car_df = CarsGeneratorPython()
car_df = CarsGeneratorPySpark()

# Show the resulting DataFrame
car_df.show()

car_df.write.parquet('s3a://spark/data/dims/cars',mode='overwrite')

spark.stop()