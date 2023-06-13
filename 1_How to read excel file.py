#pandas

import pandas as pd

# Read CSV file
df = pd.read_csv('file.csv')

# Display the dataframe
print(df.head())




#Pyspark
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("CSV Reader") \
    .getOrCreate()

# Read CSV file
df = spark.read.csv('file.csv', header=True, inferSchema=True)

# Display the dataframe
df.show()








#SQL

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("CSV Reader") \
    .getOrCreate()

# Read CSV file
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("file.csv")

# Register dataframe as a temporary table
df.createOrReplaceTempView("my_table")

# Query the table using SQL
result = spark.sql("SELECT * FROM my_table")

# Display the result
result.show()
