#!/usr/bin/env python3

#### rm -rf  spark-warehouse/learn_spark_db.db/people_array

# Notes about CSV here:
# - no space after comma before a string
# - empty quotes turns to null
# - comma with no  entry becomes the empty string
# - no comma delimiting the string (e.g. when it's the last one) is null
 
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .master("local") \
    .appName('learn_spark') \
    .getOrCreate()
    #.config("spark.some.config.option", "some-value") \

spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")
spark.sql("DROP TABLE if exists learn_spark_db.people_array")
csv_file = "people_array.csv"
schema=" id INT, name STRING, age INT, sex STRING, race STRING, numbers  STRING "
people_df = spark.read.csv(csv_file, schema=schema, quote="\"", sep=",", header=True)
people_df.write.mode("overwrite").saveAsTable("people_array")

print("Databases:")
print(spark.catalog.listDatabases())
print("Tables:")
print(spark.catalog.listTables("learn_spark_db"))
print("Columns:")
print(spark.catalog.listColumns("people_array"))


# plain
people_df = spark.sql("SELECT * from people_array ") 
print(people_df.count())
people_df.show();


# CONVERT a JSON array string to a Spark Array type
people_df = people_df.withColumn('numbers', F.from_json('numbers', ArrayType(StringType())))
people_df.show();


# *** ERROR *** this comes up as an array of string, not

# show more concretely that we do indeed have an array at that spot in the df
# the process involves types, here still dataframe
print("DF ---")
print(type(people_df))
print(people_df)
people_df.show()

# still a dataframe
print("narrow DF ---")
numbers_df=people_df.select('numbers')
print(type(numbers_df))
print(numbers_df)
numbers_df.show()

# Row
print("ROW ---")
numbers_row = numbers_df.first()
print(type(numbers_row))
print(numbers_row)
###numbers_row.show()

print("LIST ---")
numbers_cell = numbers_row.numbers
print(type(numbers_cell))
print(numbers_cell)

print("first number ---")
numbers_first = numbers_row[0]
print(type(numbers_first))
print(numbers_first)

print("first number ---, really")
numbers_first = numbers_row[0][0]
print(type(numbers_first))
print(numbers_first)


# all together, the list/array
print(people_df.select('numbers').first().numbers)
print(people_df.first().numbers)

print(people_df.select('numbers').first().numbers[0])
print(people_df.first().numbers[0])





spark.stop()
