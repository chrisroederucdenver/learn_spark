#!/usr/bin/env python3

# This script sets up a Spark Session, creates and loads a table with data,
# queries it back out into a dataframe, and digs into an array in one of its columns.
# It reads  people_array.csv which has a column of strings that look like JSON arrays.
#   a, b, "[1,2,3]"
# I think this would be adaptable to quoted a CSV sublist:
#   a, b, "1,2,3"
#
# The motivation for this is multi-valued attributes in claims data. One solution in
# past has been to create as many copies of the row as such an attribute has value.
# You have to do this once for each column, and it can cloud the identity of each row
# because for whatever an entity was in the original, now you have many rows for those.
# Now each row is some kind of sub-entity, and sends you down the road of needing to
# know what the natural keys are.

# Run with rm -rf spark-warehouse/learn_spark_db.db/people_array ; ./create_with_array.py   

# Notes about CSV here:
# - no space after comma before a string
# - empty quotes turns to null
# - comma with no  entry becomes the empty string
# - no comma delimiting the string (e.g. when it's the last one) is null
 

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
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
people_df = people_df.withColumn('numbers', F.from_json('numbers', schema=ArrayType(IntegerType()) ))
people_df.show();



# show more concretely that we do indeed have an array at that spot in the df
# the process involves types, here still dataframe
print("DF ---")
print(type(people_df))
print(people_df)
people_df.show()

# still a dataframe, but just the numbers column
print("\nnarrow DF ---")
numbers_df=people_df.select('numbers')
print(type(numbers_df))
print(numbers_df)
numbers_df.show()

# Row
print("\nROW ---")
numbers_row = numbers_df.first()
print(type(numbers_row))
print(numbers_row)

print("\nLIST by name ---")
numbers_cell = numbers_row.numbers
print(type(numbers_cell))
print(numbers_cell)

print("\nLIST by number ---")
numbers_cell = numbers_row[0]
print(type(numbers_cell))
print(numbers_cell)

print("\nfirst number ---")
numbers_first = numbers_cell[0]
print(type(numbers_first))
print(numbers_first)

###################################
# all together, the list/array
###################################
print(" short versions to the array  ---- ")
print("The select returns a dataframe, you still need the single attribute name at the end:", end='')
print(people_df.select('numbers').first())
print("Skip the select and just name the single attribute.")
print(people_df.first().numbers)

print("Short versions to particular members of the array  ---- ")
print(people_df.select('numbers').first().numbers[1])
print(people_df.first().numbers[1])




numbers_list = people_df.first().numbers
if 22 in numbers_list:
    print("yes we can do that REVCNTRCD thing")
else:
    print("keep looking")






spark.stop()
