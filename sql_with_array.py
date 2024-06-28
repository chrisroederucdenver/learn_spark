#!/usr/bin/env python3

# This script sets up a Spark Session, creates and loads a table with data,
# queries it back out into a dataframe, and digs into an array in one of its columns.
# Like create_with_array.py, this script looks into array types. There in Python.
# Here in SQL.
#
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

# Run with rm -rf spark-warehouse/learn_spark_db.db/people_int_array ; spark-warehouse/learn_spark_db.db/people_array ; ./sql_with_array.py   

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

spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")
spark.sql("DROP TABLE if exists learn_spark_db.people_array")
spark.sql("DROP TABLE if exists learn_spark_db.people_int_array")
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


# PLAIN with single strings in numbers column
print("\nPlain ---------")
people_df = spark.sql("SELECT * from people_array ") 

# CONVERT a JSON array string to a Spark Array type
people_df = people_df.withColumn('numbers', F.from_json('numbers', schema=ArrayType(IntegerType()) ))
print(type(people_df.first().numbers))

# WRITE back to DB
people_df.write.saveAsTable('people_int_array')


print("\n\n---------")
people_int_df = spark.sql("SELECT * from people_int_array ") 
people_int_df.show();
print(type(people_int_df.numbers)) 
print(type(people_int_df.first().numbers))
print(people_int_df.first().numbers)

print("\n---------")
sql = """
    SELECT id, name, numbers,
        CASE WHEN (size(array_intersect(numbers, array(22))) > 0 )
             THEN 99
             ELSE 0 
        END as special_status
    FROM people_int_array
"""
print(sql)
special_people_df = spark.sql(sql)
print(special_people_df.count())
special_people_df.show();



spark.stop()
