#!/usr/bin/env python3

#### rm -rf  spark-warehouse/learn_spark_db.db/people
 
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName('learn_spark') \
    .getOrCreate()
    #.config("spark.some.config.option", "some-value") \

spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")
#spark.sql("DROP TABLE if exists learn_spark_db.people")
csv_file = "people.csv"
schema=" id INT, name STRING, age INT, sex STRING, race STRING"
people_df = spark.read.csv(csv_file, schema=schema)
people_df.write.mode("overwrite").saveAsTable("people")

print("Databases:")
print(spark.catalog.listDatabases())
print("Tables:")
print(spark.catalog.listTables("learn_spark_db"))
print("Columns:")
print(spark.catalog.listColumns("people"))


# plain
people_df = spark.sql("SELECT * from people ") 
print(people_df.count())
people_df.show();

# aggregate with group-by
print("\ngroup by sex")
sql = "SELECT sex, avg(age) from people group by sex"
print(sql)
age_df = spark.sql(sql)
print(age_df.count())
age_df.show();

# aggregate with group-by, but don't select that var.
print("\ngroup by sex")
sql = "SELECT avg(age) from people group by sex"
print(sql)
age_df = spark.sql(sql)
print(age_df.count())
age_df.show()

# basic parition windowing with rank()
print("\npartition by name, rank")
sql = "SELECT id, name, rank() over(partition by name order by id) as idx from people"
print(sql)
rank_df = spark.sql(sql)
print(rank_df.count())
rank_df.show()

# ERROR: windowing with rank(), but group by?
# rank requires over
#print("\ngroup by name, rank")
#rank_df = spark.sql("SELECT id, name, rank() as idx from people group by name")
#print(rank_df.count())
#rank_df.show()

print("\npartition by name, sex, collect_set")
sql = "SELECT id, name, sex, age, collect_set(id) over(partition by name, sex order by id) as idx from people"
print(sql)
rank_df = spark.sql(sql)
print(rank_df.count())
rank_df.show()

print("\ngroup by name, sex")
sql = "SELECT name, sex, avg(age), collect_set(id) from people group by name, sex "
print(sql)
rank_df = spark.sql(sql)
print(rank_df.count())
rank_df.show()

spark.stop()
