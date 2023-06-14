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


# plain
sql= "SELECT * from people "
print(sql)
people_df = spark.sql(sql) 
print(people_df.count())
people_df.show();



print("...aggregate (sum(id))")
sql='''
   SELECT name, count(*) as ct, avg(age) as avg_age, sum(id) as sum_id
   FROM people 
   GROUP BY name '''
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();

print("...any()")
sql='''
   SELECT name, count(*) as ct, avg(age) as avg_age, any_value(id) as any_id
   FROM people 
   GROUP BY name
'''
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();

print("...first()")
sql='''
   SELECT name, count(*) as ct, avg(age) as avg_age,  first(id) as first_id
   FROM people 
   GROUP BY name
'''
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();






spark.stop()
