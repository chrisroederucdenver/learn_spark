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
people_df = spark.sql("SELECT * from people ") 
print(people_df.count())
people_df.show();


print("DISTINCT")
sql="SELECT distinct name, sex, race  from people ") 
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();

sql="SELECT distinct name, sex  from people ") 
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();

sql="SELECT distinct name  from people ") 
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();

print("COUNT, group-by")
sql="SELECT count(*) as ct, name, sex, race  from people group by name, sex, race ") 
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();

sql="SELECT count(*) as ct, name, sex  from people group by name, sex ") 
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();

sql="SELECT count(*) as ct, name  from people group by name ") 
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();


spark.stop()
