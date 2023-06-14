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


print("rank() partition-by")

sql="SELECT name, sex, race, rank() over(partition by name, sex, race order by id) as idx from people "
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();

sql="SELECT name, sex, rank() over(partition by name, sex order by id) as idx from people "
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();

sql="SELECT name, rank() over(partition by name order by id) as idx from people "
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();




#print("just partition-by") # doesn't work
#people_df = spark.sql("SELECT name, sex, race, over(partition by name, sex, race order by id) from people ") 
#print(people_df.count())
#people_df.show();
#
#people_df = spark.sql("SELECT name, sex, over(partition by name, sex order by id)  from people ") 
#print(people_df.count())
#people_df.show();
#
#people_df = spark.sql("SELECT name,  over(partition by name order by id) from people ") 
#print(people_df.count())
#people_df.show();


#print("COUNT, partition-by") # requires a group-by for the count
#people_df = spark.sql("SELECT count(*) as ct, name, sex, race, rank() over(partition by name, sex, race order by id) from people ") 
#print(people_df.count())
#people_df.show();
#
#people_df = spark.sql("SELECT count(*) as ct, name, sex, rank() over(partition by name, sex order by id) from people ") 
#print(people_df.count())
#people_df.show();
#
#people_df = spark.sql("SELECT count(*) as ct, name, rank() over(partiion by name order by id) from people ") 
#print(people_df.count())
#people_df.show();




spark.stop()
