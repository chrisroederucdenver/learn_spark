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



# IT GETS INTERESTING WHEN  YOU USE BOTH
# "Add the columns or the expression to the GROUP BY, aggregate the expression, or use "any_value(id)" "
# "if you do not care which of the values within a group is returned.;"
#people_df = spark.sql(
#'''
#    SELECT count(*) as ct, name, sex, race, 
#           rank() over(partition by name, sex, race order by id) 
#    from people 
#    group by name, sex, race 
#'''
#) 
#print(people_df.count())
#people_df.show();

# "pyspark.errors.exceptions.captured.AnalysisException: [MISSING_AGGREGATION]"
# "The non-aggregating expression "id" is based on columns which are not participating in the GROUP BY clause."
# "Add the columns or the expression to the GROUP BY, aggregate the expression, or use "any_value(id)" "
# "if you do not care which of the values within a group is returned.;
#people_df = spark.sql(
#'''
#   SELECT name, count(*) as ct, avg(age) as avg_age, 
#           rank() over(partition by name order by id ) 
#   FROM people 
#   GROUP BY name
#'''
#)
#print(people_df.count())
#people_df.show();

print("...add to group-by")
sql='''
   SELECT name, count(*) as ct, avg(age) as avg_age, 
           rank() over(partition by name order by id ) as idx
   FROM people 
   GROUP BY name, id '''
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();

print("...aggregate (sum(id))")
sql='''
   SELECT name, count(*) as ct, avg(age) as avg_age, 
           rank() over(partition by name order by sum(id) ) as idx
   FROM people 
   GROUP BY name '''
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();

print("...any()")
sql='''
   SELECT name, count(*) as ct, avg(age) as avg_age, 
           rank() over(partition by name order by any_value(id) ) as idx
   FROM people 
   GROUP BY name
'''
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();

print("...first()")
sql='''
   SELECT name, count(*) as ct, avg(age) as avg_age, 
           rank() over(partition by name order by first(id) ) as idx
   FROM people 
   GROUP BY name
'''
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();






spark.stop()
