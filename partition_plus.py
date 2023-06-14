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
csv_file = "people_plus.csv"
schema=" id INT, name STRING, age INT, sex STRING, race STRING, plate STRING, color STRING"
people_df = spark.read.csv(csv_file, schema=schema)
people_df.write.mode("overwrite").saveAsTable("people_plus")


# plain
people_df = spark.sql("SELECT * from people_plus ") 
print(people_df.count())
people_df.show();


# without using the ID field, identify the people:
#sql="SELECT distinct name, sex, race from people_plus"
#print(sql)
#people_df = spark.sql(sql)
#print(people_df.count())
#people_df.show();
#
## ....without distinct
#sql="SELECT count(*) as ct, name, sex, race from people_plus group by name, sex, race"
#print(sql)
#people_df = spark.sql(sql)
#print(people_df.count())
#people_df.show();

#...don't need to aggregate anythinng, just group
sql="SELECT name, sex, race from people_plus group by name, sex, race order by name"
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();


# create an index on the sub-rows
sql='''
    SELECT name, sex, race, 
           rank() over(partition by  name, sex, race order by plate, color) as idx,
           plate, color
    FROM people_plus
'''
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();


# Get back to the 11 rows with a where on the idx (since it's not part of the aggregation/grouping
# Where Al works...
sql='''
    SELECT name, sex, race, 
           rank() over(partition by  name, sex, race order by plate, color) as idx,
           plate, color
    FROM people_plus
    WHERE name = 'Al'
    -- WHERE idx = 1
    -- HAVING idx=1
'''
#print(sql)
#people_df = spark.sql(sql)
#print(people_df.count())
#people_df.show();

# does where idx work? NO
# SO the result of a rank() is not available in that stage!
sql='''
    SELECT name, sex, race, 
           rank() over(partition by  name, sex, race order by plate, color) as idx,
           plate, color
    FROM people_plus
    WHERE idx = 1
    -- HAVING idx=1
'''
#print(sql)
#people_df = spark.sql(sql)
#print(people_df.count())
#people_df.show();


# Can you do it with Having? ...needs a group-by, NO
sql='''
    SELECT name, sex, race, 
           rank() over(partition by  name, sex, race order by plate, color) as idx,
           plate, color
           from people_plus
    GROUP BY name, sex, race, plate, color
    HAVING idx = 1
'''
#print(sql)
#people_df = spark.sql(sql)
#print(people_df.count())
#people_df.show();

#  2 queries works
sql='''
    WITH A as (
        SELECT name, sex, race, 
           rank() over(partition by  name, sex, race order by plate, color) as idx,
           plate, color
           from people_plus
        GROUP BY name, sex, race, plate, color
    )
    SELECT A.name, A.sex, A.race, A.plate, A.color from A
    WHERE idx = 1
    ORDER BY name
'''
print(sql)
people_df = spark.sql(sql)
print(people_df.count())
people_df.show();





spark.stop()
