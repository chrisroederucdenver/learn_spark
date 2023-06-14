# Explore Spark SQL
I need to play with collect_set and expand my familiarity with both "group by" and "over" windowing.

Here is some python code to do set up a small dataset and demo some of these features.

- create.py creates a table of people with some duplicate names, but distinct ids. This creates a situation where you can play with grouping the data by name allowing for explorations of the contrast between group by and partition by.
- distinct.py  uses the same data to consider how distinct mixes in or can be replaced.
- aggregate_not.py shows what any_value() and first() do. They don't really aggregate, but they work in that context, picking a single value from the group sort-of like the aggregating functions min,max, avg etc. do. In that sense, they are aggregators.
- parition.py This is a demo of rank() over different partitions. Like group-by in that you're dividing the result set into groups based on certain fields, over() is different in that there is no requirement on aggregating the variables that are not in the group-by phrase. You get every row, so there's no need to aggregate anything. Behind all this is an intuition that the columns listed in the group-by are keys to the result-set. Since you only get one row per "key", you have to aggregate. The key idea holds for the partitions created by over(), but you don't need to aggregate since  you get all rows back.
- both.py Using group-by and rank/over at the same time requires a clear understanding of the two and how they fit into the SQL query pipeline. 


# partitioning vs grouping
This page, 
https://towardsdatascience.com/the-6-steps-of-a-sql-select-statement-process-b3696a49a642
describes 6 stages to SQL processing. 


1. Getting Data (From, Join)
2. Row Filter (Where)
3. Grouping (Group by)
4. Group Filter (Having)
5. Return Expressions (Select)
6. Order & Paging (Order by & Limit / Offset)

Group-by is known to happend after inital result set creating with aggregating functions and then the having filter. Learning rank/over was confusing for me because I wanted to see it like group-b. It is, and it is not. The initial result set has to be fetched, and it is grouped, but the aggregation and distillation to a fewer number of rows doesn't happen. Functions like rank() and row_Number() (?) are applied to every row within the context of the partition before the aggregation step. After this step, is when group-by is applied and it lets you do some mind-stretchings like partion on one set of columns and group-by a different set!

## Challenge: find some creative uses or examples of grouping and paritioning to show a use of both in the same query...
Actually, with a lack of good keys in a dataset like people, and a denormalized table that includes many entities for each person, you might use rank() to create an artificial key for each person, and then want to count those rows for each.



# TODO
- I have figured out how to drop a table from within spark, so the run.sh script removes the file from the file system.
