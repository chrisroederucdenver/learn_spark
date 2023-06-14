#!/bin/bash

#echo "** create? **")
#rm -rf spark-warehouse/learn_spark_db.db/people 
#./create.py

#echo "** distinct **"
#rm -rf spark-warehouse/learn_spark_db.db/people 
#./distinct.py

#echo "** aggregate_not **"
#rm -rf spark-warehouse/learn_spark_db.db/people 
#./aggregate_not.py

#echo "** partition **"
#rm -rf spark-warehouse/learn_spark_db.db/people 
#./partition.py

#echo "** both **"
#rm -rf spark-warehouse/learn_spark_db.db/people 
#./both.py

echo "** partition plus **"
rm -rf spark-warehouse/learn_spark_db.db/people_plus
./partition_plus.py
