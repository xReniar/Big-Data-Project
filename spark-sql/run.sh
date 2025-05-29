#!/bin/bash

# remove output folder if already exists
hdfs dfs -rm -r -f /user/$USER/spark-sql/$1

$SPARK_HOME/bin/spark-submit \
    --master local[*] \
    "$1".py \
    -input /user/$USER/data/data-1.0%.csv \
    -output /user/$USER/spark-sql/$1