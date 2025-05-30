#!/bin/bash

# remove output folder if already exists
hdfs dfs -rm -r -f /user/$USER/spark-core/$1

$SPARK_HOME/bin/spark-submit \
    --master local[*] \
    $1.py \
    -input /user/$USER/data/$2.csv \
    -output /user/$USER/spark-core/$1