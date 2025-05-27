#!/bin/bash

# remove output folder if already exists
#hdfs dfs -rm -r -f /user/$USER/spark-sql/$1

$SPARK_HOME/bin/spark-submit \
    --master local[6] \
    "$1".py