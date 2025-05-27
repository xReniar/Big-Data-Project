#!/bin/bash

# remove output folder if already exists
#hdfs dfs -rm -r -f /user/$USER/spark-core/$1

rm -rf $1-result
$SPARK_HOME/bin/spark-submit \
    --master local[*] \
    "$1".py