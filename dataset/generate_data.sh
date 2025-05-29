#!/bin/bash

if [ ! -f "data/data_cleaned.csv" ]; then
    $SPARK_HOME/bin/spark-submit \
        --master local[*] \
        clean_data.py

    cd data
    rm -rf used_cars_data.csv
    mv part-*.csv data_cleaned.csv
    cd ..
fi

hdfs dfs -mkdir -p /user/$USER/data
hdfs dfs -put data/data_cleaned.csv /user/$USER/data
#hdfs dfs -mv /user/$USER/data/part-*.csv data/data_cleaned.csv

$SPARK_HOME/bin/spark-submit \
    --master local[*] \
    generate_portions.py \
    --fractions "0.01 0.2 0.5 0.7"