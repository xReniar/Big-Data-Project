#!/bin/bash

if [ ! -f "data/data_cleaned.csv" ]; then
    mkdir data
    curl -L -o \
        data/us-used-cars-dataset.zip \
        https://www.kaggle.com/api/v1/datasets/download/ananaymital/us-used-cars-dataset

    cd data
    unzip us-used-cars-dataset.zip
    rm -rf us-used-cars-dataset.zip
    cd ..

    # data cleaning
    $SPARK_HOME/bin/spark-submit \
        --master local[*] \
        clean_data.py

    # put cleaned dataset in hdfs
    cd data
    rm -rf used_cars_data.csv
    mv part-*.csv data_cleaned.csv
    hdfs dfs -mkdir -p /user/$USER/data
    hdfs dfs -put data_cleaned.csv /user/$USER/data
    cd ..

else
    echo "dataset already downloaded and ready to use"
fi