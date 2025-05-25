#!/bin/bash

if [ -z "$ROOT_DIR" ]; then
    export ROOT_DIR=$(pwd)
fi

# download data
if [ ! -f "data/used_cars_data.csv" ]; then
    mkdir data
    curl -L -o data/us-used-cars-dataset.zip https://www.kaggle.com/api/v1/datasets/download/ananaymital/us-used-cars-dataset
    cd data
    unzip us-used-cars-dataset.zip
    rm -rf us-used-cars-dataset.zip
    cd ..
else
    echo "dataset already downloaded and ready to use"
fi

# start hadoop
$HADOOP_HOME/sbin/stop-dfs.sh
rm -rf /tmp/*
$HADOOP_HOME/bin/hdfs namenode -format
$HADOOP_HOME/sbin/start-dfs.sh