#!/bin/bash

hdfs dfs -rm -r /user/$USER/map-reduce/job-1
hdfs dfs -mkdir -p /user/$USER/map-reduce/

hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.4.1.jar \
    -mapper mapper.py \
    -reducer reducer.py \
    -input /user/$USER/data/data_cleaned.csv \
    -output /user/$USER/map-reduce/job-1