#!/bin/bash

hdfs dfs -rm -r -f /user/$USER/map-reduce/$1
hdfs dfs -mkdir -p /user/$USER/map-reduce/

hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.4.1.jar \
    -mapper $1/mapper.py \
    -reducer $1/reducer.py \
    -input /user/$USER/data/$2.csv \
    -output /user/$USER/map-reduce/$1/

hdfs dfs -cat /user/$USER/map-reduce/$1/part-* | head -n 10